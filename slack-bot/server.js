// ============================================================
//  Newforest Task Tracker — Slack Bot
//  Deployed on Render. Listens to #task-updates channel + DMs.
// ============================================================

const { App } = require('@slack/bolt');
const { createClient } = require('@supabase/supabase-js');
const Anthropic = require('@anthropic-ai/sdk');

// ── CLIENTS ─────────────────────────────────────────────────
const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  signingSecret: process.env.SLACK_SIGNING_SECRET,
});

const sb = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY  // service role — bypasses RLS
);

const claude = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

// ── LINEAGES ─────────────────────────────────────────────────
const LINEAGES = [
  'Land & Forest',
  'Farm & Garden',
  'Building Improvements',
  'Other Building',
  'Site Infrastructure',
  'Housekeeping',
  'Kitchen',
  'Dining',
  'Administration',
];

// ── MESSAGE HANDLER ──────────────────────────────────────────
// Fires for channel messages AND DMs
app.message(async ({ message, say, client }) => {
  // Ignore bot messages, edits, deletes
  if (message.subtype || message.bot_id) return;

  await handleMessage({ text: message.text, slackUserId: message.user, say, client });
});

async function handleMessage({ text, slackUserId, say, client }) {
  if (!text || !text.trim()) return;

  // ── 1. Get Slack user's real name ──
  let userName = 'Unknown';
  try {
    const info = await client.users.info({ user: slackUserId });
    userName = info.user.profile?.real_name || info.user.real_name || info.user.name;
  } catch (e) {
    console.error('users.info error:', e.message);
  }

  // ── 2. Fetch active tasks from Supabase ──
  const { data: tasks, error: tasksErr } = await sb
    .from('tasks')
    .select('id, name, stage, percent_complete, assignees, lineage')
    .not('stage', 'eq', 'complete')
    .order('created_at', { ascending: true });

  if (tasksErr) {
    console.error('Supabase fetch error:', tasksErr);
    await say('Sorry, I couldn\'t load the task list right now. Try again in a moment.');
    return;
  }

  const taskList = (tasks || []).map(t =>
    `ID: ${t.id} | "${t.name}" | Stage: ${t.stage} | ${t.percent_complete}% | Assignees: ${(t.assignees || []).join(', ') || 'none'}`
  ).join('\n');

  // ── 3. Call Claude to parse intent ──
  let parsed;
  let rawResponse;
  try {
    const response = await claude.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 800,
      messages: [{
        role: 'user',
        content: `You are a task tracker assistant for Newforest, a UK estate management company.
The person messaging is named "${userName}".
Their message: "${text}"

Active tasks in the system:
${taskList || '(no active tasks yet)'}

Available lineages (departments): ${LINEAGES.join(', ')}

Determine the INTENT and return ONLY valid JSON — no markdown, no explanation.

If the message is clearly a request to CREATE a new task (e.g. "create task", "add a task", "new task", or describes work that doesn't exist yet):
{
  "intent": "create_task",
  "new_task": {
    "name": "short task name",
    "description": "fuller description of the work",
    "priority": "low" | "medium" | "high" | "urgent",
    "assignees": ["Name1", "Name2"],
    "lineage": "matching lineage from the list, or null"
  },
  "needs_clarification": false,
  "clarification_question": null
}

If the message is a progress UPDATE on an existing task:
{
  "intent": "update_task",
  "matched_task_id": "uuid of the best matching task",
  "confidence": "high" | "low",
  "update_text": "clean progress note suitable for a log entry",
  "percent_complete": null | integer 0-100,
  "new_stage": null | "assigned" | "inprogress" | "review" | "complete",
  "needs_clarification": false,
  "clarification_question": null
}

If genuinely unclear (ONLY if you truly cannot determine intent or the key task name is missing):
{
  "intent": "unclear",
  "needs_clarification": true,
  "clarification_question": "one short specific question"
}

Rules:
- If the message mentions a task name that clearly doesn't exist in the active list AND describes new work → intent: create_task
- Match existing tasks generously — workers use shorthand (e.g. "4 corners" matches "Clear 4 corners trail")
- For updates: "done"/"finished"/"complete" → new_stage: complete, percent_complete: 100
- "started"/"begun"/"working on"/"underway" → new_stage: inprogress
- "ready for check"/"ready for review" → new_stage: review
- Extract percentages: "halfway" → 50, "nearly done" → 90, "75% done" → 75
- For task creation: infer priority from urgency words ("urgent", "asap" → urgent; "when you can" → low)
- For task creation: infer lineage from context (fallen tree → Land & Forest, building work → Building Improvements, etc.)
- For task creation: infer assignees from message if mentioned (e.g. "assign to Dwayne" → ["Dwayne"])
- Only set needs_clarification: true if you truly cannot proceed — don't ask if you can make a reasonable inference`
      }]
    });

    rawResponse = response.content[0].text.trim();
    // Strip markdown code fences if present
    rawResponse = rawResponse.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/, '').trim();
    console.log('Claude raw response:', rawResponse);
    parsed = JSON.parse(rawResponse);
  } catch (e) {
    console.error('Claude/parse error:', e.message);
    console.error('Claude raw output:', rawResponse);
    await say('Sorry, I had trouble processing that. Check Render logs for details.');
    return;
  }

  // ── 4. Needs clarification? ──
  if (parsed.needs_clarification) {
    await say(parsed.clarification_question || 'Could you clarify that a bit more?');
    return;
  }

  // ── 5A. CREATE TASK ──────────────────────────────────────────
  if (parsed.intent === 'create_task') {
    const nt = parsed.new_task || {};
    if (!nt.name) {
      await say('I couldn\'t work out a task name from that. Could you give the task a name?');
      return;
    }

    const { data: created, error: createErr } = await sb.from('tasks').insert({
      name: nt.name,
      description: nt.description || '',
      priority: nt.priority || 'medium',
      assignees: nt.assignees || [],
      lineage: nt.lineage || null,
      stage: 'assigned',
      percent_complete: 0,
      creator_name: userName,
    }).select('id').single();

    if (createErr) {
      console.error('task create error:', createErr);
      await say('I understood the request but couldn\'t create the task. Check Render logs.');
      return;
    }

    const details = [];
    if (nt.priority) details.push(`Priority: ${nt.priority}`);
    if (nt.lineage) details.push(`Lineage: ${nt.lineage}`);
    if (nt.assignees && nt.assignees.length) details.push(`Assigned to: ${nt.assignees.join(', ')}`);

    let confirm = `✅ Task created: *${nt.name}*`;
    if (details.length) confirm += `\n> ${details.join(' · ')}`;
    await say(confirm);
    return;
  }

  // ── 5B. UPDATE TASK ──────────────────────────────────────────
  if (parsed.intent === 'update_task') {
    if (!parsed.matched_task_id) {
      await say('I couldn\'t match that to any active task. Could you mention the task name?');
      return;
    }

    const task = (tasks || []).find(t => t.id === parsed.matched_task_id);
    const taskName = task?.name || 'Unknown task';

    // Write progress update log entry
    const today = new Date().toISOString().split('T')[0];
    const { error: updateErr } = await sb.from('task_updates').insert({
      task_id: parsed.matched_task_id,
      author: userName,
      text: parsed.update_text,
      date: today,
      via: 'Slack',
    });

    if (updateErr) {
      console.error('task_updates insert error:', updateErr);
      await say('I understood the update but couldn\'t save it. Please try again.');
      return;
    }

    // Update task fields if inferred
    const taskUpdates = {};
    if (parsed.percent_complete !== null && parsed.percent_complete !== undefined) {
      taskUpdates.percent_complete = parsed.percent_complete;
    }
    if (parsed.new_stage) {
      taskUpdates.stage = parsed.new_stage;
    }

    if (Object.keys(taskUpdates).length > 0) {
      const { error: taskErr } = await sb
        .from('tasks')
        .update(taskUpdates)
        .eq('id', parsed.matched_task_id);
      if (taskErr) console.error('task update error:', taskErr);
    }

    // Confirm back to Slack
    const confidenceNote = parsed.confidence === 'low' ? ' _(low confidence match)_' : '';
    let confirm = `✅ Update logged for *${taskName}*${confidenceNote}`;

    const changes = [];
    if (parsed.percent_complete !== null && parsed.percent_complete !== undefined) {
      changes.push(`${parsed.percent_complete}% complete`);
    }
    if (parsed.new_stage) {
      const stageLabel = {
        assigned: 'Assigned', inprogress: 'In Progress',
        review: 'Review', complete: 'Complete'
      }[parsed.new_stage] || parsed.new_stage;
      changes.push(`stage → ${stageLabel}`);
    }
    if (changes.length) confirm += `\n> ${changes.join(' · ')}`;

    await say(confirm);
    return;
  }

  // Fallback
  await say('I\'m not sure what you\'d like me to do. You can log a task update or create a new task — just describe it naturally.');
}

// ── START ─────────────────────────────────────────────────────
(async () => {
  const port = process.env.PORT || 3000;
  await app.start(port);
  console.log(`Newforest Task Bot running on port ${port}`);
})();
