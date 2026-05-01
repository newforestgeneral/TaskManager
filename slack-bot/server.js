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

// ── HEALTH CHECK ─────────────────────────────────────────────
const { ExpressReceiver } = require('@slack/bolt');
// Bolt's built-in receiver handles GET / automatically via express

// ── MESSAGE HANDLER ──────────────────────────────────────────
// Fires for channel messages AND DMs
app.message(async ({ message, say, client }) => {
  // Ignore bot messages, edits, deletes
  if (message.subtype || message.bot_id) return;

  await handleUpdate({ text: message.text, slackUserId: message.user, say, client });
});

async function handleUpdate({ text, slackUserId, say, client }) {
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
    `ID: ${t.id} | "${t.name}" | Stage: ${t.stage} | Progress: ${t.percent_complete}%`
  ).join('\n');

  // ── 3. Call Claude Haiku to parse the message ──
  let parsed;
  try {
    const response = await claude.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 600,
      messages: [{
        role: 'user',
        content: `You are a task tracker assistant for Newforest, an estate management company.
A worker named "${userName}" sent this message: "${text}"

Active tasks:
${taskList || '(no active tasks)'}

Return ONLY valid JSON with no markdown or explanation:
{
  "matched_task_id": "uuid or null",
  "confidence": "high" or "low",
  "update_text": "natural language update suitable for a progress log",
  "percent_complete": null or integer 0-100,
  "new_stage": null or one of: assigned / inprogress / review / complete,
  "needs_clarification": true or false,
  "clarification_question": "question to ask the worker, or null"
}

Rules:
- Match task by name similarity — be generous, workers use shorthand
- "done" / "finished" / "complete" / "all done" → new_stage: complete, percent_complete: 100
- "started" / "begun" / "working on" / "underway" → new_stage: inprogress
- "ready for check" / "ready for review" / "waiting on you" → new_stage: review
- Extract any percentage mentioned (e.g. "about halfway" → 50, "75% done" → 75)
- If the message matches no task clearly → needs_clarification: true
- If confidence is low, still attempt a match but set confidence: "low"
- update_text should be a clean first-person or third-person progress note`
      }]
    });

    // Strip markdown code fences if Claude wraps the JSON
    let rawText = response.content[0].text.trim();
    rawText = rawText.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/, '').trim();
    console.log('Claude raw response:', rawText);
    parsed = JSON.parse(rawText);
  } catch (e) {
    console.error('Claude/parse error:', e.message);
    console.error('Claude raw output:', response?.content?.[0]?.text);
    await say('Sorry, I had trouble processing that. Check Render logs for details.');
    return;
  }

  // ── 4. Needs clarification? ──
  if (parsed.needs_clarification) {
    await say(parsed.clarification_question || 'Could you clarify which task this update is for?');
    return;
  }

  if (!parsed.matched_task_id) {
    await say('I couldn\'t match that to any active task. Could you mention the task name?');
    return;
  }

  const task = (tasks || []).find(t => t.id === parsed.matched_task_id);
  const taskName = task?.name || 'Unknown task';

  // ── 5. Write progress update ──
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

  // ── 6. Update task fields if inferred ──
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

  // ── 7. Confirm back to Slack ──
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
}

// ── START ─────────────────────────────────────────────────────
(async () => {
  const port = process.env.PORT || 3000;
  await app.start(port);
  console.log(`Newforest Task Bot running on port ${port}`);
})();
