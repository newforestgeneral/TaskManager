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
  process.env.SUPABASE_SERVICE_KEY
);

const claude = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

// ── RECENT TASK MEMORY (per user, resets on server restart) ──
// Tracks the last task each Slack user created or updated
const recentTask = new Map(); // slackUserId → { id, name }

// ── CONSTANTS ────────────────────────────────────────────────
const LINEAGES = [
  'Land & Forest', 'Farm & Garden', 'Building Improvements',
  'Other Building', 'Site Infrastructure', 'Housekeeping',
  'Kitchen', 'Dining', 'Administration',
];

const STAGE_LABELS = {
  assigned: 'Assigned', inprogress: 'In Progress',
  review: 'Review', complete: 'Complete',
};

const SNAP_DELIMITER = '\n[SNAP]:';

// ── HELPERS ──────────────────────────────────────────────────
function nowLabel() {
  return new Date().toLocaleTimeString('en-GB', {
    hour: '2-digit', minute: '2-digit', timeZone: 'America/Toronto'
  });
}

function todayISO() {
  return new Date().toISOString().split('T')[0];
}

function encodeSnap(snapshot) {
  return SNAP_DELIMITER + JSON.stringify(snapshot);
}

function decodeSnap(text) {
  const idx = text.indexOf(SNAP_DELIMITER);
  if (idx === -1) return null;
  try {
    return JSON.parse(text.slice(idx + SNAP_DELIMITER.length));
  } catch {
    return null;
  }
}

function visibleText(text) {
  const idx = text.indexOf(SNAP_DELIMITER);
  return idx === -1 ? text : text.slice(0, idx);
}

// ── MESSAGE HANDLER ──────────────────────────────────────────
app.message(async ({ message, say, client }) => {
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

  // ── 2. Fetch active tasks ──
  const { data: tasks, error: tasksErr } = await sb
    .from('tasks')
    .select('id, name, stage, percent_complete, assignees, lineage, priority, description, start_date, due_date, follow_up_date, estimated_hours, location, task_notes')
    .not('stage', 'eq', 'complete')
    .order('created_at', { ascending: true });

  if (tasksErr) {
    console.error('Supabase fetch error:', tasksErr);
    await say('Sorry, I couldn\'t load the task list right now. Try again in a moment.');
    return;
  }

  const taskList = (tasks || []).map(t =>
    `ID: ${t.id} | "${t.name}" | Stage: ${t.stage} | ${t.percent_complete}% | Priority: ${t.priority} | Assignees: ${(t.assignees || []).join(', ') || 'none'} | Lineage: ${t.lineage || 'none'}`
  ).join('\n');

  const lastTouched = recentTask.get(slackUserId);
  const recentContext = lastTouched
    ? `\nMost recently created/updated task by this user: ID: ${lastTouched.id} | "${lastTouched.name}"`
    : '';

  // ── Fetch bot memory ──
  const { data: memories } = await sb
    .from('bot_memory')
    .select('category, key, value')
    .order('times_used', { ascending: false });

  const memoryContext = memories?.length
    ? '\nLearned memory (apply these facts to improve accuracy):\n' +
      memories.map(m => `- [${m.category}] "${m.key}": ${m.value}`).join('\n')
    : '';

  // ── 3. Claude intent detection ──
  let parsed;
  let rawResponse;
  try {
    const response = await claude.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 1000,
      messages: [{
        role: 'user',
        content: `You are a task tracker assistant for Newforest, a UK estate management company.
The person messaging is named "${userName}".
Their message: "${text}"

Active tasks:
${taskList || '(no active tasks yet)'}
${recentContext}
${memoryContext}

Available lineages: ${LINEAGES.join(', ')}
Valid priorities: low, medium, high, urgent
Valid stages: assigned, inprogress, review, complete
Date format: YYYY-MM-DD. Today: ${todayISO()}

Return ONLY valid JSON — no markdown, no explanation.

────────────────────────────────
INTENT: create_task
When the message describes new work that doesn't match any existing task.
{
  "intent": "create_task",
  "new_task": {
    "name": "short task name",
    "description": "fuller description",
    "priority": "low|medium|high|urgent",
    "assignees": ["Name1"],
    "lineage": "lineage or null",
    "start_date": "YYYY-MM-DD or null",
    "due_date": "YYYY-MM-DD or null",
    "estimated_hours": number or null,
    "location": "string or null"
  },
  "needs_clarification": false,
  "clarification_question": null
}

────────────────────────────────
INTENT: update_task
When the message is a progress report or a request to change fields on an existing task.
{
  "intent": "update_task",
  "matched_task_id": "uuid",
  "confidence": "high|low",
  "update_text": "human-readable progress note for the log",
  "field_changes": {
    "percent_complete": null or 0-100,
    "stage": null or stage value,
    "priority": null or priority value,
    "name": null or "new name",
    "description": null or "new description",
    "lineage": null or "lineage",
    "add_assignees": null or ["Name"],
    "remove_assignees": null or ["Name"],
    "start_date": null or "YYYY-MM-DD",
    "due_date": null or "YYYY-MM-DD",
    "follow_up_date": null or "YYYY-MM-DD",
    "estimated_hours": null or number,
    "location": null or "string",
    "notes": null or "text to append"
  },
  "needs_clarification": false,
  "clarification_question": null
}

────────────────────────────────
INTENT: query_task
When the message asks for information, details, or status on a task
(e.g. "tell me about X", "what's the status of X", "info on X", "who's assigned to X", "give me details on X").
{
  "intent": "query_task",
  "matched_task_id": "uuid or null",
  "confidence": "high|low",
  "needs_clarification": false,
  "clarification_question": null
}

────────────────────────────────
INTENT: revert
When the message asks to undo or revert the last bot change to a task
(e.g. "undo that", "revert last change to X", "roll back the gate task").
{
  "intent": "revert",
  "matched_task_id": "uuid or null",
  "confidence": "high|low",
  "needs_clarification": false,
  "clarification_question": null
}

────────────────────────────────
INTENT: unclear
LAST RESORT ONLY. Only use if the message is completely unintelligible or gives zero context.
If the user has named or described a task at all, never use this — pick the closest intent.
{
  "intent": "unclear",
  "needs_clarification": true,
  "clarification_question": "one short specific question"
}

────────────────────────────────
MEMORY EXTRACTION (add to every response)
After determining intent, include a "new_memories" array of facts worth remembering for future interactions.
Only add entries that are genuinely useful and not already covered in the learned memory above.
Leave the array empty if nothing new is worth remembering.
"new_memories": [
  {
    "category": "alias|location|worker|lineage|correction|general",
    "key": "the trigger term (short, lowercase)",
    "value": "the fact to remember"
  }
]

Good candidates for new memories:
- User corrects a lineage, priority, or assignee the bot got wrong → category: correction
- A new location or area name is mentioned → category: location
- A shorthand term is used for a task or place → category: alias
- A worker's name appears with context about their role or area → category: worker
- A new pattern about what lineage a type of work belongs to → category: lineage

Do NOT add memories for: one-off specifics with no reuse value, task IDs, percentages, dates.

────────────────────────────────
Rules:
- Match tasks generously — workers use shorthand
- "done"/"finished"/"complete" → stage: complete, percent_complete: 100
- "started"/"working on"/"underway" → stage: inprogress
- "ready for check"/"ready for review" → stage: review
- "halfway" → 50, "nearly done" → 90
- "urgent"/"asap" → priority: urgent; "when you can" → priority: low
- "assign to X"/"add X" → add_assignees; "remove X"/"unassign X" → remove_assignees
- Infer lineage from context (fallen tree → Land & Forest, roof work → Building Improvements)
- "undo"/"revert"/"roll back"/"undo that"/"go back" → intent: revert
- If the message uses "that", "it", "the one I just made", "that task", or any vague reference without naming a task, assume they mean the most recently created/updated task shown above
- "tell me about", "info on", "what's the status", "details on", "give me details", "show me" → query_task
- "info on the task" / "give me the current details" with a recently touched task in context → query_task on that task
- Match task names generously — "4 corners", "the 4 corners task", "corners task" all match "Clear 4 corners"
- NEVER ask the user to repeat information they have already provided in the same conversation
- NEVER use intent: unclear if a task name or reference has been mentioned — always attempt a match`
      }]
    });

    rawResponse = response.content[0].text.trim();
    rawResponse = rawResponse.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/, '').trim();
    console.log('Claude raw response:', rawResponse);
    parsed = JSON.parse(rawResponse);
  } catch (e) {
    console.error('Claude/parse error:', e.message);
    console.error('Claude raw output:', rawResponse);
    await say('Sorry, I had trouble processing that. Check Render logs for details.');
    return;
  }

  if (parsed.needs_clarification) {
    await say(parsed.clarification_question || 'Could you clarify that a bit more?');
    return;
  }

  // ── 5A. CREATE TASK ─────────────────────────────────────────
  if (parsed.intent === 'create_task') {
    const nt = parsed.new_task || {};
    if (!nt.name) {
      await say('I couldn\'t work out a task name. Could you give the task a name?');
      return;
    }

    // Fetch default template
    const { data: tmplRows } = await sb
      .from('task_templates')
      .select('*')
      .eq('is_default', true)
      .limit(1);
    const tmpl = tmplRows?.[0] || {};

    // Merge: template provides defaults, bot-inferred values override
    const newTask = {
      name:                nt.name,
      description:         nt.description        || tmpl.description              || '',
      priority:            nt.priority            || tmpl.default_priority         || 'medium',
      stage:               tmpl.default_stage                                      || 'assigned',
      assignees:           nt.assignees?.length   ? nt.assignees : (tmpl.default_assignees || []),
      lineage:             nt.lineage             || tmpl.default_lineage          || null,
      estimated_hours:     nt.estimated_hours     || tmpl.default_estimated_hours  || null,
      weather_dependent:   tmpl.default_weather_dependent                          || 'no',
      steps:               tmpl.default_steps                                      || [],
      materials:           tmpl.default_materials                                  || [],
      tools:               tmpl.default_tools                                      || [],
      task_notes:          tmpl.default_task_notes                                 || null,
      percent_complete:    0,
      creator_name:        userName,
      start_date:          nt.start_date  || null,
      due_date:            nt.due_date    || null,
      location:            nt.location   || null,
    };

    const { data: created, error: createErr } = await sb.from('tasks').insert(newTask).select('id').single();

    if (createErr) {
      console.error('task create error:', createErr);
      await say('Understood the request but couldn\'t create the task. Check Render logs.');
      return;
    }

    await sb.from('task_updates').insert({
      task_id: created.id,
      author: userName,
      text: `Task created via Slack by ${userName}.`,
      date: todayISO(),
      via: 'Slack Bot',
    });

    recentTask.set(slackUserId, { id: created.id, name: nt.name });

    const details = [];
    if (nt.priority) details.push(`Priority: ${nt.priority}`);
    if (nt.lineage) details.push(`Lineage: ${nt.lineage}`);
    if (nt.assignees?.length) details.push(`Assigned to: ${nt.assignees.join(', ')}`);
    if (nt.due_date) details.push(`Due: ${nt.due_date}`);
    if (nt.location) details.push(`Location: ${nt.location}`);

    let confirm = `✅ Task created: *${nt.name}* — ${nowLabel()}`;
    if (details.length) confirm += `\n> ${details.join(' · ')}`;
    await say(confirm);
    await saveMemories(parsed.new_memories, text);
    return;
  }

  // ── 5B. UPDATE TASK ─────────────────────────────────────────
  if (parsed.intent === 'update_task') {
    if (!parsed.matched_task_id) {
      await say('I couldn\'t match that to any active task. Could you mention the task name?');
      return;
    }

    const task = (tasks || []).find(t => t.id === parsed.matched_task_id);
    const taskName = task?.name || 'Unknown task';
    const fc = parsed.field_changes || {};

    // Capture before-snapshot for potential revert
    const snapshot = {
      percent_complete: task?.percent_complete,
      stage: task?.stage,
      priority: task?.priority,
      name: task?.name,
      description: task?.description,
      lineage: task?.lineage,
      assignees: task?.assignees,
      start_date: task?.start_date,
      due_date: task?.due_date,
      follow_up_date: task?.follow_up_date,
      estimated_hours: task?.estimated_hours,
      location: task?.location,
      task_notes: task?.task_notes,
    };

    // Build DB update + change log
    const dbUpdate = {};
    const changeLines = [];

    if (fc.percent_complete !== null && fc.percent_complete !== undefined) {
      dbUpdate.percent_complete = fc.percent_complete;
      changeLines.push(`Progress: ${task?.percent_complete ?? '?'}% → ${fc.percent_complete}%`);
    }
    if (fc.stage) {
      dbUpdate.stage = fc.stage;
      changeLines.push(`Stage: ${STAGE_LABELS[task?.stage] || task?.stage || '?'} → ${STAGE_LABELS[fc.stage] || fc.stage}`);
    }
    if (fc.priority) {
      dbUpdate.priority = fc.priority;
      changeLines.push(`Priority: ${task?.priority || '?'} → ${fc.priority}`);
    }
    if (fc.name) {
      dbUpdate.name = fc.name;
      changeLines.push(`Name: "${taskName}" → "${fc.name}"`);
    }
    if (fc.description) {
      dbUpdate.description = fc.description;
      changeLines.push(`Description updated`);
    }
    if (fc.lineage) {
      dbUpdate.lineage = fc.lineage;
      changeLines.push(`Lineage: ${task?.lineage || 'none'} → ${fc.lineage}`);
    }
    if (fc.start_date) {
      dbUpdate.start_date = fc.start_date;
      changeLines.push(`Start date: ${fc.start_date}`);
    }
    if (fc.due_date) {
      dbUpdate.due_date = fc.due_date;
      changeLines.push(`Due date: ${fc.due_date}`);
    }
    if (fc.follow_up_date) {
      dbUpdate.follow_up_date = fc.follow_up_date;
      changeLines.push(`Follow-up: ${fc.follow_up_date}`);
    }
    if (fc.estimated_hours !== null && fc.estimated_hours !== undefined) {
      dbUpdate.estimated_hours = fc.estimated_hours;
      changeLines.push(`Estimated hours: ${fc.estimated_hours}`);
    }
    if (fc.location) {
      dbUpdate.location = fc.location;
      changeLines.push(`Location: ${fc.location}`);
    }
    if (fc.notes) {
      const existing = task?.task_notes || '';
      dbUpdate.task_notes = existing
        ? `${existing}\n\n[${todayISO()} via Slack] ${fc.notes}`
        : `[${todayISO()} via Slack] ${fc.notes}`;
      changeLines.push(`Notes updated`);
    }
    if (fc.add_assignees?.length || fc.remove_assignees?.length) {
      let current = [...(task?.assignees || [])];
      if (fc.remove_assignees?.length) {
        const toRemove = fc.remove_assignees.map(n => n.toLowerCase());
        current = current.filter(n => !toRemove.includes(n.toLowerCase()));
      }
      if (fc.add_assignees?.length) {
        const exist = current.map(c => c.toLowerCase());
        current = [...current, ...fc.add_assignees.filter(n => !exist.includes(n.toLowerCase()))];
      }
      dbUpdate.assignees = current;
      changeLines.push(`Assignees: ${current.join(', ') || 'none'}`);
    }

    if (Object.keys(dbUpdate).length > 0) {
      const { error: taskErr } = await sb.from('tasks').update(dbUpdate).eq('id', parsed.matched_task_id);
      if (taskErr) {
        console.error('task update error:', taskErr);
        await say(`Couldn't save changes to *${taskName}*. Check Render logs.`);
        return;
      }
    }

    // Write audit log with embedded snapshot for revert
    const logParts = [];
    if (parsed.update_text) logParts.push(parsed.update_text);
    if (changeLines.length) logParts.push(`Fields changed: ${changeLines.join(' · ')}`);

    const logText = logParts.join('\n') + encodeSnap(snapshot);

    const { error: updateErr } = await sb.from('task_updates').insert({
      task_id: parsed.matched_task_id,
      author: userName,
      text: logText,
      date: todayISO(),
      via: 'Slack Bot',
    });
    if (updateErr) console.error('task_updates insert error:', updateErr);

    recentTask.set(slackUserId, { id: parsed.matched_task_id, name: fc.name || taskName });

    const confidenceNote = parsed.confidence === 'low' ? ' _(low confidence)_' : '';
    let confirm = `✅ *${fc.name || taskName}* updated — ${nowLabel()}${confidenceNote}`;
    if (changeLines.length) confirm += `\n> ${changeLines.join(' · ')}`;
    confirm += `\n_Say "undo that" to revert._`;
    await say(confirm);
    await saveMemories(parsed.new_memories, text);
    return;
  }

  // ── 5C. QUERY TASK ──────────────────────────────────────────
  if (parsed.intent === 'query_task') {
    if (!parsed.matched_task_id) {
      await say('I couldn\'t find that task. Could you give me the task name?');
      return;
    }

    const task = (tasks || []).find(t => t.id === parsed.matched_task_id);
    if (!task) {
      await say('That task doesn\'t appear to be active. It may be complete or not yet created.');
      return;
    }

    const stageLabel = STAGE_LABELS[task.stage] || task.stage;
    const assignees  = task.assignees?.join(', ') || 'Nobody assigned';
    const lines = [
      `*${task.name}*`,
      `> Stage: ${stageLabel} · Progress: ${task.percent_complete}%`,
      `> Priority: ${task.priority}${task.lineage ? ` · Lineage: ${task.lineage}` : ''}`,
      `> Assigned to: ${assignees}`,
    ];
    if (task.due_date)        lines.push(`> Due: ${task.due_date}`);
    if (task.follow_up_date)  lines.push(`> Follow-up: ${task.follow_up_date}`);
    if (task.location)        lines.push(`> Location: ${task.location}`);
    if (task.estimated_hours) lines.push(`> Estimated: ${task.estimated_hours}h`);
    if (task.description)     lines.push(`> _${task.description}_`);

    recentTask.set(slackUserId, { id: task.id, name: task.name });
    await say(lines.join('\n'));
    return;
  }

  // ── 5E. REVERT ──────────────────────────────────────────────
  if (parsed.intent === 'revert') {
    if (!parsed.matched_task_id) {
      await say('Which task should I revert? Could you mention the task name?');
      return;
    }

    const task = (tasks || []).find(t => t.id === parsed.matched_task_id);
    const taskName = task?.name || 'Unknown task';

    // Find the most recent Slack Bot entry for this task
    const { data: logs, error: logsErr } = await sb
      .from('task_updates')
      .select('id, text, created_at')
      .eq('task_id', parsed.matched_task_id)
      .eq('via', 'Slack Bot')
      .order('created_at', { ascending: false })
      .limit(1);

    if (logsErr || !logs?.length) {
      await say(`No recent bot changes found for *${taskName}* to revert.`);
      return;
    }

    const lastEntry = logs[0];
    const snapshot = decodeSnap(lastEntry.text);

    if (!snapshot) {
      await say(`Found a recent bot entry for *${taskName}* but it has no revert data.`);
      return;
    }

    // Format when the original change was made
    const changedAt = new Date(lastEntry.created_at).toLocaleTimeString('en-GB', {
      hour: '2-digit', minute: '2-digit', timeZone: 'America/Toronto'
    });
    const changedDate = new Date(lastEntry.created_at).toLocaleDateString('en-GB', {
      day: 'numeric', month: 'short', timeZone: 'America/Toronto'
    });

    // Apply snapshot back to task
    const revertData = {};
    const revertableFields = [
      'percent_complete', 'stage', 'priority', 'name', 'description',
      'lineage', 'assignees', 'start_date', 'due_date', 'follow_up_date',
      'estimated_hours', 'location', 'task_notes',
    ];
    for (const field of revertableFields) {
      if (snapshot[field] !== undefined) revertData[field] = snapshot[field];
    }

    const { error: revertErr } = await sb.from('tasks').update(revertData).eq('id', parsed.matched_task_id);
    if (revertErr) {
      console.error('revert error:', revertErr);
      await say(`Couldn't revert *${taskName}*. Check Render logs.`);
      return;
    }

    // Log the revert action
    await sb.from('task_updates').insert({
      task_id: parsed.matched_task_id,
      author: userName,
      text: `Reverted task to state before bot changes made at ${changedAt} on ${changedDate}.`,
      date: todayISO(),
      via: 'Slack Bot',
    });

    await say(`↩️ *${taskName}* reverted to state before changes made at ${changedAt} on ${changedDate}.`);
    return;
  }

  // ── Save any new memories Claude extracted ──
  await saveMemories(parsed.new_memories, text);

  // Fallback
  await say('I\'m not sure what you\'d like me to do. You can log an update, create a task, or say "undo" to revert a change.');
}

// ── MEMORY SAVE ──────────────────────────────────────────────
async function saveMemories(newMemories, sourceText) {
  if (!Array.isArray(newMemories) || !newMemories.length) return;

  for (const mem of newMemories) {
    if (!mem.category || !mem.key || !mem.value) continue;

    const key = mem.key.toLowerCase().trim();

    // Upsert — update value if key+category already exists, else insert
    const { error } = await sb.from('bot_memory').upsert(
      {
        category:   mem.category,
        key,
        value:      mem.value,
        source:     sourceText?.slice(0, 200) || null,
        updated_at: new Date().toISOString(),
      },
      { onConflict: 'category,key' }
    );

    if (error) {
      console.error('bot_memory upsert error:', error.message);
    } else {
      console.log(`Memory saved [${mem.category}] "${key}": ${mem.value}`);
    }
  }
}

// ── START ─────────────────────────────────────────────────────
(async () => {
  const port = process.env.PORT || 3000;
  await app.start(port);
  console.log(`Newforest Task Bot running on port ${port}`);
})();
