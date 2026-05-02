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

// ── SESSION STATE (resets on server restart) ─────────────────
const recentTask      = new Map(); // slackUserId → { id, name }
const creationSession = new Map(); // slackUserId → { step, data }

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

const PRIORITY_EMOJI = { urgent: '🔴', high: '🟠', medium: '🟡', low: '🟢' };

const SNAP_DELIMITER = '\n[SNAP]:';

const WIZARD_STEPS = ['name', 'description', 'assignees', 'location', 'priority', 'lineage', 'due_date'];

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
  try { return JSON.parse(text.slice(idx + SNAP_DELIMITER.length)); }
  catch { return null; }
}

function parsePriority(text) {
  const t = text.toLowerCase();
  if (/urgent|asap|emergency/.test(t)) return 'urgent';
  if (/high|important/.test(t))        return 'high';
  if (/low|whenever|can wait/.test(t)) return 'low';
  if (/med/.test(t))                   return 'medium';
  // exact match
  if (['urgent','high','medium','low'].includes(t.trim())) return t.trim();
  return null;
}

function parseLineage(text) {
  const t = text.toLowerCase();
  return LINEAGES.find(l => t.includes(l.toLowerCase())) ||
         LINEAGES.find(l => l.toLowerCase().split(/[\s&]+/).some(word => word.length > 3 && t.includes(word))) ||
         null;
}

function parseDate(text) {
  const t = text.trim().toLowerCase();
  const now = new Date();
  if (/^skip$|^none$|^no$/i.test(t)) return null;

  // YYYY-MM-DD
  if (/^\d{4}-\d{2}-\d{2}$/.test(t)) return t;

  // "tomorrow"
  if (t === 'tomorrow') {
    const d = new Date(now); d.setDate(d.getDate() + 1);
    return d.toISOString().split('T')[0];
  }

  // "next Monday" / "this Friday" / just "Friday"
  const days = ['sunday','monday','tuesday','wednesday','thursday','friday','saturday'];
  const match = t.match(/(?:next\s+)?(\w+day)/);
  if (match) {
    const target = days.indexOf(match[1]);
    if (target !== -1) {
      const d = new Date(now);
      const diff = (target - d.getDay() + 7) % 7 || 7;
      d.setDate(d.getDate() + diff);
      return d.toISOString().split('T')[0];
    }
  }

  // "May 10" / "10 May"
  const months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'];
  const mMatch = t.match(/(\d{1,2})\s+(\w+)|(\w+)\s+(\d{1,2})/);
  if (mMatch) {
    const [day, mon] = mMatch[1] ? [mMatch[1], mMatch[2]] : [mMatch[4], mMatch[3]];
    const mIdx = months.findIndex(m => mon.toLowerCase().startsWith(m));
    if (mIdx !== -1) {
      const year = now.getFullYear();
      return `${year}-${String(mIdx + 1).padStart(2,'0')}-${String(day).padStart(2,'0')}`;
    }
  }

  return null; // couldn't parse
}

function stepQuestion(step) {
  switch (step) {
    case 'name':
      return `What's the task name?\n_At any point say *skip all* to jump straight to the summary, or *cancel* to abort._`;
    case 'description':
      return `Give me a brief description of the work involved.\n_Say *skip* to leave blank._`;
    case 'priority':
      return `What's the priority level?\n\n🔴 *urgent*  ·  🟠 *high*  ·  🟡 *medium*  ·  🟢 *low*\n\n_Say *skip* for medium._`;
    case 'assignees':
      return `Who should be assigned? Name one or more people (e.g. _Dwayne_, _Dwayne and Noah_).\n_Say *nobody* or *skip* to leave unassigned._`;
    case 'lineage':
      return `Which department does this belong to?\n\n> ${LINEAGES.join('  ·  ')}\n\n_Say *skip* if unsure._`;
    case 'location':
      return `Any specific location on the property?\n_Say *skip* to leave blank._`;
    case 'due_date':
      return `When is this due? (e.g. _May 15_, _next Friday_, _2026-05-20_)\n_Say *skip* if no deadline yet._`;
  }
}

function buildSummary(data) {
  const pri = data.priority || 'medium';
  const emoji = PRIORITY_EMOJI[pri] || '🟡';
  const lines = [
    `Here's a summary of your new task — let me know if anything looks wrong:\n`,
    `📋 *${data.name}*`,
  ];
  if (data.description)           lines.push(`> 📝 ${data.description}`);
  lines.push(`> ${emoji} Priority: *${pri}*${data.lineage ? `  ·  🏢 Department: *${data.lineage}*` : ''}`);
  lines.push(`> 👤 Assigned to: *${data.assignees?.length ? data.assignees.join(', ') : 'Nobody'}*`);
  if (data.location)              lines.push(`> 📍 Location: ${data.location}`);
  if (data.due_date)              lines.push(`> 📅 Due: ${data.due_date}`);
  lines.push(`\nType *confirm* to create it, *cancel* to start over, or correct anything by restarting.`);
  return lines.join('\n');
}

// ── WIZARD SESSION HANDLER ───────────────────────────────────
async function handleCreationSession(slackUserId, userName, text, say) {
  const session = creationSession.get(slackUserId);
  const t = text.trim();

  // Cancel at any point
  if (/^(cancel|stop|quit|abort|never mind|forget it)/i.test(t)) {
    creationSession.delete(slackUserId);
    await say('No problem — task creation cancelled. 👍');
    return;
  }

  // Skip all remaining questions → jump to summary
  if (/^skip all$/i.test(t) && step !== 'confirm') {
    session.step = 'confirm';
    await say(buildSummary(data));
    return;
  }

  const { step, data } = session;
  const isSkip = /^(skip|none|no|nobody|n\/a)$/i.test(t);

  // ── Confirm step ──
  if (step === 'confirm') {
    if (/^(confirm|yes|create|do it|go ahead|ok|yep|yeah)/i.test(t)) {
      await finaliseTask(slackUserId, userName, data, say);
    } else {
      await say('Type *confirm* to create the task, or *cancel* to start over.');
    }
    return;
  }

  // ── Process current step ──
  switch (step) {
    case 'name':
      if (!t || isSkip) { await say('A task name is required. What would you like to call this task?'); return; }
      data.name = t;
      break;

    case 'description':
      if (!isSkip) data.description = t;
      break;

    case 'priority':
      if (!isSkip) {
        const p = parsePriority(t);
        data.priority = p || 'medium';
        if (!p && !isSkip) await say(`_I'll set that as medium — didn't quite catch the priority level._`);
      }
      break;

    case 'assignees':
      if (!isSkip) {
        data.assignees = t.split(/\s*(?:,|and|&)\s*/i).map(n => n.trim()).filter(Boolean);
      }
      break;

    case 'lineage':
      if (!isSkip) {
        const l = parseLineage(t);
        data.lineage = l || t; // store as-is if we can't match — Claude may have been smarter
        if (!l) await say(`_Couldn't match that exactly — I'll store it as "${t}"._`);
      }
      break;

    case 'location':
      if (!isSkip) data.location = t;
      break;

    case 'due_date':
      if (!isSkip) {
        const d = parseDate(t);
        data.due_date = d;
        if (!d && !isSkip) await say(`_Couldn't parse that date — I'll leave the due date blank._`);
      }
      break;
  }

  // ── Advance to next step ──
  const currentIdx = WIZARD_STEPS.indexOf(step);
  const nextStep = WIZARD_STEPS[currentIdx + 1];

  if (nextStep) {
    session.step = nextStep;
    await say(stepQuestion(nextStep));
  } else {
    // All steps done — show summary
    session.step = 'confirm';
    await say(buildSummary(data));
  }
}

// ── START WIZARD ─────────────────────────────────────────────
async function startWizard(slackUserId, say, prefill = {}) {
  creationSession.set(slackUserId, { step: 'name', data: { ...prefill } });

  // If name is already pre-filled, skip to description
  if (prefill.name) {
    const session = creationSession.get(slackUserId);
    session.step = 'description';
    await say(
      `Sure! Let's set up a new task. 📋\n\nI've got the name as *${prefill.name}*.\n\n` +
      stepQuestion('description')
    );
  } else {
    await say(`Sure! Let's set up a new task. 📋\n\n${stepQuestion('name', 1)}`);
  }
}

// ── FINALISE TASK CREATION ───────────────────────────────────
async function finaliseTask(slackUserId, userName, data, say) {
  // Fetch template defaults
  const { data: tmplRows } = await sb
    .from('task_templates')
    .select('*')
    .eq('is_default', true)
    .limit(1);
  const tmpl = tmplRows?.[0] || {};

  const newTask = {
    name:               data.name,
    description:        data.description        || tmpl.description           || '',
    priority:           data.priority            || tmpl.default_priority      || 'medium',
    stage:              tmpl.default_stage                                     || 'assigned',
    assignees:          data.assignees?.length   ? data.assignees : (tmpl.default_assignees || []),
    lineage:            data.lineage             || tmpl.default_lineage       || null,
    estimated_hours:    data.estimated_hours     || tmpl.default_estimated_hours || null,
    weather_dependent:  tmpl.default_weather_dependent                         || 'no',
    steps:              tmpl.default_steps                                     || [],
    materials:          tmpl.default_materials                                 || [],
    tools:              tmpl.default_tools                                     || [],
    task_notes:         tmpl.default_task_notes                                || null,
    percent_complete:   0,
    creator_name:       userName,
    start_date:         data.start_date  || null,
    due_date:           data.due_date    || null,
    location:           data.location   || null,
  };

  const { data: created, error: createErr } = await sb
    .from('tasks').insert(newTask).select('id').single();

  if (createErr) {
    console.error('task create error:', createErr);
    await say('Something went wrong creating the task. Check Render logs.');
    creationSession.delete(slackUserId);
    return;
  }

  await sb.from('task_updates').insert({
    task_id: created.id,
    author:  userName,
    text:    `Task created via Slack by ${userName}.`,
    date:    todayISO(),
    via:     'Slack Bot',
  });

  recentTask.set(slackUserId, { id: created.id, name: data.name });
  creationSession.delete(slackUserId);

  const pri = data.priority || 'medium';
  const emoji = PRIORITY_EMOJI[pri] || '🟡';
  const details = [];
  if (pri)                  details.push(`${emoji} ${pri}`);
  if (data.lineage)         details.push(`${data.lineage}`);
  if (data.assignees?.length) details.push(`👤 ${data.assignees.join(', ')}`);
  if (data.due_date)        details.push(`📅 ${data.due_date}`);

  let confirm = `✅ Task created: *${data.name}* — ${nowLabel()}`;
  if (details.length) confirm += `\n> ${details.join('  ·  ')}`;
  await say(confirm);
}

// ── MAIN MESSAGE HANDLER ─────────────────────────────────────
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
  } catch (e) { console.error('users.info error:', e.message); }

  // ── 2. Active creation wizard? Handle it directly ──
  if (creationSession.has(slackUserId)) {
    await handleCreationSession(slackUserId, userName, text, say);
    return;
  }

  // ── 3. Fetch active tasks ──
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

  // ── 4. Fetch bot memory ──
  const { data: memories } = await sb
    .from('bot_memory')
    .select('category, key, value')
    .order('times_used', { ascending: false });

  const memoryContext = memories?.length
    ? '\nLearned memory (apply these facts to improve accuracy):\n' +
      memories.map(m => `- [${m.category}] "${m.key}": ${m.value}`).join('\n')
    : '';

  // ── 5. Call Claude for intent ──
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
When the message wants to create a new task. Extract any info already provided.
{
  "intent": "create_task",
  "prefill": {
    "name": "task name if mentioned, else null",
    "description": "description if mentioned, else null",
    "priority": "priority if mentioned, else null",
    "assignees": ["Name"] or [],
    "lineage": "lineage if mentioned, else null",
    "location": "location if mentioned, else null",
    "due_date": "YYYY-MM-DD if mentioned, else null"
  },
  "new_memories": []
}

────────────────────────────────
INTENT: update_task
When the message is a progress report or field change on an existing task.
{
  "intent": "update_task",
  "matched_task_id": "uuid",
  "confidence": "high|low",
  "update_text": "human-readable progress note",
  "field_changes": {
    "percent_complete": null or 0-100,
    "stage": null or "assigned|inprogress|review|complete",
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
  "clarification_question": null,
  "new_memories": []
}

────────────────────────────────
INTENT: query_task
When asking for info or status on a task.
{
  "intent": "query_task",
  "matched_task_id": "uuid or null",
  "confidence": "high|low",
  "new_memories": []
}

────────────────────────────────
INTENT: revert
When asking to undo the last bot change.
{
  "intent": "revert",
  "matched_task_id": "uuid or null",
  "confidence": "high|low",
  "new_memories": []
}

────────────────────────────────
INTENT: unclear
LAST RESORT — only if message is completely unintelligible.
{
  "intent": "unclear",
  "needs_clarification": true,
  "clarification_question": "one short specific question",
  "new_memories": []
}

────────────────────────────────
MEMORY EXTRACTION
Add genuinely useful facts to new_memories. Leave empty if nothing new.
"new_memories": [{ "category": "alias|location|worker|lineage|correction|general", "key": "lowercase term", "value": "fact to remember" }]

Rules:
- "create", "new task", "add task", "need to add" → intent: create_task
- Match existing tasks generously — workers use shorthand
- "done"/"finished" → stage: complete, percent_complete: 100
- "started"/"working on" → stage: inprogress
- "ready for check" → stage: review
- "halfway" → 50, "nearly done" → 90
- "assign to X"/"add X" → add_assignees; "remove X" → remove_assignees
- "tell me about"/"info on"/"details on"/"status of" → query_task
- "undo"/"revert"/"roll back" → revert
- If message uses "that"/"it" without a task name, use the most recently touched task
- If a task name or reference is mentioned, NEVER use intent: unclear — always attempt a match
- NEVER ask the user to repeat info they already gave`
      }]
    });

    rawResponse = response.content[0].text.trim();
    rawResponse = rawResponse.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/, '').trim();
    console.log('Claude raw response:', rawResponse);
    parsed = JSON.parse(rawResponse);
  } catch (e) {
    console.error('Claude/parse error:', e.message, rawResponse);
    await say('Sorry, I had trouble processing that. Check Render logs for details.');
    return;
  }

  if (parsed.needs_clarification) {
    await say(parsed.clarification_question || 'Could you clarify that a bit more?');
    return;
  }

  // ── 6A. CREATE TASK → start wizard ──────────────────────────
  if (parsed.intent === 'create_task') {
    const prefill = {};
    const p = parsed.prefill || {};
    if (p.name)              prefill.name        = p.name;
    if (p.description)       prefill.description = p.description;
    if (p.priority)          prefill.priority    = p.priority;
    if (p.assignees?.length) prefill.assignees   = p.assignees;
    if (p.lineage)           prefill.lineage     = p.lineage;
    if (p.location)          prefill.location    = p.location;
    if (p.due_date)          prefill.due_date    = p.due_date;

    await startWizard(slackUserId, say, prefill);
    await saveMemories(parsed.new_memories, text);
    return;
  }

  const today = todayISO();

  // ── 6B. QUERY TASK ───────────────────────────────────────────
  if (parsed.intent === 'query_task') {
    if (!parsed.matched_task_id) {
      await say('I couldn\'t find that task. Could you give me the task name?');
      return;
    }

    const task = (tasks || []).find(t => t.id === parsed.matched_task_id);
    if (!task) {
      await say('That task doesn\'t appear to be active. It may already be complete.');
      return;
    }

    const stageLabel = STAGE_LABELS[task.stage] || task.stage;
    const assignees  = task.assignees?.join(', ') || 'Nobody';
    const pri = task.priority || 'medium';
    const lines = [
      `*${task.name}*`,
      `> ${PRIORITY_EMOJI[pri] || '🟡'} Priority: ${pri}  ·  Stage: ${stageLabel} (${task.percent_complete}%)`,
      `> 👤 Assigned to: ${assignees}`,
    ];
    if (task.lineage)         lines.push(`> 🏢 Department: ${task.lineage}`);
    if (task.due_date)        lines.push(`> 📅 Due: ${task.due_date}`);
    if (task.follow_up_date)  lines.push(`> 🔔 Follow-up: ${task.follow_up_date}`);
    if (task.location)        lines.push(`> 📍 Location: ${task.location}`);
    if (task.estimated_hours) lines.push(`> ⏱ Estimated: ${task.estimated_hours}h`);
    if (task.description)     lines.push(`> _${task.description}_`);

    recentTask.set(slackUserId, { id: task.id, name: task.name });
    await say(lines.join('\n'));
    await saveMemories(parsed.new_memories, text);
    return;
  }

  // ── 6C. UPDATE TASK ──────────────────────────────────────────
  if (parsed.intent === 'update_task') {
    if (!parsed.matched_task_id) {
      await say('I couldn\'t match that to any active task. Could you mention the task name?');
      return;
    }

    const task = (tasks || []).find(t => t.id === parsed.matched_task_id);
    const taskName = task?.name || 'Unknown task';
    const fc = parsed.field_changes || {};

    const snapshot = {
      percent_complete: task?.percent_complete, stage: task?.stage,
      priority: task?.priority, name: task?.name, description: task?.description,
      lineage: task?.lineage, assignees: task?.assignees, start_date: task?.start_date,
      due_date: task?.due_date, follow_up_date: task?.follow_up_date,
      estimated_hours: task?.estimated_hours, location: task?.location, task_notes: task?.task_notes,
    };

    const dbUpdate = {};
    const changeLines = [];

    if (fc.percent_complete !== null && fc.percent_complete !== undefined) {
      dbUpdate.percent_complete = fc.percent_complete;
      changeLines.push(`Progress: ${task?.percent_complete ?? '?'}% → ${fc.percent_complete}%`);
    }
    if (fc.stage) {
      dbUpdate.stage = fc.stage;
      changeLines.push(`Stage: ${STAGE_LABELS[task?.stage] || task?.stage} → ${STAGE_LABELS[fc.stage]}`);
    }
    if (fc.priority) {
      dbUpdate.priority = fc.priority;
      changeLines.push(`Priority: ${task?.priority} → ${fc.priority}`);
    }
    if (fc.name) {
      dbUpdate.name = fc.name;
      changeLines.push(`Name: "${taskName}" → "${fc.name}"`);
    }
    if (fc.description) { dbUpdate.description = fc.description; changeLines.push(`Description updated`); }
    if (fc.lineage) {
      dbUpdate.lineage = fc.lineage;
      changeLines.push(`Lineage: ${task?.lineage || 'none'} → ${fc.lineage}`);
    }
    if (fc.start_date)     { dbUpdate.start_date     = fc.start_date;     changeLines.push(`Start: ${fc.start_date}`); }
    if (fc.due_date)       { dbUpdate.due_date       = fc.due_date;       changeLines.push(`Due: ${fc.due_date}`); }
    if (fc.follow_up_date) { dbUpdate.follow_up_date = fc.follow_up_date; changeLines.push(`Follow-up: ${fc.follow_up_date}`); }
    if (fc.estimated_hours !== null && fc.estimated_hours !== undefined) {
      dbUpdate.estimated_hours = fc.estimated_hours;
      changeLines.push(`Hours: ${fc.estimated_hours}`);
    }
    if (fc.location) { dbUpdate.location = fc.location; changeLines.push(`Location: ${fc.location}`); }
    if (fc.notes) {
      const existing = task?.task_notes || '';
      dbUpdate.task_notes = existing ? `${existing}\n\n[${today} via Slack] ${fc.notes}` : `[${today} via Slack] ${fc.notes}`;
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

    const logParts = [];
    if (parsed.update_text) logParts.push(parsed.update_text);
    if (changeLines.length) logParts.push(`Fields changed: ${changeLines.join(' · ')}`);

    await sb.from('task_updates').insert({
      task_id: parsed.matched_task_id,
      author:  userName,
      text:    logParts.join('\n') + encodeSnap(snapshot),
      date:    today,
      via:     'Slack Bot',
    });

    recentTask.set(slackUserId, { id: parsed.matched_task_id, name: fc.name || taskName });

    const confidenceNote = parsed.confidence === 'low' ? ' _(low confidence)_' : '';
    let confirm = `✅ *${fc.name || taskName}* updated — ${nowLabel()}${confidenceNote}`;
    if (changeLines.length) confirm += `\n> ${changeLines.join('  ·  ')}`;
    confirm += `\n_Say "undo that" to revert._`;
    await say(confirm);
    await saveMemories(parsed.new_memories, text);
    return;
  }

  // ── 6D. REVERT ───────────────────────────────────────────────
  if (parsed.intent === 'revert') {
    if (!parsed.matched_task_id) {
      await say('Which task should I revert? Could you mention the task name?');
      return;
    }

    const task = (tasks || []).find(t => t.id === parsed.matched_task_id);
    const taskName = task?.name || 'Unknown task';

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
    const snapshot  = decodeSnap(lastEntry.text);
    if (!snapshot) {
      await say(`Found a recent bot entry for *${taskName}* but it has no revert data.`);
      return;
    }

    const changedAt   = new Date(lastEntry.created_at).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', timeZone: 'America/Toronto' });
    const changedDate = new Date(lastEntry.created_at).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', timeZone: 'America/Toronto' });

    const revertData = {};
    const revertFields = ['percent_complete','stage','priority','name','description','lineage','assignees','start_date','due_date','follow_up_date','estimated_hours','location','task_notes'];
    for (const field of revertFields) {
      if (snapshot[field] !== undefined) revertData[field] = snapshot[field];
    }

    const { error: revertErr } = await sb.from('tasks').update(revertData).eq('id', parsed.matched_task_id);
    if (revertErr) {
      console.error('revert error:', revertErr);
      await say(`Couldn't revert *${taskName}*. Check Render logs.`);
      return;
    }

    await sb.from('task_updates').insert({
      task_id: parsed.matched_task_id,
      author:  userName,
      text:    `Reverted to state before bot changes made at ${changedAt} on ${changedDate}.`,
      date:    today,
      via:     'Slack Bot',
    });

    await say(`↩️ *${taskName}* reverted to state before changes made at ${changedAt} on ${changedDate}.`);
    await saveMemories(parsed.new_memories, text);
    return;
  }

  // Fallback
  await say('Not sure what you\'d like — you can update a task, create a new one, ask for task details, or say "undo" to revert a change.');
}

// ── MEMORY SAVE ──────────────────────────────────────────────
async function saveMemories(newMemories, sourceText) {
  if (!Array.isArray(newMemories) || !newMemories.length) return;
  for (const mem of newMemories) {
    if (!mem.category || !mem.key || !mem.value) continue;
    const key = mem.key.toLowerCase().trim();
    const { error } = await sb.from('bot_memory').upsert(
      { category: mem.category, key, value: mem.value, source: sourceText?.slice(0, 200) || null, updated_at: new Date().toISOString() },
      { onConflict: 'category,key' }
    );
    if (error) console.error('bot_memory upsert error:', error.message);
    else console.log(`Memory saved [${mem.category}] "${key}": ${mem.value}`);
  }
}

// ── START ─────────────────────────────────────────────────────
(async () => {
  const port = process.env.PORT || 3000;
  await app.start(port);
  console.log(`Newforest Task Bot running on port ${port}`);
})();
