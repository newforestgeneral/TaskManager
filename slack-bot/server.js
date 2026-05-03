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

// Ignore Slack retry events — when the bot takes >3 s, Slack resends the
// event. Without this, the handler runs twice and the user gets double replies.
app.use(async ({ next, context }) => {
  if (context.retryNum) {
    console.log(`Slack retry #${context.retryNum} ignored`);
    return;
  }
  await next();
});

const sb = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

const claude = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

// ── SESSION STATE (resets on server restart) ─────────────────
const recentTask      = new Map(); // slackUserId → { id, name }
const recentList      = new Map(); // slackUserId → [{ id, name }, ...] — last list shown
const creationSession = new Map(); // slackUserId → { step, data }
const disambigSession = new Map(); // slackUserId → { options:[{label,value}], onResolve }

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

function capitalize(str) {
  if (!str) return '';
  return str.charAt(0).toUpperCase() + str.slice(1);
}

// Synchronous — matches a name/abbreviation against a pre-fetched workers array.
// Used by both resolveWorkerName() and the list_tasks filter.
// Returns the resolved first name (capitalized) or null if no match.
function matchWorkerName(name, workers) {
  if (!name || !workers?.length) return null;
  const nl = name.toLowerCase().trim();

  // 1. Exact key match
  const exact = workers.find(w => w.key === nl);
  if (exact) return capitalize(exact.key.split(' ')[0]);

  // 2. Input starts with a key ("Keith Macabenta" → "Keith")
  const sw = workers.find(w => nl === w.key || nl.startsWith(w.key + ' '));
  if (sw) return capitalize(sw.key.split(' ')[0]);

  // 3. Key starts with input ("Keith" matches key "keith macabenta")
  const ww = workers.find(w => w.key.startsWith(nl + ' ') || w.key === nl);
  if (ww) return capitalize(ww.key.split(' ')[0]);

  // 4. Initials — e.g. "KM" → "Keith Macabenta"
  //    Checks key words and full name extracted from the value field.
  if (/^[A-Z]{1,6}$/i.test(name.trim().replace(/\s/g, ''))) {
    const initials = name.trim().replace(/\s/g, '').toUpperCase();
    for (const w of workers) {
      const keyInitials = w.key.split(/\s+/).map(p => p[0]?.toUpperCase() || '').join('');
      if (keyInitials === initials) return capitalize(w.key.split(' ')[0]);

      const fullNameMatch = w.value.match(/^([^.]+?)\s+is\s/i);
      if (fullNameMatch) {
        const valueWords = fullNameMatch[1].trim().split(/\s+/);
        const valueInitials = valueWords.map(p => p[0]?.toUpperCase() || '').join('');
        if (valueInitials === initials) return capitalize(valueWords[0]);
      }
    }
  }

  // 5. Single letter — first worker whose name starts with that letter
  if (/^[A-Z]$/i.test(name.trim())) {
    const letter = name.trim().toLowerCase();
    const first = workers.find(w => w.key.startsWith(letter));
    if (first) return capitalize(first.key.split(' ')[0]);
  }

  return null;
}

// Score tasks by how many words from the message appear in the task name.
// Returns up to `limit` best matches.
function findCandidateTasks(text, tasks, limit = 4) {
  const words = text.toLowerCase().split(/\s+/).filter(w => w.length > 2);
  if (!words.length) return [];
  return tasks
    .map(t => {
      const name = t.name.toLowerCase();
      const score = words.reduce((s, w) => s + (name.includes(w) ? 1 : 0), 0);
      return { task: t, score };
    })
    .filter(x => x.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, limit)
    .map(x => x.task);
}

function stepQuestion(step) {
  switch (step) {
    case 'name':
      return `What's the task name? _(*skip all* to jump to summary, *cancel* to quit)_`;
    case 'description':
      return `Brief description of the work? _(*skip* to leave blank)_`;
    case 'assignees':
      return `Who's on this one? _(e.g. Dwayne, Dwayne and Noah — or *skip*)_`;
    case 'location':
      return `Where on the property? _(*skip* if not specific)_`;
    case 'priority':
      return `Priority — 🔴 *urgent* · 🟠 *high* · 🟡 *medium* · 🟢 *low* _(*skip* to leave unset)_`;
    case 'lineage':
      return `Department?\n> ${LINEAGES.join('  ·  ')}\n_(*skip* if unsure)_`;
    case 'due_date':
      return `Due date? _(May 15, next Friday, 2026-05-20 — or *skip*)_`;
  }
}

function buildSummary(data) {
  const pri = data.priority;
  const emoji = pri ? (PRIORITY_EMOJI[pri] || '⚪') : '⚪';
  const lines = [`Here's what I've got — look right?\n`, `📋 *${data.name}*`];
  if (data.description)              lines.push(`> 📝 ${data.description}`);
  if (pri)                           lines.push(`> ${emoji} ${pri}${data.lineage ? `  ·  🏢 ${data.lineage}` : ''}`);
  else if (data.lineage)             lines.push(`> 🏢 ${data.lineage}`);
  lines.push(`> 👤 ${data.assignees?.length ? data.assignees.join(', ') : 'Nobody assigned'}`);
  if (data.location)                 lines.push(`> 📍 ${data.location}`);
  if (data.due_date)                 lines.push(`> 📅 Due ${data.due_date}`);
  lines.push(`\n*confirm* to create · *cancel* to scrap it`);
  return lines.join('\n');
}

// ── DISAMBIGUATION HANDLER ───────────────────────────────────
// Called when a numbered-choice prompt is awaiting user input.
async function handleDisambig(slackUserId, text, say) {
  const session = disambigSession.get(slackUserId);
  const t = text.trim();

  if (/^(cancel|stop|quit|nevermind|forget it)/i.test(t)) {
    disambigSession.delete(slackUserId);
    await say('No worries — cancelled. 👍');
    return;
  }

  // Try numeric selection first
  const num = parseInt(t);
  if (!isNaN(num) && num >= 1 && num <= session.options.length) {
    const chosen = session.options[num - 1];
    disambigSession.delete(slackUserId);
    await session.onResolve(chosen.value, say);
    return;
  }

  // Try matching by typed text (e.g. user types "Keith" instead of "1")
  const textMatch = session.options.find(o => o.label.toLowerCase().startsWith(t.toLowerCase()));
  if (textMatch) {
    disambigSession.delete(slackUserId);
    await session.onResolve(textMatch.value, say);
    return;
  }

  // Didn't understand — re-prompt
  const list = session.options.map((o, i) => `${i + 1}. ${o.label}`).join('\n');
  await say(`Type the number of your choice:\n${list}`);
}

// ── WIZARD SESSION HANDLER ───────────────────────────────────
async function handleCreationSession(slackUserId, userName, text, say) {
  const session = creationSession.get(slackUserId);
  const t = text.trim();

  // Cancel at any point
  if (/^(cancel|stop|quit|abort|never mind|forget it)/i.test(t)) {
    creationSession.delete(slackUserId);
    await say('Scrapped. 👍');
    return;
  }

  const { step, data } = session;

  // Skip all remaining questions → jump to summary
  if (/^skip all$/i.test(t) && step !== 'confirm') {
    session.step = 'confirm';
    await say(buildSummary(data));
    return;
  }
  const isSkip = /^(skip|none|no|nobody|n\/a)$/i.test(t);

  // ── Confirm step ──
  if (step === 'confirm') {
    if (/^(confirm|yes|create|do it|go ahead|ok|okay|yep|yeah|yup|looks good|good|correct|right|sure|sounds good|perfect|great)/i.test(t) || /^y$/i.test(t)) {
      await finaliseTask(slackUserId, userName, data, say);
    } else {
      await say('*confirm* to create it or *cancel* to start over.');
    }
    return;
  }

  // ── Process current step ──
  switch (step) {
    case 'name':
      if (!t || isSkip) { await say('Need a name for this one — what should I call it?'); return; }
      data.name = t;
      break;

    case 'description':
      if (!isSkip) data.description = t;
      break;

    case 'priority':
      if (!isSkip) {
        const p = parsePriority(t);
        if (p) {
          data.priority = p;
        } else {
          await say(`Didn't catch that — urgent, high, medium, low, or skip?`);
          return;
        }
      }
      break;

    case 'assignees': {
      if (!isSkip) {
        const parts = t.split(/\s*(?:,|and|&)\s*/i).map(n => n.trim()).filter(Boolean);

        // Look up known workers in bot_memory to catch partial/ambiguous names
        const { data: workers } = await sb.from('bot_memory').select('key').eq('category', 'worker');
        const workerKeys = (workers || []).map(w => w.key);

        const resolved = [];
        let ambiguousPart = null;
        let ambiguousMatches = [];

        for (const part of parts) {
          const pl = part.toLowerCase().trim();
          if (!pl) continue;
          const exact = workerKeys.find(k => k === pl);
          if (exact) { resolved.push(capitalize(exact)); continue; }
          const partials = workerKeys.filter(k => k.startsWith(pl));
          if (partials.length === 1) {
            resolved.push(capitalize(partials[0]));
          } else if (partials.length > 1) {
            ambiguousPart = part;
            ambiguousMatches = partials;
            break; // handle one ambiguity at a time
          } else {
            resolved.push(part); // unknown name — store as typed
          }
        }

        if (ambiguousPart) {
          // Pause the wizard and ask for clarification
          const capturedSession = session;
          disambigSession.set(slackUserId, {
            options: ambiguousMatches.map(k => ({ label: capitalize(k), value: capitalize(k) })),
            onResolve: async (chosenName, say) => {
              capturedSession.data.assignees = [...resolved, chosenName];
              const nextIdx = WIZARD_STEPS.indexOf('assignees') + 1;
              const nextStep = WIZARD_STEPS[nextIdx];
              capturedSession.step = nextStep || 'confirm';
              await say(nextStep ? stepQuestion(nextStep) : buildSummary(capturedSession.data));
            }
          });
          const list = ambiguousMatches.map((k, i) => `${i + 1}. ${capitalize(k)}`).join('\n');
          await say(`Who did you mean?\n${list}\n_Type the number._`);
          return;
        }

        data.assignees = resolved;
      }
      break;
    }

    case 'lineage':
      if (!isSkip) {
        const l = parseLineage(t);
        data.lineage = l || t; // store as-is if we can't match — Claude may have been smarter
        if (!l) await say(`Storing it as "${t}" — you can update it in the app if needed.`);
      }
      break;

    case 'location':
      if (!isSkip) data.location = t;
      break;

    case 'due_date':
      if (!isSkip) {
        const d = parseDate(t);
        data.due_date = d;
        if (!d && !isSkip) await say(`Couldn't read that date — leaving it blank, you can add it in the app.`);
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
    await say(`Got it — new task: *${prefill.name}*\n\n${stepQuestion('description')}`);
  } else {
    await say(`New task — let's go. 📋\n\n${stepQuestion('name')}`);
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
    priority:           data.priority            || tmpl.default_priority      || null,
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
    await say('Hit an error saving that — check Render logs.');
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

  // Remember anyone assigned
  await ensureWorkerMemory(newTask.assignees);

  const pri = data.priority || 'medium';
  const emoji = PRIORITY_EMOJI[pri] || '🟡';
  const details = [];
  if (pri)                  details.push(`${emoji} ${pri}`);
  if (data.lineage)         details.push(`${data.lineage}`);
  if (data.assignees?.length) details.push(`👤 ${data.assignees.join(', ')}`);
  if (data.due_date)        details.push(`📅 ${data.due_date}`);

  let confirm = `✅ *${data.name}* created — ${nowLabel()}`;
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

  // ── 1. Get Slack user's real name and resolve to known worker ──
  let userName = 'Unknown';
  try {
    const info = await client.users.info({ user: slackUserId });
    // Prefer display_name → real_name → username, in that order
    const rawName = info.user.profile?.display_name || info.user.profile?.real_name || info.user.real_name || info.user.name;
    console.log(`Slack identity: userId=${slackUserId} rawName="${rawName}"`);
    userName = await resolveWorkerName(slackUserId, rawName);
    console.log(`Resolved userName="${userName}"`);
  } catch (e) { console.error('users.info error:', e.message); }

  // ── 2. Disambiguation awaiting? Handle it first ──
  if (disambigSession.has(slackUserId)) {
    await handleDisambig(slackUserId, text, say);
    return;
  }

  // ── 3. Active creation wizard? Handle it directly ──
  if (creationSession.has(slackUserId)) {
    await handleCreationSession(slackUserId, userName, text, say);
    return;
  }

  // ── 3. Short-circuit: obvious follow-up on the last task ──
  const last = recentTask.get(slackUserId);
  if (last && /^(more info|more info on (that|it|this)|more details|tell me more|details|expand|what about it|show me more|more on that|info on that|more)$/i.test(text.trim())) {
    // Treat as query_task on the last touched task — no need to call Claude
    const { data: taskRow } = await sb
      .from('tasks')
      .select('id, name, stage, percent_complete, assignees, lineage, priority, description, start_date, due_date, follow_up_date, estimated_hours, location, task_notes')
      .eq('id', last.id)
      .single();

    if (taskRow) {
      const stageLabel = STAGE_LABELS[taskRow.stage] || taskRow.stage;
      const pri = taskRow.priority || null;
      const lines = [`*${taskRow.name}*`];
      lines.push(`> ${pri ? (PRIORITY_EMOJI[pri] + ' ' + pri) : 'No priority'}  ·  ${stageLabel} (${taskRow.percent_complete}%)`);
      lines.push(`> 👤 ${taskRow.assignees?.join(', ') || 'Nobody assigned'}`);
      if (taskRow.lineage)         lines.push(`> 🏢 ${taskRow.lineage}`);
      if (taskRow.due_date)        lines.push(`> 📅 Due ${taskRow.due_date}`);
      if (taskRow.follow_up_date)  lines.push(`> 🔔 Follow-up ${taskRow.follow_up_date}`);
      if (taskRow.location)        lines.push(`> 📍 ${taskRow.location}`);
      if (taskRow.estimated_hours) lines.push(`> ⏱ ${taskRow.estimated_hours}h estimated`);
      if (taskRow.description)     lines.push(`> _${taskRow.description}_`);
      await say(lines.join('\n'));
    } else {
      await say(`Can't find *${last.name}* — it may have been deleted.`);
    }
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
    await say('Can\'t reach the database right now — give it a second and try again.');
    return;
  }

  const taskList = (tasks || []).map(t =>
    `ID: ${t.id} | "${t.name}" | Stage: ${t.stage} | ${t.percent_complete}% | Priority: ${t.priority} | Assignees: ${(t.assignees || []).join(', ') || 'none'} | Lineage: ${t.lineage || 'none'}`
  ).join('\n');

  const lastTouched = recentTask.get(slackUserId);
  const lastList = recentList.get(slackUserId);

  const recentContext = [
    lastTouched ? `Most recently created/updated task by this user: ID: ${lastTouched.id} | "${lastTouched.name}"` : '',
    lastList?.length > 1
      ? `Tasks just listed to this user:\n${lastList.map(t => `  ID: ${t.id} | "${t.name}"`).join('\n')}`
      : '',
  ].filter(Boolean).join('\n');

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
      model: 'claude-sonnet-4-6',
      max_tokens: 1000,
      messages: [{
        role: 'user',
        content: `You are a task tracker assistant for Newforest, a Canadian retreat and meditation center undergoing construction on old and new buildings and landscaping.
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
When asking for info or status on ONE specific task.
{
  "intent": "query_task",
  "matched_task_id": "uuid or null",
  "confidence": "high|low",
  "new_memories": []
}

────────────────────────────────
INTENT: list_tasks
When asking for a list of tasks — filtered by assignee, stage, department, priority, or "all".
Examples: "show me all tasks", "what's assigned to Dwayne", "show me Keith's tasks",
          "show me all tasks assigned to me", "what tasks are in progress", "anything urgent".
{
  "intent": "list_tasks",
  "filter_assignee": "name or 'me' if referring to themselves, or null for no filter",
  "filter_stage": "assigned|inprogress|review|complete or null",
  "filter_lineage": "lineage name or null",
  "filter_priority": "low|medium|high|urgent or null",
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

Before choosing an intent, think about what the person is actually trying to do.
People message casually and imprecisely. Read the meaning, not just the words.

Thinking process (do this internally):
1. Is there a task name or description in the message? If yes, which active task is closest?
2. What action are they trying to take? Update, create, ask, undo?
3. If they used "it", "that", "the one" etc — they mean the most recently touched task.
4. If the message is vague but plausible — pick the most likely interpretation and go with it.
5. Only ask for clarification if you genuinely have no idea what they mean AND the most recently touched task doesn't help.

Signal patterns:
- "done", "finished", "all done", "wrapped up", "sorted" → stage: complete, percent: 100
- "started", "on it", "working on it", "underway", "begun" → stage: inprogress
- "ready for you", "ready for check", "check this" → stage: review
- "halfway", "half done" → 50 · "nearly done", "almost there" → 90
- "add X", "assign X", "put X on it" → add_assignees
- "take X off", "remove X", "unassign X" → remove_assignees
- "show me all tasks", "list tasks", "what's assigned to X", "X's tasks", "show me X's tasks", "tasks for X", "what am I working on", "my tasks", "what tasks are in progress/urgent/etc" → list_tasks
- If the filter name matches the sender's own name or is their likely initials/abbreviation, use filter_assignee: "me" rather than the literal string
- "tell me about", "what's happening with", "how's X going", "update on X", "info on" → query_task (single task)
- "undo", "go back", "revert", "roll back", "that was wrong" → revert
- "create", "new task", "add a task", "need to log", "can you add" → create_task
- A message with no matching active task that describes new work → create_task
- If task name is mentioned: ALWAYS attempt a match, never return unclear
- If the most recently touched task exists: use it for vague references like "that", "it", "that one", "more info on that"
- If a list of tasks was just shown and the user says "more info on that" or similar, and there is only one task in the recently listed tasks — use that task for query_task
- If multiple tasks were listed and the user says "more info on that" without specifying — use query_task with the first/most obvious match from the recently listed tasks
- NEVER use intent: unclear if you can make a reasonable guess`
      }]
    });

    rawResponse = response.content[0].text.trim();
    rawResponse = rawResponse.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/, '').trim();
    console.log('Claude raw response:', rawResponse);
    parsed = JSON.parse(rawResponse);
  } catch (e) {
    console.error('Claude/parse error:', e.message, rawResponse);
    await say('Something went wrong on my end — check Render logs.');
    return;
  }

  if (parsed.needs_clarification) {
    await say(parsed.clarification_question || 'Can you say that a different way — didn\'t quite get it.');
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

  // ── 6B. LIST TASKS ──────────────────────────────────────────
  if (parsed.intent === 'list_tasks') {
    let filtered = tasks || [];

    // Resolve the assignee filter — handles "me", initials, abbreviations, aliases.
    let assigneeFilter = parsed.filter_assignee === 'me' ? userName : parsed.filter_assignee;

    if (assigneeFilter && assigneeFilter !== userName) {
      // Fetch workers + aliases in one call
      const { data: allMem } = await sb.from('bot_memory').select('key, value, category');
      const workers = (allMem || []).filter(m => m.category === 'worker');
      const aliases  = (allMem || []).filter(m => m.category === 'alias');

      // 1. Try worker key / initials match
      const workerMatch = matchWorkerName(assigneeFilter, workers);
      if (workerMatch) {
        assigneeFilter = workerMatch;
      } else {
        // 2. Try saved aliases (e.g. Claude stored "km" → "KM refers to Keith")
        const aliasEntry = aliases.find(a => a.key === assigneeFilter.toLowerCase());
        if (aliasEntry) {
          // Pull the first known worker name that appears in the alias value
          const workerNames = workers.map(w => capitalize(w.key.split(' ')[0]));
          const hit = workerNames.find(wn => aliasEntry.value.includes(wn));
          if (hit) {
            assigneeFilter = hit;
          } else {
            // Fall back: first capitalized word in the alias value
            const cap = aliasEntry.value.match(/\b([A-Z][a-z]{1,})\b/);
            if (cap) assigneeFilter = cap[1];
          }
        } else {
          // 3. Check if the filter matches the sending user's own initials/prefix
          //    e.g. KM is Keith and types "KM tasks" — resolve to themselves
          const userWords = userName.split(/\s+/);
          const userInitials = userWords.map(w => w[0]?.toUpperCase() || '').join('');
          const filterUpper  = assigneeFilter.toUpperCase();
          if (
            filterUpper === userInitials ||
            userName.toLowerCase().startsWith(assigneeFilter.toLowerCase())
          ) {
            assigneeFilter = userName;
          }
        }
      }
    }

    if (assigneeFilter) {
      filtered = filtered.filter(t =>
        (t.assignees || []).some(a => a.toLowerCase().includes(assigneeFilter.toLowerCase()))
      );
    }
    if (parsed.filter_stage)    filtered = filtered.filter(t => t.stage === parsed.filter_stage);
    if (parsed.filter_lineage)  filtered = filtered.filter(t => t.lineage?.toLowerCase().includes(parsed.filter_lineage.toLowerCase()));
    if (parsed.filter_priority) filtered = filtered.filter(t => t.priority === parsed.filter_priority);

    if (!filtered.length) {
      const who = assigneeFilter ? ` for *${assigneeFilter}*` : '';
      await say(`No active tasks found${who}.`);
      await saveMemories(parsed.new_memories, text);
      return;
    }

    // Store list context so follow-up messages can reference it
    recentList.set(slackUserId, filtered.map(t => ({ id: t.id, name: t.name })));

    // If only one result, set it as the recent task too — makes "more info on that" work
    if (filtered.length === 1) {
      recentTask.set(slackUserId, { id: filtered[0].id, name: filtered[0].name });
    }

    const who = assigneeFilter ? ` assigned to *${assigneeFilter}*` : '';
    const stageFilter = parsed.filter_stage ? ` · ${STAGE_LABELS[parsed.filter_stage] || parsed.filter_stage}` : '';
    let reply = `*${filtered.length} active task${filtered.length > 1 ? 's' : ''}${who}${stageFilter}:*\n`;

    reply += filtered.map(t => {
      const pri = t.priority ? `${PRIORITY_EMOJI[t.priority] || '⚪'} ` : '';
      const stage = STAGE_LABELS[t.stage] || t.stage;
      const pct = t.percent_complete > 0 ? ` · ${t.percent_complete}%` : '';
      const assignees = t.assignees?.length ? ` · 👤 ${t.assignees.join(', ')}` : '';
      return `> ${pri}*${t.name}* — ${stage}${pct}${assignees}`;
    }).join('\n');

    await say(reply);
    await saveMemories(parsed.new_memories, text);
    return;
  }

  // ── 6D. QUERY TASK ───────────────────────────────────────────
  if (parsed.intent === 'query_task') {
    if (!parsed.matched_task_id) {
      await say('Can\'t place that one — what\'s the task name?');
      return;
    }

    if (parsed.confidence === 'low') {
      const candidates = findCandidateTasks(text, tasks);
      const matched = (tasks || []).find(t => t.id === parsed.matched_task_id);
      const options = matched
        ? [matched, ...candidates.filter(t => t.id !== matched.id)].slice(0, 4)
        : candidates.slice(0, 4);

      if (options.length > 1) {
        disambigSession.set(slackUserId, {
          options: options.map(t => ({ label: t.name, value: t.id })),
          onResolve: async (chosenId, say) => {
            const chosen = options.find(t => t.id === chosenId);
            recentTask.set(slackUserId, { id: chosenId, name: chosen?.name || chosenId });
            await say(`Got it — ask me anything about *${chosen?.name}*.`);
          }
        });
        const list = options.map((t, i) => `${i + 1}. ${t.name}`).join('\n');
        await say(`Which task?\n${list}\n_Type the number._`);
        return;
      }
    }

    const task = (tasks || []).find(t => t.id === parsed.matched_task_id);
    if (!task) {
      await say('Not showing up in the active list — might already be done.');
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
      await say('Can\'t match that to anything active — which task are you referring to?');
      return;
    }

    // Low confidence — present candidates and let the user confirm
    if (parsed.confidence === 'low') {
      const candidates = findCandidateTasks(text, tasks);
      const matched = (tasks || []).find(t => t.id === parsed.matched_task_id);
      const options = matched
        ? [matched, ...candidates.filter(t => t.id !== matched.id)].slice(0, 4)
        : candidates.slice(0, 4);

      if (options.length > 1) {
        disambigSession.set(slackUserId, {
          options: options.map(t => ({ label: t.name, value: t.id })),
          onResolve: async (chosenId, say) => {
            const chosen = options.find(t => t.id === chosenId);
            recentTask.set(slackUserId, { id: chosenId, name: chosen?.name || chosenId });
            await say(`Got it — *${chosen?.name}* is in focus. Go ahead with your update.`);
          }
        });
        const list = options.map((t, i) => `${i + 1}. ${t.name}`).join('\n');
        await say(`Which task?\n${list}\n_Type the number._`);
        return;
      }
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
      await ensureWorkerMemory(current);
    }

    if (Object.keys(dbUpdate).length > 0) {
      const { error: taskErr } = await sb.from('tasks').update(dbUpdate).eq('id', parsed.matched_task_id);
      if (taskErr) {
        console.error('task update error:', taskErr);
        await say(`Something went wrong saving *${taskName}* — check Render logs.`);
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
    confirm += `\n_"undo that" to roll it back_`;
    await say(confirm);
    await saveMemories(parsed.new_memories, text);
    return;
  }

  // ── 6D. REVERT ───────────────────────────────────────────────
  if (parsed.intent === 'revert') {
    if (!parsed.matched_task_id) {
      await say('Which task are we reverting?');
      return;
    }

    if (parsed.confidence === 'low') {
      const candidates = findCandidateTasks(text, tasks);
      const matched = (tasks || []).find(t => t.id === parsed.matched_task_id);
      const options = matched
        ? [matched, ...candidates.filter(t => t.id !== matched.id)].slice(0, 4)
        : candidates.slice(0, 4);

      if (options.length > 1) {
        disambigSession.set(slackUserId, {
          options: options.map(t => ({ label: t.name, value: t.id })),
          onResolve: async (chosenId, say) => {
            const chosen = options.find(t => t.id === chosenId);
            recentTask.set(slackUserId, { id: chosenId, name: chosen?.name || chosenId });
            await say(`Got it — say "undo that" again and I'll revert *${chosen?.name}*.`);
          }
        });
        const list = options.map((t, i) => `${i + 1}. ${t.name}`).join('\n');
        await say(`Which task are we reverting?\n${list}\n_Type the number._`);
        return;
      }
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
      await say(`No bot changes on record for *${taskName}* — nothing to revert.`);
      return;
    }

    const lastEntry = logs[0];
    const snapshot  = decodeSnap(lastEntry.text);
    if (!snapshot) {
      await say(`Found the log entry for *${taskName}* but there's no snapshot to restore from.`);
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
      await say(`Hit an error reverting *${taskName}* — check Render logs.`);
      return;
    }

    await sb.from('task_updates').insert({
      task_id: parsed.matched_task_id,
      author:  userName,
      text:    `Reverted to state before bot changes made at ${changedAt} on ${changedDate}.`,
      date:    today,
      via:     'Slack Bot',
    });

    await say(`↩️ *${taskName}* rolled back to how it was at ${changedAt} on ${changedDate}.`);
    await saveMemories(parsed.new_memories, text);
    return;
  }

  // Fallback
  await say('Not sure what you\'re after — you can update a task, create one, ask about one, or say "undo" to roll something back.');
}

// ── SLACK NAME → WORKER NAME RESOLVER ───────────────────────
// Tries to match a Slack display/real name to a known worker in bot_memory.
// Handles: exact match, prefix match, initials (e.g. "KM" → "Keith Macabenta"),
// and single-letter abbreviations. Caches the result as an alias in bot_memory.
async function resolveWorkerName(slackUserId, slackName) {
  if (!slackName || slackName === 'Unknown') return slackName;

  // Check if we've already cached this resolution
  const { data: cached } = await sb
    .from('bot_memory')
    .select('value')
    .eq('category', 'alias')
    .eq('key', `slack:${slackUserId}`)
    .limit(1);
  if (cached?.[0]) return cached[0].value;

  // Load workers AND aliases in one call
  const { data: allMem } = await sb.from('bot_memory').select('key, value, category');
  const workers = (allMem || []).filter(m => m.category === 'worker');
  const aliases  = (allMem || []).filter(m => m.category === 'alias');

  let resolved = matchWorkerName(slackName, workers);

  // If worker matching failed, check saved aliases (e.g. "km" → "KM is an alias for Keith")
  if (!resolved) {
    const aliasEntry = aliases.find(a => a.key === slackName.toLowerCase());
    if (aliasEntry) {
      // Find a worker name that appears in the alias value
      const workerNames = workers.map(w => capitalize(w.key.split(' ')[0]));
      const hit = workerNames.find(wn => aliasEntry.value.includes(wn));
      if (hit) {
        resolved = hit;
      } else {
        // Fall back: first capitalized word in the alias value (e.g. "Keith" from "KM is an alias for Keith")
        const cap = aliasEntry.value.match(/\b([A-Z][a-z]{1,})\b/);
        if (cap) resolved = cap[1];
      }
    }
  }

  if (resolved) {
    // Cache for next time — saves the lookup on every message
    await sb.from('bot_memory').upsert(
      {
        category: 'alias',
        key: `slack:${slackUserId}`,
        value: resolved,
        source: `auto-resolved from Slack name "${slackName}"`,
        updated_at: new Date().toISOString(),
      },
      { onConflict: 'category,key' }
    );
    console.log(`Resolved Slack user "${slackName}" → "${resolved}"`);
    return resolved;
  }

  return slackName; // no match — use Slack name as-is
}

// ── WORKER MEMORY ────────────────────────────────────────────
// Called whenever we see a name used as an assignee.
// Saves a worker memory entry if one doesn't already exist for that name.
async function ensureWorkerMemory(names = []) {
  for (const name of names) {
    if (!name || name.length < 2) continue;
    const key = name.toLowerCase().trim();

    // Check if we already have a memory entry for this worker
    const { data: existing } = await sb
      .from('bot_memory')
      .select('id')
      .eq('category', 'worker')
      .eq('key', key)
      .limit(1);

    if (existing?.length) continue; // already known

    // Save a new entry
    const { error } = await sb.from('bot_memory').upsert(
      { category: 'worker', key, value: `${name} is a team member who can be assigned to tasks.`, source: 'auto-detected from assignee', updated_at: new Date().toISOString() },
      { onConflict: 'category,key' }
    );
    if (error) console.error(`ensureWorkerMemory error for "${name}":`, error.message);
    else console.log(`Worker memory saved: "${name}"`);
  }
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
