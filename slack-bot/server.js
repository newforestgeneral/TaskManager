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
const disambigSession    = new Map(); // slackUserId → { options:[{label,value}], onResolve }
const rescheduleSession  = new Map(); // slackUserId → { task, suggestedDate }

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

const TASK_STAGE_NAMES = {
  0: 'Name Only',
  1: 'Created',
  2: 'Scheduled',
  3: 'Started',
  4: 'Reporting',
  5: 'Lapsed',
  6: 'Final Progress',
  7: 'Closed',
};

// Text stage kept in sync so the web app kanban still works
const STAGE_FOR_TASK_STAGE = {
  0: 'assigned', 1: 'assigned', 2: 'assigned',
  3: 'inprogress', 4: 'inprogress', 5: 'inprogress',
  6: 'review',
  7: 'complete',
};

// Lineages whose tasks are always weather-checked
const OUTDOOR_LINEAGES = ['Land & Forest', 'Farm & Garden', 'Site Infrastructure'];
// Lineages checked only when weather_dependent = 'yes'
const PARTIAL_OUTDOOR_LINEAGES = ['Building Improvements', 'Other Building'];

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

// Returns true if right now is Mon–Fri 9am–5pm America/Toronto
function isWorkingHours() {
  const now  = new Date();
  const dow  = new Intl.DateTimeFormat('en-US', { timeZone: 'America/Toronto', weekday: 'short' }).format(now);
  const hour = parseInt(new Intl.DateTimeFormat('en-US', { timeZone: 'America/Toronto', hour: 'numeric', hour12: false }).format(now), 10);
  return !['Sat', 'Sun'].includes(dow) && hour >= 9 && hour < 17;
}

// Returns true if a given YYYY-MM-DD date is a working day (Mon–Fri, Toronto)
function isWorkerAvailableOnDate(dateStr) {
  const d   = new Date(dateStr + 'T17:00:00Z'); // use 5pm UTC to stay in Toronto's calendar date
  const dow = new Intl.DateTimeFormat('en-US', { timeZone: 'America/Toronto', weekday: 'short' }).format(d);
  return !['Sat', 'Sun'].includes(dow);
}

// Returns the next clear (no bad weather + Mon–Fri) date strictly after startDateStr.
// Returns null if none found within maxDaysAhead days.
function findNextClearDay(startDateStr, badDays, maxDaysAhead = 14) {
  const base = new Date(startDateStr + 'T17:00:00Z');
  for (let i = 1; i <= maxDaysAhead; i++) {
    const next    = new Date(base.getTime() + i * 24 * 60 * 60 * 1000);
    const dateStr = next.toLocaleDateString('en-CA', { timeZone: 'America/Toronto' });
    if (isWorkerAvailableOnDate(dateStr) && !badDays.has(dateStr)) return dateStr;
  }
  return null;
}

// Counts working hours (Mon–Fri 9am–5pm Toronto) between two ISO timestamps.
// Used for the lapse check — a task only lapses during working time.
function workingHoursElapsed(startISO, endISO) {
  const TZ = 'America/Toronto';
  const WORK_START = 9;
  const WORK_END   = 17;

  function parts(date) {
    const dow  = new Intl.DateTimeFormat('en-US', { timeZone: TZ, weekday: 'short' }).format(date);
    const time = new Intl.DateTimeFormat('en-US', { timeZone: TZ, hour: '2-digit', minute: '2-digit', hour12: false }).format(date);
    const [h, m] = time.split(':').map(Number);
    const dowMap = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
    return { dow: dowMap[dow], hour: h === 24 ? 0 : h, minute: m };
  }

  // Advance date to the next weekday at WORK_START.
  // Adding multiples of 24 h is DST-approximate but accurate enough for an hourly lapse check.
  function nextWorkday9am(date) {
    let d = new Date(date.getTime() + 24 * 60 * 60 * 1000);
    for (let i = 0; i < 3; i++) {
      const p = parts(d);
      if (p.dow >= 1 && p.dow <= 5) {
        const mOffset = (WORK_START - p.hour) * 60 - p.minute;
        return new Date(d.getTime() + mOffset * 60 * 1000);
      }
      d = new Date(d.getTime() + 24 * 60 * 60 * 1000);
    }
    return d;
  }

  const start = new Date(startISO);
  const end   = new Date(endISO);
  if (end <= start) return 0;

  let totalMinutes = 0;
  let cursor = new Date(start);

  while (cursor < end) {
    const p = parts(cursor);

    // Weekend → jump to Monday 9am
    if (p.dow === 0 || p.dow === 6) {
      const daysToMon = p.dow === 0 ? 1 : 2;
      let d = new Date(cursor.getTime() + daysToMon * 24 * 60 * 60 * 1000);
      const p2 = parts(d);
      cursor = new Date(d.getTime() + ((WORK_START - p2.hour) * 60 - p2.minute) * 60 * 1000);
      continue;
    }

    // Before 9am → jump to 9am
    if (p.hour < WORK_START) {
      cursor = new Date(cursor.getTime() + ((WORK_START - p.hour) * 60 - p.minute) * 60 * 1000);
      continue;
    }

    // After 5pm → jump to next workday 9am
    if (p.hour >= WORK_END) {
      cursor = nextWorkday9am(cursor);
      continue;
    }

    // In working hours — count remaining work minutes today or until end
    const minsLeftToday = (WORK_END - p.hour) * 60 - p.minute;
    const msToEnd       = end - cursor;

    if (msToEnd <= minsLeftToday * 60 * 1000) {
      totalMinutes += Math.floor(msToEnd / 60000);
      break;
    }

    totalMinutes += minsLeftToday;
    cursor = nextWorkday9am(cursor);
  }

  return totalMinutes / 60;
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

// ── STAGE MANAGEMENT ─────────────────────────────────────────

// Persist a stage change and keep the legacy text stage in sync for the web app.
async function setTaskStage(taskId, newStage, extraFields = {}) {
  const update = {
    task_stage: newStage,
    stage: STAGE_FOR_TASK_STAGE[newStage] || 'assigned',
    ...extraFields,
  };
  if (newStage === 3 && !extraFields.stage_started_at) {
    update.stage_started_at = new Date().toISOString();
  }
  const { error } = await sb.from('tasks').update(update).eq('id', taskId);
  if (error) console.error(`setTaskStage error (task ${taskId} → ${newStage}):`, error.message);
  return !error;
}

// Called after any field update. Silently promotes stages 0→1 and 1→2
// when the required fields are now present. Stages 3+ require explicit triggers.
async function autoPromoteStage(taskId, task) {
  if (!task || task.task_stage >= 3) return null; // 3+ are event-driven

  let target = task.task_stage;

  if (target < 1 && task.description &&
      (task.tools?.length > 0 || task.materials?.length > 0)) {
    target = 1;
  }
  if (target < 2 && task.start_date && task.estimated_hours && task.assignees?.length > 0) {
    target = 2;
  }

  if (target !== task.task_stage) {
    await setTaskStage(taskId, target);
    console.log(`Auto-promoted task "${task.name}": S${task.task_stage} → S${target}`);
    return target;
  }
  return null;
}

// Hourly lapse check — runs only during working hours.
// Finds stage-3/4 tasks whose working hours elapsed since start exceeds estimated_hours,
// marks them as stage 5, and posts a Slack alert.
async function checkLapsedTasks(slackClient, channelId) {
  if (!channelId) return;

  const { data: activeTasks, error } = await sb
    .from('tasks')
    .select('id, name, task_stage, assignees, start_date, estimated_hours, stage_started_at')
    .in('task_stage', [3, 4])
    .not('estimated_hours', 'is', null);

  if (error) { console.error('checkLapsedTasks fetch error:', error.message); return; }

  const now = new Date().toISOString();

  for (const task of activeTasks || []) {
    const refDate = task.stage_started_at || task.start_date;
    if (!refDate) continue;

    const elapsed = workingHoursElapsed(refDate, now);
    if (elapsed < task.estimated_hours) continue;

    // Check for any real update since the task started (exclude bot snapshots)
    const { data: updates } = await sb
      .from('task_updates')
      .select('created_at')
      .eq('task_id', task.id)
      .gte('created_at', refDate)
      .not('text', 'like', '%[SNAP]%')
      .order('created_at', { ascending: false })
      .limit(1);

    // If there's been a human update since start, don't lapse
    if (updates?.length > 0) continue;

    // Mark as lapsed
    await setTaskStage(task.id, 5);
    await sb.from('task_updates').insert({
      task_id: task.id,
      author:  'Task Bot',
      text:    `Task automatically marked as Lapsed — ${task.estimated_hours}h of working time elapsed with no progress report.`,
      date:    todayISO(),
      via:     'Slack Bot',
    });

    const assigneeNames = task.assignees?.length ? task.assignees.join(', ') : 'unassigned';
    try {
      await slackClient.chat.postMessage({
        channel: channelId,
        text: `⚠️ *${task.name}* has lapsed — ${task.estimated_hours}h of working time elapsed with no update.\n> 👤 ${assigneeNames} · Please log a progress report to continue.`,
      });
    } catch (e) {
      console.error('Lapse alert post error:', e.message);
    }
    console.log(`Task "${task.name}" lapsed after ${elapsed.toFixed(1)}h working hours`);
  }
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

  const assignees = data.assignees?.length ? data.assignees : (tmpl.default_assignees || []);
  const description = data.description || tmpl.description || '';
  const tools = tmpl.default_tools || [];
  const materials = tmpl.default_materials || [];
  const estimatedHours = data.estimated_hours || tmpl.default_estimated_hours || null;

  // Compute initial task_stage from what was provided
  let initialStage = 0;
  if (description && (tools.length > 0 || materials.length > 0)) initialStage = 1;
  if (initialStage >= 1 && data.start_date && estimatedHours && assignees.length > 0) initialStage = 2;

  const newTask = {
    name:               data.name,
    description,
    priority:           data.priority            || tmpl.default_priority      || null,
    stage:              STAGE_FOR_TASK_STAGE[initialStage] || 'assigned',
    task_stage:         initialStage,
    assignees,
    lineage:            data.lineage             || tmpl.default_lineage       || null,
    estimated_hours:    estimatedHours,
    weather_dependent:  tmpl.default_weather_dependent                         || 'no',
    steps:              tmpl.default_steps                                     || [],
    materials,
    tools,
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

  // ── Active reschedule confirmation? ──
  if (rescheduleSession.has(slackUserId)) {
    await handleRescheduleReply(slackUserId, text, say);
    return;
  }

  // ── 3. Short-circuit: obvious follow-up on the last task ──
  const last = recentTask.get(slackUserId);
  if (last && /^(more info|more info on (that|it|this)|more details|tell me more|details|expand|what about it|show me more|more on that|info on that|more)$/i.test(text.trim())) {
    // Treat as query_task on the last touched task — no need to call Claude
    const { data: taskRow } = await sb
      .from('tasks')
      .select('id, name, stage, task_stage, percent_complete, assignees, lineage, priority, description, start_date, due_date, follow_up_date, estimated_hours, location, task_notes')
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

  // ── Abbreviated commands ─────────────────────────────────────
  // nt [name]  → new task (start wizard, optional name prefill)
  // c <term>   → change focus to a different task
  // et [text]  → edit the current focused task
  const trimmed = text.trim();

  // weather — manual weather check
  if (/^(weather|weather update|check weather|weather check|weather report|forecast|what('?s| is) the weather|weather this week|weather next week|weather forecast|any weather|bad weather)(\?)?$/i.test(trimmed)) {
    await say('Checking forecast…');
    try {
      const forecast = await fetchWeatherForecast();
      if (!forecast) { await say('⚠️ No weather data — check that OPENWEATHER_API_KEY is set.'); return; }

      // Full weather report (5-day + hourly)
      await say(buildWeatherReport(forecast));

      // Check for at-risk tasks and append impact summary
      const badDays = getBadWeatherDays(forecast);
      if (badDays.size > 0) {
        const { data: allTasks } = await sb
          .from('tasks')
          .select('id, name, task_stage, assignees, start_date, due_date, lineage, weather_dependent')
          .in('task_stage', [2, 3, 4]);

        const atRisk = [];
        for (const task of allTasks || []) {
          const fullOutdoor    = OUTDOOR_LINEAGES.includes(task.lineage);
          const partialOutdoor = PARTIAL_OUTDOOR_LINEAGES.includes(task.lineage) && task.weather_dependent === 'yes';
          if (!fullOutdoor && !partialOutdoor) continue;
          const hits = [task.start_date, task.due_date].filter(d => d && badDays.has(d));
          if (hits.length) atRisk.push({ task, badDates: hits });
        }

        if (!atRisk.length) {
          await say('✅ No outdoor tasks scheduled on bad weather days.');
        } else {
          const byDay = {};
          for (const { task, badDates } of atRisk) {
            for (const d of badDates) {
              (byDay[d] = byDay[d] || []).push(task);
            }
          }
          const lines = Object.keys(byDay).sort().map(day => {
            const label = new Date(day + 'T17:00:00Z').toLocaleDateString('en-US', {
              timeZone: 'America/Toronto', weekday: 'long', month: 'short', day: 'numeric',
            });
            const taskLines = byDay[day].map(t =>
              `  • *${t.name}* · 👤 ${t.assignees?.join(', ') || 'unassigned'}`
            ).join('\n');
            return `⚠️ *${label}*\n${taskLines}`;
          });
          await say(`*🌧 At-risk outdoor tasks:*\n${lines.join('\n\n')}\n_Manage dates in the app, or say "reschedule [task name]" to push them._`);

          // Also notify #task-updates so the whole team sees it
          const channelId = process.env.TASK_UPDATES_CHANNEL_ID || null;
          if (channelId) await checkWeatherAlerts(client, channelId);
        }
      } else {
        await say('☀️ No bad weather days in the forecast — all tasks on track.');
      }

    } catch (e) {
      console.error('Manual weather check error:', e.message);
      await say(`Weather check failed: ${e.message}`);
    }
    return;
  }

  // schedule — today's recommended tasks per worker
  if (/^(schedule|today'?s schedule|what('?s| is) scheduled|today'?s plan|daily plan|what should we work on|what to work on|today'?s tasks|tasks today)(\?)?$/i.test(trimmed)) {
    await handleScheduleCommand(say);
    return;
  }

  // reschedule <task name> — check weather conflict and propose a new start date
  if (/^reschedule\s+\S/i.test(trimmed)) {
    await handleRescheduleCommand(slackUserId, trimmed.replace(/^reschedule\s+/i, '').trim(), say);
    return;
  }

  // nt — new task
  if (/^nt(\s|$)/i.test(trimmed)) {
    const nameArg = trimmed.slice(2).trim();
    await startWizard(slackUserId, say, nameArg ? { name: nameArg } : {});
    return;
  }

  // ── 3. Fetch active tasks ──
  const { data: tasks, error: tasksErr } = await sb
    .from('tasks')
    .select('id, name, stage, task_stage, percent_complete, assignees, lineage, priority, description, start_date, due_date, follow_up_date, estimated_hours, location, task_notes, stage_started_at, tools, materials, weather_dependent')
    .not('stage', 'eq', 'complete')
    .order('created_at', { ascending: true });

  if (tasksErr) {
    console.error('Supabase fetch error:', tasksErr);
    await say('Can\'t reach the database right now — give it a second and try again.');
    return;
  }

  // c <term> — switch focused task
  if (/^c\s+\S/i.test(trimmed)) {
    const searchTerm = trimmed.slice(1).trim();
    const candidates = findCandidateTasks(searchTerm, tasks || []);
    if (!candidates.length) {
      await say(`No active task matches "${searchTerm}".`);
      return;
    }
    if (candidates.length === 1) {
      recentTask.set(slackUserId, { id: candidates[0].id, name: candidates[0].name });
      await say(`In focus: *${candidates[0].name}* — say "more info", update it, or ask away.`);
      return;
    }
    disambigSession.set(slackUserId, {
      options: candidates.slice(0, 4).map(t => ({ label: t.name, value: t.id })),
      onResolve: async (chosenId, say) => {
        const chosen = candidates.find(t => t.id === chosenId);
        recentTask.set(slackUserId, { id: chosenId, name: chosen?.name });
        await say(`In focus: *${chosen?.name}* — say "more info", update it, or ask away.`);
      }
    });
    const list = candidates.slice(0, 4).map((t, i) => `${i + 1}. ${t.name}`).join('\n');
    await say(`Which task?\n${list}\n_Type the number._`);
    return;
  }

  // et [text] — edit focused task
  // "et" alone: prompt what to change
  // "et <changes>": transform into explicit update message for Claude
  let claudeText = text; // default — passed to Claude unchanged
  if (/^et(\s|$)/i.test(trimmed)) {
    const last = recentTask.get(slackUserId);
    if (!last) {
      await say(`No task in focus. Use \`c <task name>\` to select one first, or start with the task name.`);
      return;
    }
    const editContent = trimmed.slice(2).trim();
    if (!editContent) {
      await say(`Editing *${last.name}* — what would you like to change?\n> e.g. "priority high" · "80% done" · "assign Keith" · "due Friday"`);
      return;
    }
    // Rephrase so Claude has zero ambiguity about which task and what to do
    claudeText = `Update "${last.name}" (ID: ${last.id}): ${editContent}`;
  }

  const taskList = (tasks || []).map(t =>
    `ID: ${t.id} | "${t.name}" | TaskStage: S${t.task_stage ?? 0} (${TASK_STAGE_NAMES[t.task_stage ?? 0]}) | ${t.percent_complete}% | Priority: ${t.priority} | Assignees: ${(t.assignees || []).join(', ') || 'none'} | Lineage: ${t.lineage || 'none'}`
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
Their message: "${claudeText}"

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

    // ── Stage transition detection ──────────────────────────
    const currentStage = task?.task_stage ?? 0;
    const msgLower = text.toLowerCase();

    // S2→S3: assignee confirms they've started
    if (currentStage === 2 && /\b(started|starting|on it|underway|begun|kicked off|going now|beginning)\b/i.test(msgLower)) {
      dbUpdate.task_stage       = 3;
      dbUpdate.stage            = 'inprogress';
      dbUpdate.stage_started_at = new Date().toISOString();
      changeLines.push(`Stage: ${TASK_STAGE_NAMES[2]} → ${TASK_STAGE_NAMES[3]}`);
    }

    // S3→S4: any progress update when already started
    if (currentStage === 3 && !dbUpdate.task_stage) {
      dbUpdate.task_stage = 4;
      changeLines.push(`Stage: ${TASK_STAGE_NAMES[3]} → ${TASK_STAGE_NAMES[4]}`);
    }

    // S5→S4: recovery from lapsed when assignee logs a new update
    if (currentStage === 5 && !dbUpdate.task_stage) {
      dbUpdate.task_stage = 4;
      dbUpdate.stage      = 'inprogress';
      changeLines.push(`Stage: ${TASK_STAGE_NAMES[5]} → ${TASK_STAGE_NAMES[4]} (recovered)`);
    }

    // S3/S4/S5→S6: assignee marks as final
    if ([3, 4, 5].includes(currentStage) && !dbUpdate.task_stage &&
        /\b(final|last update|all done|fully complete|finished|wrapped up|done done|that's it)\b/i.test(msgLower)) {
      dbUpdate.task_stage = 6;
      dbUpdate.stage      = 'review';
      changeLines.push(`Stage: → ${TASK_STAGE_NAMES[6]}`);
    }

    // S6→S7: admin/manager closes the task
    if (currentStage === 6 && !dbUpdate.task_stage &&
        /\b(close|closing|closed|done|complete|finished|shut it|wrap it up)\b/i.test(msgLower)) {
      dbUpdate.task_stage = 7;
      dbUpdate.stage      = 'complete';
      changeLines.push(`Stage: → ${TASK_STAGE_NAMES[7]}`);
    }

    // Auto-promote stages 0–2 if new field values now qualify
    if (!dbUpdate.task_stage && currentStage < 3) {
      const merged = { ...task, ...dbUpdate };
      const newStage = await autoPromoteStage(parsed.matched_task_id, merged);
      if (newStage !== null) {
        changeLines.push(`Stage: S${currentStage} → S${newStage} (${TASK_STAGE_NAMES[newStage]})`);
        // setTaskStage already persisted it — remove from dbUpdate to avoid double-write
        delete dbUpdate.task_stage;
        delete dbUpdate.stage;
        delete dbUpdate.stage_started_at;
      }
    }
    // ── End stage transitions ────────────────────────────────

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

// ── WEATHER SCHEDULING ───────────────────────────────────────

async function fetchWeatherForecast() {
  const lat    = process.env.NEWFOREST_LAT    || '43.9196';
  const lng    = process.env.NEWFOREST_LNG    || '-80.0940';
  const apiKey = process.env.OPENWEATHER_API_KEY;
  if (!apiKey) { console.warn('OPENWEATHER_API_KEY not set — skipping weather check'); return null; }

  const url = `https://api.openweathermap.org/data/2.5/forecast?lat=${lat}&lon=${lng}&appid=${apiKey}&units=metric`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`OpenWeatherMap ${res.status}: ${res.statusText}`);
  return res.json();
}

// Returns a Set of "YYYY-MM-DD" date strings (Toronto) with bad weather.
// Bad = rain probability > 60%, temp < 2°C (frost), or any snow.
function getBadWeatherDays(forecast) {
  const TZ      = 'America/Toronto';
  const badDays = new Set();

  for (const item of forecast.list || []) {
    const date      = new Date(item.dt * 1000).toLocaleDateString('en-CA', { timeZone: TZ });
    const rainProb  = (item.pop || 0) * 100;
    const temp      = item.main?.temp ?? 99;
    const weather   = item.weather?.[0]?.main?.toLowerCase() || '';
    const hasSnow   = weather === 'snow' || (item.snow?.['3h'] ?? 0) > 0;
    const hasFrost  = temp < 2;
    const hasRain   = rainProb > 60;

    if (hasSnow || hasFrost || hasRain) badDays.add(date);
  }
  return badDays;
}

// Builds a full Slack-formatted weather report: 5-day overview + hourly breakdown.
function buildWeatherReport(forecast) {
  const TZ = 'America/Toronto';

  function iconEmoji(icon) {
    const id = icon?.slice(0, 2) || '';
    return ({ '01':'☀️','02':'🌤','03':'⛅','04':'☁️','09':'🌧','10':'🌦','11':'⛈','13':'🌨','50':'🌫' })[id] || '🌡';
  }

  function windDir(deg) {
    if (deg == null) return '  ';
    return ['N ','NE','E ','SE','S ','SW','W ','NW'][Math.round(deg / 45) % 8];
  }

  function fmtDate(dateStr, opts) {
    // Use noon UTC to avoid DST/timezone boundary issues with date-only strings
    return new Date(dateStr + 'T17:00:00Z').toLocaleDateString('en-US', { timeZone: TZ, ...opts });
  }

  function fmtTime(dt) {
    return new Date(dt * 1000).toLocaleTimeString('en-US', {
      timeZone: TZ, hour: 'numeric', minute: '2-digit', hour12: true,
    }).replace(' ', '').toLowerCase();
  }

  // Group 3-hour slots by Toronto calendar date
  const byDay = {};
  for (const item of forecast.list || []) {
    const date = new Date(item.dt * 1000).toLocaleDateString('en-CA', { timeZone: TZ });
    (byDay[date] = byDay[date] || []).push(item);
  }
  const days = Object.keys(byDay).sort().slice(0, 5);

  // ── 5-day summary ────────────────────────────────────────────
  const summaryLines = days.map(d => {
    const slots   = byDay[d];
    const high    = Math.round(Math.max(...slots.map(s => s.main.temp_max ?? s.main.temp)));
    const low     = Math.round(Math.min(...slots.map(s => s.main.temp_min ?? s.main.temp)));
    const maxPop  = Math.round(Math.max(...slots.map(s => (s.pop || 0) * 100)));
    const avgKmh  = Math.round(slots.reduce((a, s) => a + (s.wind?.speed || 0), 0) / slots.length * 3.6);
    const mid     = slots[Math.floor(slots.length / 2)] || slots[0];
    const emoji   = iconEmoji(mid.weather?.[0]?.icon);
    const desc    = capitalize(mid.weather?.[0]?.description || 'n/a');
    const dayLbl  = fmtDate(d, { weekday: 'short', month: 'short', day: 'numeric' }).padEnd(11);
    const condLbl = (emoji + ' ' + desc).padEnd(20);
    const tempLbl = (`${high > 0 ? '+' : ''}${high}°/${low > 0 ? '+' : ''}${low}°C`).padStart(10);
    const popLbl  = (maxPop + '% rain').padStart(9);
    const windLbl = windDir(mid.wind?.deg) + ' ' + avgKmh + 'km/h';
    return `${dayLbl} ${condLbl}${tempLbl}  ${popLbl}  ${windLbl}`;
  });

  // ── Hourly breakdown ─────────────────────────────────────────
  const hourlyBlocks = days.map(d => {
    const dayFull = fmtDate(d, { weekday: 'long', month: 'short', day: 'numeric' });
    const lines   = byDay[d].map(s => {
      const t     = fmtTime(s.dt).padStart(7);
      const emoji = iconEmoji(s.weather?.[0]?.icon);
      const desc  = capitalize(s.weather?.[0]?.description || '').slice(0, 13).padEnd(13);
      const temp  = (`${Math.round(s.main.temp) > 0 ? '+' : ''}${Math.round(s.main.temp)}°C`).padStart(5);
      const pop   = (Math.round((s.pop || 0) * 100) + '%').padStart(4);
      const wind  = windDir(s.wind?.deg).trim() + ' ' + Math.round((s.wind?.speed || 0) * 3.6) + 'km/h';
      const snow  = s.snow?.['3h'] ? ` ❄${s.snow['3h']}mm` : '';
      const rain  = s.rain?.['3h'] ? ` 💧${s.rain['3h']}mm` : '';
      return `${t}  ${emoji} ${desc}  ${temp}  ${pop} rain  ${wind}${rain}${snow}`;
    });
    return `*${dayFull}*\n\`\`\`\n${lines.join('\n')}\n\`\`\``;
  });

  return [
    '*🌤 Forecast — Orangeville, ON*',
    '',
    '*5-Day Overview*',
    '```',
    summaryLines.join('\n'),
    '```',
    '',
    '*Hourly Breakdown (3h intervals)*',
    '',
    hourlyBlocks.join('\n\n'),
  ].join('\n');
}

async function checkWeatherAlerts(slackClient, channelId) {
  if (!channelId) return;
  try {
    const forecast = await fetchWeatherForecast();
    if (!forecast) return;

    const badDays = getBadWeatherDays(forecast);
    if (badDays.size === 0) { console.log('Weather check: no bad days in forecast'); return; }

    // Tasks in stages 2–4 with scheduled dates on bad days
    const { data: tasks } = await sb
      .from('tasks')
      .select('id, name, task_stage, assignees, start_date, due_date, lineage, weather_dependent')
      .in('task_stage', [2, 3, 4]);

    const atRisk = [];
    for (const task of tasks || []) {
      const fullOutdoor    = OUTDOOR_LINEAGES.includes(task.lineage);
      const partialOutdoor = PARTIAL_OUTDOOR_LINEAGES.includes(task.lineage) && task.weather_dependent === 'yes';
      if (!fullOutdoor && !partialOutdoor) continue;

      const hits = [task.start_date, task.due_date].filter(d => d && badDays.has(d));
      if (hits.length) atRisk.push({ task, badDates: hits });
    }

    if (!atRisk.length) { console.log('Weather check: no at-risk tasks'); return; }

    // Group tasks by bad day for the Slack message
    const byDay = {};
    for (const { task, badDates } of atRisk) {
      for (const d of badDates) {
        if (!byDay[d]) byDay[d] = [];
        byDay[d].push(task);
      }
    }

    const lines = Object.keys(byDay).sort().map(day => {
      const label = new Date(day + 'T12:00:00Z').toLocaleDateString('en-US', {
        timeZone: 'America/Toronto', weekday: 'long', month: 'short', day: 'numeric',
      });
      const taskLines = byDay[day].map(t => {
        const who = t.assignees?.join(', ') || 'unassigned';
        return `  • *${t.name}* · 👤 ${who}`;
      }).join('\n');
      return `🌧 *Weather alert — ${label}*\n${taskLines}`;
    });

    await slackClient.chat.postMessage({
      channel: channelId,
      text: lines.join('\n\n') + '\n\n_Reply "reschedule [task name]" to push dates, or manage in the app._',
    });

    // Record the check time in bot_memory
    await sb.from('bot_memory').upsert(
      { category: 'config', key: 'last_weather_check', value: new Date().toISOString(),
        source: 'auto', updated_at: new Date().toISOString() },
      { onConflict: 'category,key' }
    );
    console.log(`Weather alert sent — ${atRisk.length} tasks at risk across ${Object.keys(byDay).length} bad day(s)`);
  } catch (e) {
    console.error('Weather check error:', e.message);
  }
}

// ── SCHEDULING ───────────────────────────────────────────────

// Returns array of { task, conflictDates, suggestedDate } for outdoor/weather-sensitive
// tasks in stages 0–2 whose start_date falls on a bad weather day.
function getScheduleConflicts(tasks, badDays) {
  const conflicts = [];
  for (const task of tasks || []) {
    if (task.task_stage > 2) continue; // already started — don't auto-suggest
    const fullOutdoor    = OUTDOOR_LINEAGES.includes(task.lineage);
    const partialOutdoor = PARTIAL_OUTDOOR_LINEAGES.includes(task.lineage) && task.weather_dependent === 'yes';
    if (!fullOutdoor && !partialOutdoor) continue;
    if (!task.start_date) continue;

    const conflictDates = [task.start_date].filter(d => badDays.has(d));
    if (!conflictDates.length) continue;

    const suggestedDate = findNextClearDay(task.start_date, badDays);
    conflicts.push({ task, conflictDates, suggestedDate });
  }
  return conflicts;
}

// Returns tasks sorted by scheduling priority for today's digest / schedule command.
// Order: lapsed > overdue > due soon > weather window closing > priority field > oldest start_date.
function rankTasksForToday(tasks, badDays) {
  const TZ      = 'America/Toronto';
  const today   = new Date().toLocaleDateString('en-CA', { timeZone: TZ });
  const in3Days = new Date(Date.now() + 3 * 24 * 60 * 60 * 1000).toLocaleDateString('en-CA', { timeZone: TZ });
  const tomorrow = new Date(Date.now() + 24 * 60 * 60 * 1000).toLocaleDateString('en-CA', { timeZone: TZ });

  const PRIORITY_SCORE = { urgent: 4, high: 3, medium: 2, low: 1 };

  return (tasks || [])
    .filter(t => t.task_stage < 7)
    .map(t => {
      let score = 0;

      if (t.task_stage === 5) score += 1000;                        // lapsed
      if (t.due_date && t.due_date < today)  score += 800;          // overdue
      if (t.due_date && t.due_date <= tomorrow) score += 200;       // due tomorrow
      if (t.due_date && t.due_date <= in3Days)  score += 300;       // due soon

      // Weather window closing — outdoor task, clear today, bad tomorrow
      const isOutdoor = OUTDOOR_LINEAGES.includes(t.lineage) ||
        (PARTIAL_OUTDOOR_LINEAGES.includes(t.lineage) && t.weather_dependent === 'yes');
      if (isOutdoor && !badDays.has(today) && badDays.has(tomorrow)) score += 400;

      score += (PRIORITY_SCORE[t.priority] || 0) * 10;

      // Older start_date = slightly higher (tiebreaker, capped at 30 days)
      if (t.start_date) {
        const daysOld = (Date.now() - new Date(t.start_date + 'T17:00:00Z').getTime()) / 86400000;
        score += Math.min(daysOld, 30);
      }

      return { task: t, score };
    })
    .sort((a, b) => b.score - a.score)
    .map(x => x.task);
}

// Handles `reschedule <task name>` — finds the task, checks weather conflict, prompts user.
async function handleRescheduleCommand(slackUserId, taskNameHint, say) {
  const forecast = await fetchWeatherForecast();
  const badDays  = forecast ? getBadWeatherDays(forecast) : new Set();

  const { data: tasks } = await sb
    .from('tasks')
    .select('id, name, task_stage, assignees, lineage, start_date, weather_dependent')
    .lte('task_stage', 2)
    .not('stage', 'eq', 'complete');

  const candidates = findCandidateTasks(taskNameHint, tasks || []);
  const task = candidates[0];

  if (!task) {
    await say(`Can't find "${taskNameHint}" — it may already be started or completed.`);
    return;
  }
  if (!task.start_date) {
    await say(`*${task.name}* has no start date set — add one in the app first.`);
    return;
  }

  const isOutdoor = OUTDOOR_LINEAGES.includes(task.lineage) ||
    (PARTIAL_OUTDOOR_LINEAGES.includes(task.lineage) && task.weather_dependent === 'yes');
  if (!isOutdoor) {
    await say(`*${task.name}* isn't weather-sensitive — no rescheduling needed.`);
    return;
  }

  const conflictDates = [task.start_date].filter(d => badDays.has(d));
  if (!conflictDates.length) {
    const lbl = new Date(task.start_date + 'T17:00:00Z').toLocaleDateString('en-US', {
      timeZone: 'America/Toronto', weekday: 'short', month: 'short', day: 'numeric',
    });
    await say(`No weather conflict on *${task.name}*'s scheduled date (${lbl}) — looks clear.`);
    return;
  }

  const suggestedDate = findNextClearDay(task.start_date, badDays);
  const conflictLbl = new Date(conflictDates[0] + 'T17:00:00Z').toLocaleDateString('en-US', {
    timeZone: 'America/Toronto', weekday: 'short', month: 'short', day: 'numeric',
  });
  const suggestLbl = suggestedDate
    ? new Date(suggestedDate + 'T17:00:00Z').toLocaleDateString('en-US', {
        timeZone: 'America/Toronto', weekday: 'short', month: 'short', day: 'numeric',
      })
    : null;

  rescheduleSession.set(slackUserId, { task, suggestedDate });

  const prompt = suggestedDate
    ? `Move to *${suggestLbl}* (${suggestedDate})? Reply \`yes\`, a different date, or \`skip\`.`
    : `No clear day found in the next 2 weeks. Reply with a date to move it to, or \`skip\`.`;

  await say(`⛈ Bad weather on *${conflictLbl}* blocks *${task.name}*. ${prompt}`);
}

// Handles the yes/date/skip reply to a pending reschedule prompt.
async function handleRescheduleReply(slackUserId, text, say) {
  const session = rescheduleSession.get(slackUserId);
  const t = text.trim();

  if (/^(skip|no|cancel|forget it|never mind)/i.test(t)) {
    rescheduleSession.delete(slackUserId);
    await say(`Skipped — *${session.task.name}* stays on ${session.task.start_date}.`);
    return;
  }

  let newDate = null;
  if (/^yes$/i.test(t)) {
    newDate = session.suggestedDate;
  } else {
    newDate = parseDate(t);
  }

  if (!newDate) {
    await say(`Didn't catch that date — try \`yes\`, a date like "May 20", or \`skip\`.`);
    return;
  }

  const { error } = await sb.from('tasks').update({ start_date: newDate }).eq('id', session.task.id);
  if (error) {
    console.error('reschedule update error:', error.message);
    await say(`Hit an error updating *${session.task.name}* — check Render logs.`);
    rescheduleSession.delete(slackUserId);
    return;
  }

  await sb.from('task_updates').insert({
    task_id: session.task.id,
    author:  'Task Bot',
    text:    `Start date rescheduled from ${session.task.start_date} to ${newDate} due to weather conflict.`,
    date:    todayISO(),
    via:     'Slack Bot',
  });

  rescheduleSession.delete(slackUserId);

  const newLbl = new Date(newDate + 'T17:00:00Z').toLocaleDateString('en-US', {
    timeZone: 'America/Toronto', weekday: 'short', month: 'short', day: 'numeric',
  });
  await say(`✅ *${session.task.name}* rescheduled to ${newLbl}.`);
}

// On-demand `schedule` command — today's recommended tasks per worker + weather conflicts.
async function handleScheduleCommand(say) {
  try {
    const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Toronto' });

    if (!isWorkerAvailableOnDate(today)) {
      const nextDay = findNextClearDay(today, new Set(), 3) || 'Monday';
      await say(`📅 Today is a weekend — no scheduled work. Next working day: ${nextDay}`);
      return;
    }

    const forecast = await fetchWeatherForecast();
    const badDays  = forecast ? getBadWeatherDays(forecast) : new Set();

    const { data: tasks } = await sb
      .from('tasks')
      .select('id, name, task_stage, assignees, lineage, priority, start_date, due_date, estimated_hours, weather_dependent, location')
      .not('stage', 'eq', 'complete')
      .order('created_at', { ascending: true });

    if (!tasks?.length) { await say('No active tasks found.'); return; }

    const ranked   = rankTasksForToday(tasks, badDays);
    const todayBad = badDays.has(today);
    const weatherNote = todayBad ? '⚠️ _Bad weather today — outdoor tasks affected_\n\n' : '';

    const PRIORITY_EMOJI_MAP = { urgent: '🔴', high: '🟠', medium: '🟡', low: '🟢' };
    const STAGE_BADGE = { 5: '⚠️ LAPSED · ', 6: '✅ FINAL · ' };

    // Group by worker
    const byWorker = {};
    const unassigned = [];
    for (const task of ranked) {
      if (!task.assignees?.length) { unassigned.push(task); continue; }
      for (const assignee of task.assignees) {
        (byWorker[assignee] = byWorker[assignee] || []).push(task);
      }
    }

    const sections = Object.entries(byWorker).map(([worker, workerTasks]) => {
      const lines = workerTasks.slice(0, 5).map(t => {
        const pri     = PRIORITY_EMOJI_MAP[t.priority] || '⚪';
        const badge   = STAGE_BADGE[t.task_stage] || '';
        const isOut   = OUTDOOR_LINEAGES.includes(t.lineage) ||
          (PARTIAL_OUTDOOR_LINEAGES.includes(t.lineage) && t.weather_dependent === 'yes');
        const wFlag   = isOut && todayBad ? ' 🌧' : '';
        const hrs     = t.estimated_hours ? ` · ${t.estimated_hours}h` : '';
        const overdue = t.due_date && t.due_date < today ? ' 🔔 overdue' : '';
        return `> ${pri} ${badge}*${t.name}*${wFlag}${hrs}${overdue}`;
      });
      return `*${worker}*\n${lines.join('\n')}`;
    });

    if (unassigned.length) {
      sections.push(`*Unassigned*\n${unassigned.slice(0, 3).map(t => `> ⚪ *${t.name}*`).join('\n')}`);
    }

    // Upcoming conflicts
    const conflicts = getScheduleConflicts(tasks, badDays);
    const conflictNote = conflicts.length
      ? `\n\n⚠️ *Upcoming weather conflicts:*\n${conflicts.map(c =>
          `  • *${c.task.name}* — scheduled ${c.conflictDates[0]}${c.suggestedDate ? `, suggest ${c.suggestedDate}` : ''}`
        ).join('\n')}\n_Say "reschedule [task name]" to push dates._`
      : '';

    const dateLabel = new Date(today + 'T17:00:00Z').toLocaleDateString('en-US', {
      timeZone: 'America/Toronto', weekday: 'long', month: 'short', day: 'numeric',
    });

    await say(`📅 *Schedule — ${dateLabel}*\n\n${weatherNote}${sections.join('\n\n')}${conflictNote}`);
  } catch (e) {
    console.error('schedule command error:', e.message);
    await say(`Schedule check failed: ${e.message}`);
  }
}

// Posts the daily morning digest to #task-updates.
async function postDailyDigest(slackClient, channelId) {
  const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Toronto' });
  if (!isWorkerAvailableOnDate(today)) return; // skip weekends

  try {
    const forecast = await fetchWeatherForecast();
    const badDays  = forecast ? getBadWeatherDays(forecast) : new Set();

    const { data: tasks } = await sb
      .from('tasks')
      .select('id, name, task_stage, assignees, lineage, priority, start_date, due_date, estimated_hours, weather_dependent')
      .not('stage', 'eq', 'complete')
      .order('created_at', { ascending: true });

    if (!tasks?.length) return;

    const ranked   = rankTasksForToday(tasks, badDays);
    const todayBad = badDays.has(today);
    const weatherNote = todayBad ? '⚠️ _Bad weather today — outdoor tasks affected_\n\n' : '';

    const PRIORITY_EMOJI_MAP = { urgent: '🔴', high: '🟠', medium: '🟡', low: '🟢' };
    const STAGE_BADGE = { 5: '⚠️ LAPSED · ', 6: '✅ FINAL · ' };

    const byWorker = {};
    const unassigned = [];
    for (const task of ranked) {
      if (!task.assignees?.length) { unassigned.push(task); continue; }
      for (const assignee of task.assignees) {
        (byWorker[assignee] = byWorker[assignee] || []).push(task);
      }
    }

    const sections = Object.entries(byWorker).map(([worker, workerTasks]) => {
      const lines = workerTasks.slice(0, 5).map(t => {
        const pri     = PRIORITY_EMOJI_MAP[t.priority] || '⚪';
        const badge   = STAGE_BADGE[t.task_stage] || '';
        const isOut   = OUTDOOR_LINEAGES.includes(t.lineage) ||
          (PARTIAL_OUTDOOR_LINEAGES.includes(t.lineage) && t.weather_dependent === 'yes');
        const wFlag   = isOut && todayBad ? ' 🌧' : '';
        const hrs     = t.estimated_hours ? ` · ${t.estimated_hours}h` : '';
        const overdue = t.due_date && t.due_date < today ? ' 🔔' : '';
        return `> ${pri} ${badge}*${t.name}*${wFlag}${hrs}${overdue}`;
      });
      return `*${worker}*\n${lines.join('\n')}`;
    });

    if (unassigned.length) {
      sections.push(`*Unassigned*\n${unassigned.slice(0, 3).map(t => `> ⚪ *${t.name}*`).join('\n')}`);
    }

    const conflicts = getScheduleConflicts(tasks, badDays);
    const conflictNote = conflicts.length
      ? `\n\n⚠️ *Upcoming weather conflicts (${conflicts.length}):*\n${conflicts.map(c =>
          `  • *${c.task.name}* — scheduled ${c.conflictDates[0]}${c.suggestedDate ? `, suggest ${c.suggestedDate}` : ''}`
        ).join('\n')}\n_Say "reschedule [task name]" to push dates._`
      : '';

    const dateLabel = new Date(today + 'T17:00:00Z').toLocaleDateString('en-US', {
      timeZone: 'America/Toronto', weekday: 'long', month: 'long', day: 'numeric',
    });

    await slackClient.chat.postMessage({
      channel: channelId,
      text: `📅 *Good morning — ${dateLabel}*\n\n${weatherNote}${sections.join('\n\n')}${conflictNote}`,
    });

    console.log(`Daily digest posted — ${Object.keys(byWorker).length} workers, ${ranked.length} tasks, ${conflicts.length} conflicts`);
  } catch (e) {
    console.error('postDailyDigest error:', e.message);
  }
}

// Resolve a channel name to its Slack channel ID.
async function resolveChannelId(slackClient, name) {
  try {
    const cleanName = name.replace(/^#/, '');
    let cursor;
    do {
      const res = await slackClient.conversations.list({ types: 'public_channel,private_channel', limit: 200, cursor });
      const found = res.channels?.find(c => c.name === cleanName);
      if (found) return found.id;
      cursor = res.response_metadata?.next_cursor;
    } while (cursor);
  } catch (e) { console.error('resolveChannelId error:', e.message); }
  return null;
}

// Called once the bot is running. Sets up hourly lapse check and 24h weather check.
async function startScheduledChecks(slackClient) {
  // Prefer env var — avoids needing channels:read scope
  const channelId = process.env.TASK_UPDATES_CHANNEL_ID || await resolveChannelId(slackClient, 'task-updates');
  if (!channelId) { console.warn('Could not resolve #task-updates channel — set TASK_UPDATES_CHANNEL_ID env var or add channels:read scope'); return; }
  console.log(`Scheduled checks armed — channel: ${channelId}`);

  // ── Hourly lapse check (working hours only) ──
  setInterval(async () => {
    if (!isWorkingHours()) return;
    await checkLapsedTasks(slackClient, channelId);
  }, 60 * 60 * 1000);

  // ── 24h weather check (first run after 10s startup delay) ──
  setTimeout(() => checkWeatherAlerts(slackClient, channelId), 10_000);
  setInterval(() => checkWeatherAlerts(slackClient, channelId), 24 * 60 * 60 * 1000);

  // ── Daily 7:30am digest (Toronto time) ──
  // Computes ms until next 7:30am Toronto, fires once, then every 24h.
  function scheduleNextDigest() {
    const TZ        = 'America/Toronto';
    const nowStr    = new Date().toLocaleString('en-US', { timeZone: TZ });
    const torontoNow = new Date(nowStr);
    const next730   = new Date(nowStr);
    next730.setHours(7, 30, 0, 0);
    if (next730 <= torontoNow) next730.setDate(next730.getDate() + 1);
    const msUntil = next730 - torontoNow;
    console.log(`Daily digest scheduled in ${Math.round(msUntil / 60000)} min`);
    setTimeout(async () => {
      await postDailyDigest(slackClient, channelId);
      setInterval(() => postDailyDigest(slackClient, channelId), 24 * 60 * 60 * 1000);
    }, msUntil);
  }
  scheduleNextDigest();
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

  // Kick off lapse + weather checks after bot is fully online
  await startScheduledChecks(app.client);
})();
