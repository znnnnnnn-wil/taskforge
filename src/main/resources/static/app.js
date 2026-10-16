'use strict';
const $ = id => document.getElementById(id);
let apiKey = '', offset = 0, selected = null, refreshing = false;
const size = 20;
const examples = {
  echo: {message: 'Hello, TaskForge'}, sha256: {text: 'TaskForge'},
  report: {values: [12.5, 20, 35.5]}, delay: {milliseconds: 15000}
};
$('request-key').value = crypto.randomUUID();
function notice(message, error = false) {
  $('notice').textContent = message;
  $('notice').className = error ? 'error' : '';
}
async function api(path, options = {}) {
  const response = await fetch('/api' + path, {
    ...options, headers: {'X-API-Key': apiKey, 'Content-Type': 'application/json'}
  });
  const body = await response.json();
  if (!response.ok) throw new Error(body.message || `请求失败 (${response.status})`);
  return body;
}
function cell(row, value) {
  const td = row.insertCell(); td.textContent = value; return td;
}
function button(label, action) {
  const node = document.createElement('button');
  node.type = 'button'; node.textContent = label;
  node.addEventListener('click', () => action().catch(error => notice(error.message, true)));
  return node;
}
function badge(status) {
  const node = document.createElement('span'); node.textContent = status;
  node.className = 'badge ' + status; return node;
}
function date(value) { return value ? new Date(value).toLocaleString('zh-CN', {hour12: false}) : '—'; }
function spec() {
  return {handler: $('handler').value, payload: JSON.parse($('payload').value),
    priority: Number($('priority').value), maxAttempts: Number($('max-attempts').value),
    retryDelaySeconds: Number($('retry-delay').value), timeoutSeconds: Number($('timeout').value)};
}
async function details(id) {
  selected = id;
  const [task, attempts] = await Promise.all([api('/tasks/' + id), api('/tasks/' + id + '/attempts')]);
  $('selected-id').textContent = id;
  $('detail').textContent = JSON.stringify({task, attempts}, null, 2);
  $('detail-panel').open = true;
  $('cancel').disabled = ['SUCCEEDED', 'FAILED', 'CANCELLED'].includes(task.status);
}
async function refresh() {
  if (refreshing || !apiKey) return;
  refreshing = true;
  try {
    const status = $('status').value;
    const query = `?limit=${size}&offset=${offset}` + (status ? '&status=' + status : '');
    const [overview, tasks, schedules] = await Promise.all([
      api('/overview'), api('/tasks' + query), api('/schedules?limit=100')
    ]);
    const counts = {};
    overview.counts.forEach(item => { counts[item.status || item.STATUS] = Number(item.total ?? item.TOTAL); });
    $('pending').textContent = (counts.PENDING || 0) + (counts.RETRY || 0);
    $('running').textContent = counts.RUNNING || 0;
    $('succeeded').textContent = counts.SUCCEEDED || 0;
    $('failed').textContent = counts.FAILED || 0;
    $('node').textContent = `节点 ${overview.nodeId} · 本节点活跃 ${overview.active}/${overview.concurrency}`;
    $('tasks').replaceChildren();
    for (const task of tasks) {
      const row = $('tasks').insertRow();
      const first = cell(row, '');
      const link = button(task.handler, () => details(task.id)); link.className = 'task-name'; first.append(link);
      const id = document.createElement('span'); id.className = 'task-id'; id.textContent = task.id.slice(0, 18); first.append(id);
      cell(row, '').append(badge(task.status));
      cell(row, task.progress + '%'); cell(row, `${task.attempts}/${task.maxAttempts}`); cell(row, date(task.createdAt));
    }
    if (!tasks.length) cell($('tasks').insertRow(), '暂无任务');
    $('previous').disabled = offset === 0;
    $('next').disabled = tasks.length < size;
    $('page').textContent = String(offset / size + 1);
    $('schedules').replaceChildren();
    for (const schedule of schedules) {
      const row = $('schedules').insertRow();
      cell(row, schedule.name); cell(row, `${schedule.cron} · ${schedule.zone}`); cell(row, date(schedule.nextRun));
      cell(row, schedule.enabled ? '启用' : '暂停');
      cell(row, '').append(button(schedule.enabled ? '暂停' : '启用', async () => {
        await api('/schedules/' + schedule.id + '/enabled', {method: 'PUT', body: JSON.stringify({enabled: !schedule.enabled})});
        await refresh(); notice('调度状态已更新');
      }));
    }
    if (!schedules.length) cell($('schedules').insertRow(), '暂无周期调度');
    $('nodes').replaceChildren();
    for (const node of overview.nodes) {
      const row = $('nodes').insertRow();
      cell(row, node.nodeId); cell(row, node.workerEnabled ? '执行节点' : 'API 节点');
      cell(row, `${node.active}/${node.capacity}`); cell(row, date(node.lastSeen));
      cell(row, node.online ? '在线' : '离线');
    }
    if (selected) await details(selected);
  } finally { refreshing = false; }
}
$('connect').addEventListener('submit', async event => {
  event.preventDefault(); apiKey = $('key').value.trim();
  try { await refresh(); notice('已连接 · 每 5 秒自动刷新'); }
  catch (error) { notice(error.message, true); }
});
$('handler').addEventListener('change', () => { $('payload').value = JSON.stringify(examples[$('handler').value], null, 2); });
$('submit-task').addEventListener('submit', async event => {
  event.preventDefault(); const submit = event.submitter; submit.disabled = true;
  try {
    const input = {...spec(), requestKey: $('request-key').value};
    if ($('available').value) input.availableAt = new Date($('available').value).toISOString();
    const task = await api('/tasks', {method: 'POST', body: JSON.stringify(input)});
    $('request-key').value = crypto.randomUUID(); offset = 0;
    await refresh(); await details(task.id); notice('任务已提交：' + task.id);
  } catch (error) { notice(error.message, true); }
  finally { submit.disabled = false; }
});
$('submit-schedule').addEventListener('submit', async event => {
  event.preventDefault(); event.submitter.disabled = true;
  try {
    await api('/schedules', {method: 'POST', body: JSON.stringify({name: $('schedule-name').value,
      cron: $('cron').value, zone: $('zone').value, task: spec()})});
    await refresh(); notice('周期调度已创建');
  } catch (error) { notice(error.message, true); }
  finally { event.submitter.disabled = false; }
});
$('cancel').addEventListener('click', async () => {
  if (!selected) return;
  try { await api('/tasks/' + selected + '/cancel', {method: 'POST'}); await refresh(); notice('取消请求已处理'); }
  catch (error) { notice(error.message, true); }
});
$('refresh').addEventListener('click', () => refresh().catch(error => notice(error.message, true)));
$('status').addEventListener('change', () => { offset = 0; refresh().catch(error => notice(error.message, true)); });
$('previous').addEventListener('click', () => { offset = Math.max(0, offset - size); refresh().catch(error => notice(error.message, true)); });
$('next').addEventListener('click', () => { offset += size; refresh().catch(error => notice(error.message, true)); });
setInterval(() => refresh().catch(error => notice(error.message, true)), 5000);
