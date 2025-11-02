document.addEventListener('DOMContentLoaded', () => {
  const envButtons = document.querySelectorAll('.env-switch .pill');
  envButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      envButtons.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
    });
  });
  document.querySelectorAll('[data-action="pause"]').forEach(btn => {
    btn.addEventListener('click', () => {
      const card = btn.closest('.pipeline-card');
      const badge = card.querySelector('.badge');
      badge.textContent = 'PAUSED';
      badge.className = 'badge warn';
    });
  });
  document.querySelectorAll('[data-action="resume"]').forEach(btn => {
    btn.addEventListener('click', () => {
      const card = btn.closest('.pipeline-card');
      const badge = card.querySelector('.badge');
      badge.textContent = 'RUNNING';
      badge.className = 'badge success';
    });
  });

  // 路由初始化与默认总览加载
  initNavRouting();
  // 总览采用方案A：加载聚合摘要
  loadSummary();
  // 新建作业表单事件绑定
  setupCreateJobForm();
  // 作业卡片操作事件绑定
  setupJobActions();
  // 作业详情弹窗交互绑定
  setupJobDetailModal();
  // 作业状态筛选交互绑定
  setupEditJobForm();
  setupJobsFilters();
  // 作业搜索输入绑定
  setupJobsSearch();
  // 分页控件交互绑定
  setupJobsPagination();
  // 初始化分页默认值
  window.__jobsPage = 1;
  window.__jobsPageSize = 12;
});

function initNavRouting() {
  const navItems = document.querySelectorAll('.sidebar .nav-item');
  const pages = document.querySelectorAll('.content .page');
  function showPage(target) {
    pages.forEach(p => {
      const active = p.dataset.page === target;
      p.classList.toggle('active', active);
      p.style.display = active ? 'block' : 'none';
    });
    // 页面数据加载
    if (target === 'overview') {
      loadSummary();
    } else if (target === 'topics') {
      loadKafkaTopicsInto('topics-page-list');
    } else if (target === 'jobs') {
      loadStarRocksJobsInto('jobs-page-list');
    }
  }
  navItems.forEach(link => {
    link.addEventListener('click', (e) => {
      e.preventDefault();
      const target = link.dataset.page;
      navItems.forEach(n => n.classList.remove('active'));
      link.classList.add('active');
      showPage(target || 'overview');
    });
  });
  // 初始仅显示 active 页面
  pages.forEach(p => p.style.display = p.classList.contains('active') ? 'block' : 'none');
}

// 总览摘要加载（方案A）
async function loadSummary() {
  try {
    const res = await fetch('/api/summary');
    const s = await res.json();
    // 统计卡片
    setStatByTitle('运行中的作业', s?.jobs?.running ?? 0);
    const tp = s?.throughput?.current ?? 0;
    setStatByTitle('每分钟吞吐', formatNumber(tp));
    setProgressPercent(tp > 0 ? Math.min(100, Math.round(tp / 1000)) : 0);
    setStatByTitle('错误行（近10分钟）', s?.errors?.last_10m ?? 0, true);
    const lagMs = s?.lag?.p95_ms ?? 0;
    setStatByTitle('消费延迟', (lagMs/1000).toFixed(1) + 's');

    // 状态分布
    const distBox = document.getElementById('distribution-grid');
    if (distBox) distBox.innerHTML = renderDistributionCards(s);

    // 异常 Top3
    const anBox = document.getElementById('anomaly-list');
    const anoms = Array.isArray(s?.anomalies) ? s.anomalies : [];
    if (anBox) {
      anBox.innerHTML = anoms.length ? anoms.map(renderAnomalyCard).join('') : '<div class="empty muted">暂无异常</div>';
    }
  } catch (e) {
    console.warn('加载摘要失败', e);
    const distBox = document.getElementById('distribution-grid');
    if (distBox) distBox.innerHTML = '<div class="empty muted">加载失败，请稍后重试</div>';
    const anBox = document.getElementById('anomaly-list');
    if (anBox) anBox.innerHTML = '<div class="empty muted">加载失败，请稍后重试</div>';
  }
}

function setStatByTitle(title, value, warn=false) {
  const cards = document.querySelectorAll('.stat-card');
  cards.forEach(card => {
    const t = card.querySelector('.stat-title');
    const v = card.querySelector('.stat-value');
    if (t && v && t.textContent.trim() === title) {
      v.textContent = String(value);
      v.classList.toggle('warn', !!warn);
    }
  });
}

function setProgressPercent(pct) {
  const bars = document.querySelectorAll('.progress .bar');
  bars.forEach(bar => { bar.style.width = pct + '%'; });
}

function formatNumber(n) {
  const x = Number(n)||0;
  if (x >= 1000) return Math.round(x/1000) + 'k';
  return String(x);
}

function setupCreateJobForm() {
  const openBtn = document.getElementById('btn-open-create-job');
  const cancelBtn = document.getElementById('btn-cancel-create-job');
  const submitBtn = document.getElementById('btn-submit-create-job');
  const modal = document.getElementById('modal-create-job');
  const overlay = modal ? modal.querySelector('.modal-overlay') : null;
  if (!openBtn || !cancelBtn || !submitBtn || !modal) return;
  openBtn.addEventListener('click', () => { modal.classList.remove('hidden'); });
  cancelBtn.addEventListener('click', () => { modal.classList.add('hidden'); });
  if (overlay) overlay.addEventListener('click', () => { modal.classList.add('hidden'); });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') modal.classList.add('hidden');
  });
  submitBtn.addEventListener('click', async () => {
    submitCreateJob().catch(e => {
      console.warn('创建作业失败', e);
      alert('创建失败：' + (e?.message || '未知错误'));
    });
  });
}

async function submitCreateJob() {
  const name = document.getElementById('cj-name').value.trim();
  const table = document.getElementById('cj-table').value.trim();
  const brokers = document.getElementById('cj-brokers').value.trim() || 'kafka:9092';
  const topic = document.getElementById('cj-topic').value.trim();
  let group = document.getElementById('cj-group').value.trim();
  if (!group && topic) group = 'sr-' + topic;
  const columnsStr = document.getElementById('cj-columns').value.trim();
  const columns = columnsStr ? columnsStr.split(',').map(s => s.trim()).filter(Boolean) : [];
  const setStr = document.getElementById('cj-set').value.trim();
  const jsonpathsStr = document.getElementById('cj-jsonpaths').value.trim();
  const concurrency = Number(document.getElementById('cj-concurrency').value) || 3;
  const interval = Number(document.getElementById('cj-interval').value) || 5;

  if (!name || !table || !topic) {
    alert('请填写作业名称、目标表、Kafka 主题');
    return;
  }

  const setMap = {};
  if (setStr && setStr.includes('=')) {
    const i = setStr.indexOf('=');
    const k = setStr.slice(0, i).trim();
    const v = setStr.slice(i+1).trim();
    if (k && v) setMap[k] = v;
  }
  let jsonpaths = jsonpathsStr;
  if (!jsonpaths) {
    // 根据列自动生成简单 jsonpaths
    jsonpaths = '[' + columns.map(c => `"$.${c}"`).join(',') + ']';
  }

  const payload = {
    name,
    table,
    kafka: { broker_list: brokers, topic, group_id: group || ('sr-'+name) },
    columns,
    set: setMap,
    properties: {
      desired_concurrent_number: String(concurrency),
      max_batch_interval: String(interval),
      max_batch_rows: '200000',
      max_batch_size: '209715200',
      strict_mode: 'false',
      format: 'json',
      jsonpaths: jsonpaths,
    }
  };

  const res = await fetch('/api/starrocks/jobs', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const data = await res.json();
  if (!res.ok || data?.ok !== true) {
    throw new Error(data?.error || '请求失败');
  }
  alert('创建成功：' + name);
  document.getElementById('modal-create-job').classList.add('hidden');
  // 刷新列表
  loadStarRocksJobsInto('jobs-page-list');
}

function setupJobActions() {
  const box = document.getElementById('jobs-page-list');
  if (!box) return;
  box.addEventListener('click', async (e) => {
    const titleEl = e.target.closest('h3');
    if (titleEl && titleEl.parentElement && titleEl.closest('.data-card')) {
      const card = titleEl.closest('.data-card');
      const name = card?.dataset?.name;
      if (name) {
        openJobDetail(name).catch(err => {
          console.warn('加载作业详情失败', err);
          alert('加载作业详情失败：' + (err?.message || '未知错误'));
        });
        return;
      }
    }
    const btn = e.target.closest('button');
    if (!btn || !btn.dataset || !btn.dataset.action) return;
    const card = btn.closest('.data-card');
    const name = card?.dataset?.name;
    const action = btn.dataset.action;
    if (!name) return;
    try {
      if (action === 'pause') {
        const res = await fetch(`/api/starrocks/jobs/${encodeURIComponent(name)}/pause`, { method: 'POST' });
        const data = await res.json(); if (!res.ok || data?.ok !== true) throw new Error(data?.error || '暂停失败');
        alert('已暂停：' + name);
      } else if (action === 'resume') {
        const res = await fetch(`/api/starrocks/jobs/${encodeURIComponent(name)}/resume`, { method: 'POST' });
        const data = await res.json(); if (!res.ok || data?.ok !== true) throw new Error(data?.error || '恢复失败');
        alert('已恢复：' + name);
      } else if (action === 'stop' || action === 'delete') {
        if (!confirm('确定要停止/删除该作业吗？')) return;
        const res = await fetch(`/api/starrocks/jobs/${encodeURIComponent(name)}/stop`, { method: 'POST' });
        const data = await res.json(); if (!res.ok || data?.ok !== true) throw new Error(data?.error || '停止失败');
        alert('已停止：' + name);
      }
      if (action === 'edit') {
        openEditJob(name).catch(err => {
          console.warn('打开作业编辑弹窗失败', err);
          alert('打开编辑弹窗失败：' + (err?.message || '未知错误'));
        });
        return; // 打开编辑弹窗不刷新列表
      }
      // 操作后刷新列表
      loadStarRocksJobsInto('jobs-page-list');
    } catch (err) {
      console.warn('作业操作失败', err);
      alert('操作失败：' + (err?.message || '未知错误'));
    }
  });
}

function setupJobDetailModal() {
  const modal = document.getElementById('modal-job-detail');
  if (!modal) return;
  const overlay = modal.querySelector('.modal-overlay');
  const closeBtn = document.getElementById('btn-close-job-detail');
  const close = () => modal.classList.add('hidden');
  if (overlay) overlay.addEventListener('click', close);
  if (closeBtn) closeBtn.addEventListener('click', close);
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') close(); });
}

async function openJobDetail(name) {
  const res = await fetch(`/api/starrocks/jobs/${encodeURIComponent(name)}`);
  const d = await res.json();
  if (!res.ok) throw new Error(d?.error || '请求失败');
  const modal = document.getElementById('modal-job-detail');
  if (!modal) return;
  // 填充基本信息
  const setText = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = String(val ?? '—'); };
  setText('jd-title', `作业详情 · ${d.name || name}`);
  setText('jd-name', d.name || name);
  setText('jd-state', (d.state || '').toUpperCase());
  setText('jd-table', d.table || '—');
  setText('jd-sql', d.create_sql || '—');
  // Properties
  const propsBox = document.getElementById('jd-props');
  if (propsBox) {
    propsBox.innerHTML = '';
    const props = d.properties || {};
    const keys = Object.keys(props).sort();
    if (keys.length === 0) {
      propsBox.innerHTML = '<div class="muted">—</div>';
    } else {
      keys.forEach(k => {
        const v = props[k];
        const div = document.createElement('div');
        div.innerHTML = `<span class="key">${k}</span><span class="val">${v}</span>`;
        propsBox.appendChild(div);
      });
    }
  }
  // Kafka
  const kafkaBox = document.getElementById('jd-kafka');
  if (kafkaBox) {
    kafkaBox.innerHTML = '';
    const kk = d.kafka || {};
    const keys = Object.keys(kk).sort();
    if (keys.length === 0) {
      kafkaBox.innerHTML = '<div class="muted">—</div>';
    } else {
      keys.forEach(k => {
        const v = kk[k];
        const div = document.createElement('div');
        div.innerHTML = `<span class="key">${k}</span><span class="val">${v}</span>`;
        kafkaBox.appendChild(div);
      });
    }
  }
  modal.classList.remove('hidden');
}

function setupEditJobForm() {
  const modal = document.getElementById('modal-edit-job');
  if (!modal) return;
  const cancelBtn = document.getElementById('btn-cancel-edit-job');
  const submitBtn = document.getElementById('btn-submit-edit-job');
  const overlay = modal.querySelector('.modal-overlay');
  const close = () => modal.classList.add('hidden');
  if (cancelBtn) cancelBtn.addEventListener('click', close);
  if (overlay) overlay.addEventListener('click', close);
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') close(); });
  if (submitBtn) {
    submitBtn.addEventListener('click', async () => {
      const name = modal.dataset.editingJob;
      if (!name) {
        alert('未指定要修改的作业');
        return;
      }
      submitEditJob(name).catch(e => {
        console.warn('修改作业失败', e);
        alert('修改失败：' + (e?.message || '未知错误'));
      });
    });
  }
}

async function openEditJob(name) {
  const res = await fetch(`/api/starrocks/jobs/${encodeURIComponent(name)}`);
  const d = await res.json();
  if (!res.ok) throw new Error(d?.error || '请求失败');
  const modal = document.getElementById('modal-edit-job');
  if (!modal) return;
  
  modal.dataset.editingJob = name;
  
  const titleEl = document.getElementById('ej-title');
  if (titleEl) titleEl.textContent = `修改 Routine Load · ${name}`;

  const p = d.properties || {};
  const setValue = (id, value) => {
    const el = document.getElementById(id);
    if (el) {
      if (el.tagName === 'SELECT') {
        el.value = (value === true || value === 'true') ? 'true' : (value === false || value === 'false') ? 'false' : '';
      } else {
        el.value = value ?? '';
      }
    }
  };

  setValue('ej-desired_concurrent_number', p.desired_concurrent_number);
  setValue('ej-max_error_number', p.max_error_number);
  setValue('ej-max_batch_interval', p.max_batch_interval);
  setValue('ej-max_batch_rows', p.max_batch_rows);
  setValue('ej-max_batch_size', p.max_batch_size);
  setValue('ej-jsonpaths', p.jsonpaths);
  setValue('ej-json_root', p.json_root);
  setValue('ej-strip_outer_array', p.strip_outer_array);
  setValue('ej-strict_mode', p.strict_mode);
  setValue('ej-timezone', p.timezone);
  
  modal.classList.remove('hidden');
}

async function submitEditJob(name) {
  const getValue = (id) => {
    const el = document.getElementById(id);
    return el ? el.value.trim() : null;
  };

  const props = {
    desired_concurrent_number: getValue('ej-desired_concurrent_number'),
    max_error_number: getValue('ej-max_error_number'),
    max_batch_interval: getValue('ej-max_batch_interval'),
    max_batch_rows: getValue('ej-max_batch_rows'),
    max_batch_size: getValue('ej-max_batch_size'),
    jsonpaths: getValue('ej-jsonpaths'),
    json_root: getValue('ej-json_root'),
    strip_outer_array: getValue('ej-strip_outer_array'),
    strict_mode: getValue('ej-strict_mode'),
    timezone: getValue('ej-timezone'),
  };

  const payload = {};
  for (const key in props) {
    if (props[key] !== null && props[key] !== '') {
      payload[key] = props[key];
    }
  }

  if (Object.keys(payload).length === 0) {
    alert('没有要修改的属性');
    return;
  }

  const res = await fetch(`/api/starrocks/jobs/${encodeURIComponent(name)}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ properties: payload }),
  });

  if (!res.ok) {
    const err = await res.json();
    throw new Error(err?.error || '请求失败');
  }
  
  alert('作业 ' + name + ' 修改成功');
  document.getElementById('modal-edit-job').classList.add('hidden');
  loadStarRocksJobsInto('jobs-page-list');
}

function renderDistributionCards(s) {
  const j = s?.jobs || {}; const k = s?.kafka || {};
  const jobsTotal = Number(j.total || 0);
  const jobsItems = [
    { label: 'RUNNING', value: Number(j.running || 0), color: 'var(--success)' },
    { label: 'PAUSED', value: Number(j.paused || 0), color: 'var(--warn)' },
    { label: 'FAILED', value: Number(j.failed || 0), color: 'var(--error)' },
  ];
  const parts = Number(k.partitions || 0);
  const under = Math.min(Number(k.under_replicated || 0), parts);
  const healthy = Math.max(0, parts - under);
  const kafkaItems = [
    { label: '健康分区', value: healthy, color: 'var(--info)' },
    { label: '欠副本', value: under, color: 'var(--warn)' },
  ];
  const kafkaTitle = 'Kafka 副本健康';
  const cards = [
    renderPieCard('作业状态分布', jobsTotal, jobsItems),
    renderPieCard(kafkaTitle, parts, kafkaItems, { extra: `<div class="kv"><div><span class="key">主题</span><span class="val">${Number(k.topics||0)}</span></div></div>` })
  ];
  return cards.join('');
}

function renderPieCard(title, total, items, opts={}) {
  const totalVal = Number(total || items.reduce((a,b)=>a+(Number(b.value)||0),0));
  const svg = buildDonutSVG(items, totalVal);
  const legend = items.map(i => {
    const v = Number(i.value || 0);
    const pct = totalVal>0 ? Math.round(v/totalVal*100) : 0;
    return `<div class="legend-item"><span class="legend-dot" style="background:${i.color}"></span><span class="legend-label">${i.label}</span><span class="legend-val">${v}（${pct}%）</span></div>`;
  }).join('');
  const extra = opts.extra || '';
  return `
    <article class="data-card pie-card">
      <div class="card-header">
        <div>
          <h3>${title}</h3>
          <p class="muted">总数 ${totalVal}</p>
        </div>
      </div>
      <div class="pie-wrap">
        ${svg}
        <div class="pie-legend">${legend}</div>
      </div>
      ${extra}
    </article>
  `;
}

function buildDonutSVG(items, total) {
  const r = 42; const cx = 50; const cy = 50; const strokeW = 12;
  const C = 2 * Math.PI * r;
  let offset = 0;
  const segs = items.map(i => {
    const val = Number(i.value||0);
    const len = total>0 ? (val/total) * C : 0;
    const seg = `<circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="${i.color}" stroke-width="${strokeW}" stroke-dasharray="${len} ${C-len}" stroke-dashoffset="${-offset}" transform="rotate(-90 ${cx} ${cy})" />`;
    offset += len;
    return seg;
  }).join('');
  const holeText = `<text x="${cx}" y="${cy+4}" text-anchor="middle" font-size="12" fill="#1A2B3C">${total}</text>`;
  const bg = `<circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="#EEF5FF" stroke-width="${strokeW}" />`;
  return `<svg class="pie-chart" viewBox="0 0 100 100" width="140" height="140">${bg}${segs}${holeText}</svg>`;
}

function renderAnomalyCard(a) {
  const name = a?.name || '-';
  const type = a?.type || '-';
  const state = (a?.state || '-').toUpperCase();
  const cls = state === 'FAILED' ? 'warn' : state === 'PAUSED' ? 'warn' : 'info';
  return `
    <article class="data-card">
      <div class="card-header">
        <div>
          <h3>${name}</h3>
          <p class="muted">${type}</p>
        </div>
        <span class="badge ${cls}">${state}</span>
      </div>
    </article>
  `;
}

async function loadKafkaTopics() {
  const box = document.getElementById('topics-list');
  try {
    const res = await fetch('/api/kafka/topics');
    const topics = await res.json();
    if (!Array.isArray(topics) || topics.length === 0) {
      box.innerHTML = '<div class="empty muted">暂无主题数据</div>';
      return;
    }
    box.innerHTML = topics.map(t => renderTopicCard(t)).join('');
  } catch (e) {
    console.warn('加载 Kafka 主题失败', e);
    box.innerHTML = '<div class="empty muted">加载失败，请稍后重试</div>';
  }
}

async function loadKafkaTopicsInto(containerId) {
  const box = document.getElementById(containerId);
  if (!box) return;
  try {
    const res = await fetch('/api/kafka/topics');
    const topics = await res.json();
    if (!Array.isArray(topics) || topics.length === 0) {
      box.innerHTML = '<div class="empty muted">暂无主题数据</div>';
      return;
    }
    box.innerHTML = topics.map(t => renderTopicCard(t)).join('');
  } catch (e) {
    console.warn('加载 Kafka 主题失败', e);
    box.innerHTML = '<div class="empty muted">加载失败，请稍后重试</div>';
  }
}

function renderTopicCard(t) {
  const name = t.name || '-';
  const partitions = t.partitions != null ? t.partitions : '-';
  return `
    <article class="data-card">
      <div class="card-header">
        <div>
          <h3>${name}</h3>
          <p class="muted">Kafka 主题</p>
        </div>
        <span class="badge info">${partitions} 分区</span>
      </div>
      <div class="kv">
        <div><span class="key">副本因子</span><span class="val">—</span></div>
        <div><span class="key">消费组</span><span class="val">—</span></div>
      </div>
    </article>
  `;
}

async function loadStarRocksJobs() {
  const box = document.getElementById('jobs-list');
  try {
    const res = await fetch('/api/starrocks/jobs');
    const jobs = await res.json();
    if (!Array.isArray(jobs) || jobs.length === 0) {
      box.innerHTML = '<div class="empty muted">暂无作业数据</div>';
      // 更新统计：运行中的作业置为 0
      setRunningJobsCount(0);
      return;
    }
    box.innerHTML = jobs.map(j => renderJobCard(j)).join('');
    // 更新统计：根据 RUNNING 数量刷新
    const running = jobs.filter(j => (j.state||'').toUpperCase() === 'RUNNING').length;
    setRunningJobsCount(running);
  } catch (e) {
    console.warn('加载 StarRocks 作业失败', e);
    box.innerHTML = '<div class="empty muted">加载失败，请稍后重试</div>';
  }
}

async function loadStarRocksJobsInto(containerId) {
  const box = document.getElementById(containerId);
  if (!box) return;
  try {
    const page = Number(window.__jobsPage || 1);
    let size = Number(window.__jobsPageSize || 12);
    const filter = (window.__jobFilter || 'ALL').toUpperCase();
    const q = (window.__jobsSearchQuery || '').trim();
    // 搜索时扩大单页数量，以提升匹配效果
    if (q) size = Math.max(size, 60);
    const params = new URLSearchParams({ page: String(page), page_size: String(size) });
    if (filter !== 'ALL') params.set('state', filter);
    const res = await fetch('/api/starrocks/jobs?' + params.toString());
    const jobs = await res.json();
    if (!Array.isArray(jobs) || jobs.length === 0) {
      box.innerHTML = '<div class="empty muted">暂无作业数据</div>';
      updateJobsPagination(0);
      return;
    }
    box.innerHTML = jobs.map(j => renderJobCard(j)).join('');
    // 更新分页信息
    const total = Number(res.headers.get('X-Total-Count') || '0');
    updateJobsPagination(total);
    // 保险：如果后端未按筛选返回，也应用前端筛选
    applyJobFilter();
  } catch (e) {
    console.warn('加载 StarRocks 作业失败', e);
    box.innerHTML = '<div class="empty muted">加载失败，请稍后重试</div>';
  }
}

function renderJobCard(j) {
  const name = j.name || '-';
  const state = (j.state || '-').toUpperCase();
  const table = j.table || '-';
  const processed = (typeof j.processed === 'number') ? j.processed : (j.processed_rows || j.loaded_rows || null);
  const errors = (typeof j.errors === 'number') ? j.errors : (j.error_rows || null);
  const badgeClass = state === 'RUNNING' ? 'success' : state === 'PAUSED' ? 'warn' : 'info';
  const footer = (state === 'RUNNING')
    ? '<button class="ghost" data-action="pause">暂停</button><button class="ghost" data-action="edit">修改</button><button class="secondary" data-action="stop">停止</button>'
    : (state === 'PAUSED')
      ? '<button class="primary" data-action="resume">恢复</button><button class="ghost" data-action="edit">修改</button><button class="secondary" data-action="stop">停止</button>'
      : '<button class="ghost" data-action="edit">修改</button><button class="secondary" data-action="delete">删除</button>';
  return `
    <article class="data-card" data-name="${name}" data-table="${table}" data-state="${state}">
      <div class="card-header">
        <div>
          <h3>${name}</h3>
          <p class="muted">Routine Load → 表 <code>${table}</code></p>
        </div>
        <span class="badge ${badgeClass}">${state}</span>
      </div>
      <div class="kv">
        <div><span class="key">已处理行</span><span class="val">${processed!=null ? processed : '—'}</span></div>
        <div><span class="key">错误行</span><span class="val">${errors!=null ? errors : '—'}</span></div>
      </div>
      <div class="card-footer">
        ${footer}
        <div class="spacer"></div>
      </div>
    </article>
  `;
}

function setRunningJobsCount(n) {
  const statCards = document.querySelectorAll('.stat-card .stat-title');
  statCards.forEach((el) => {
    if (el.textContent.trim() === '运行中的作业') {
      const valEl = el.parentElement.querySelector('.stat-value');
      if (valEl) valEl.textContent = String(n);
    }
  });
}

// ===== 作业状态筛选 =====
function setupJobsFilters() {
  const container = document.getElementById('jobs-filters');
  if (!container) return;
  const pills = container.querySelectorAll('.pill');
  // 默认筛选：全部
  window.__jobFilter = 'ALL';
  pills.forEach(btn => {
    btn.addEventListener('click', () => {
      pills.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      const f = (btn.dataset.filter || 'all').toUpperCase();
      window.__jobFilter = f;
      // 切换筛选时重置到第一页并重新加载
      window.__jobsPage = 1;
      loadStarRocksJobsInto('jobs-page-list');
    });
  });
}

function ensureJobsNoMatchBox() {
  const list = document.getElementById('jobs-page-list');
  if (!list) return null;
  let box = document.getElementById('jobs-no-match');
  if (!box) {
    box = document.createElement('div');
    box.id = 'jobs-no-match';
    box.className = 'empty muted';
    box.textContent = '没有匹配的作业';
    box.style.display = 'none';
    list.appendChild(box);
  }
  return box;
}

function applyJobFilter() {
  const list = document.getElementById('jobs-page-list');
  if (!list) return;
  const cards = list.querySelectorAll('.data-card');
  const filter = (window.__jobFilter || 'ALL').toUpperCase();
  const q = (window.__jobsSearchQuery || '').toLowerCase();
  let anyVisible = false;
  cards.forEach(card => {
    const state = (card.dataset.state || '').toUpperCase();
    const byFilter = (filter === 'ALL') || (state === filter);
    const name = (card.dataset.name || '').toLowerCase();
    const table = (card.dataset.table || '').toLowerCase();
    const byQuery = !q || name.includes(q) || table.includes(q);
    const show = byFilter && byQuery;
    card.style.display = show ? '' : 'none';
    if (show) anyVisible = true;
  });
  const emptyBox = ensureJobsNoMatchBox();
  if (emptyBox) emptyBox.style.display = anyVisible ? 'none' : 'block';
}

// ===== 分页控件 =====
function setupJobsPagination() {
  const container = document.getElementById('jobs-pagination');
  if (!container) return;
  container.addEventListener('click', (e) => {
    const btn = e.target.closest('button');
    if (!btn) return;
    const act = btn.dataset.pageAction;
    const totalPages = Number(window.__jobsTotalPages || 1);
    if (act === 'prev') {
      if (window.__jobsPage > 1) window.__jobsPage--;
    } else if (act === 'next') {
      if (window.__jobsPage < totalPages) window.__jobsPage++;
    }
    loadStarRocksJobsInto('jobs-page-list');
  });
}

function updateJobsPagination(total) {
  const size = Number(window.__jobsPageSize || 12);
  const totalPages = Math.max(1, Math.ceil((Number(total) || 0) / size));
  window.__jobsTotalPages = totalPages;
  const page = Math.min(Number(window.__jobsPage || 1), totalPages);
  window.__jobsPage = page;
  const indicator = document.getElementById('jobs-page-indicator');
  if (indicator) indicator.textContent = `第 ${page} / ${totalPages} 页`;
  const container = document.getElementById('jobs-pagination');
  if (container) {
    const prev = container.querySelector('[data-page-action="prev"]');
    const next = container.querySelector('[data-page-action="next"]');
    if (prev) prev.disabled = page <= 1;
    if (next) next.disabled = page >= totalPages;
  }
}

// ===== 作业搜索输入 =====
function setupJobsSearch() {
  const input = document.getElementById('jobs-search-input');
  if (!input) return;
  window.__jobsSearchQuery = '';
  input.addEventListener('input', () => {
    window.__jobsSearchQuery = (input.value || '').trim().toLowerCase();
    // 搜索时回到第一页，重新加载数据并应用筛选
    window.__jobsPage = 1;
    loadStarRocksJobsInto('jobs-page-list');
  });
}