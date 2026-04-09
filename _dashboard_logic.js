// ============================================================
// ② QUEUE CONFIG — 7 queues
// ============================================================
const QUEUES = [
  {
    id:'q1_toufang', name:'投放误漏', fullName:'【供应商】投放误漏case',
    icon:'📢', color:'#3b82f6',
    metricKeys:['violation_rate','miss_rate'],
    metricLabels:{'violation_rate':'违规准确率','miss_rate':'漏率'},
    dataStatus:'pending', data:[]
  },
  {
    id:'q2_erjiansimple', name:'简单二审', fullName:'【供应商】简单二审误漏case',
    icon:'📋', color:'#22c55e',
    metricKeys:['violation_rate','miss_rate'],
    metricLabels:{'violation_rate':'违规准确率','miss_rate':'漏率'},
    dataStatus:'pending', data:[]
  },
  {
    id:'q3_erjian_4qi_gt', name:'四期-二审GT', fullName:'【四期供应商】二审周推质检分歧单（二审GT）',
    icon:'🔄', color:'#f97316',
    metricKeys:['violation_rate','miss_rate'],
    metricLabels:{'violation_rate':'违规准确率','miss_rate':'漏率'},
    dataStatus:'pending', data:[]
  },
  {
    id:'q3b_erjian_4qi_qiepian', name:'四期-切片GT', fullName:'【四期供应商】二审周推质检分歧单（二审切片GT）',
    icon:'🔪', color:'#f59e0b',
    metricKeys:['violation_rate','miss_rate'],
    metricLabels:{'violation_rate':'违规准确率','miss_rate':'漏率'},
    dataStatus:'pending', data:[]
  },
  {
    id:'q4_jubao_4qi', name:'四期-举报', fullName:'【四期供应商】举报周推质检分歧单',
    icon:'🚨', color:'#a855f7', hasPrePost:true,
    metricKeys:['pre_violation_rate','pre_miss_rate','pre_accuracy','post_violation_rate','post_miss_rate','post_accuracy'],
    metricLabels:{
      'pre_violation_rate':'申诉前-违规准确率','pre_miss_rate':'申诉前-漏率','pre_accuracy':'申诉前-准确率',
      'post_violation_rate':'申诉后-违规准确率','post_miss_rate':'申诉后-漏率','post_accuracy':'申诉后-准确率'
    },
    dataStatus:'pending', data:[]
  },
  {
    id:'q5_lahei', name:'拉黑误漏', fullName:'【供应商】拉黑误漏case',
    icon:'🚫', color:'#ef4444',
    metricKeys:['violation_rate','miss_rate'],
    metricLabels:{'violation_rate':'违规准确率','miss_rate':'漏率'},
    dataStatus:'pending', data:[]
  },
  {
    id:'q6_shangqiang', name:'上墙文本', fullName:'上墙文本申诉-云雀',
    icon:'📝', color:'#06b6d4',
    metricKeys:['audit_accuracy'],
    metricLabels:{'audit_accuracy':'审核准确率'},
    metricColors:{'audit_accuracy':'#06b6d4'},
    dataStatus:'pending', data:[]
  }
];

let activeQueueId = 'q6_shangqiang';
let filteredData = [];
let sortCol = null, sortDir = 'desc';
let chartTrend=null, chartExtra1=null, chartExtra2=null;

// ============================================================
// ③ HYDRATE
// ============================================================
function hydrateQueuesFromDB() {
  if (!DB_DATA || typeof DB_DATA !== 'object') return;
  // Safety: filter out future dates (today and before only)
  var todayStr = new Date().toISOString().split('T')[0];
  QUEUES.forEach(q => {
    const rows = DB_DATA[q.id];
    if (!rows || !Array.isArray(rows) || rows.length===0) return;
    const sorted = [...rows].sort((a,b)=>(a.date||'').localeCompare(b.date||''));
    const byDate = {};
    sorted.forEach(r => {
      const d = r.date;
      if (!d || d > todayStr) return; // skip future dates
      if (!byDate[d]) byDate[d] = { date:d };
      Object.keys(r).forEach(k => {
        if (k!=='_queueName' && k!=='date' && r[k]!=null) byDate[d][k]=r[k];
      });
    });
    q.data = Object.values(byDate).sort((a,b)=>a.date.localeCompare(b.date));
    q.dataStatus = 'loaded';
  });
}

// ============================================================
// ④ INIT UI
// ============================================================
function initUI() {
  var qt = document.getElementById('queueTabs');
  qt.innerHTML = QUEUES.map(function(q){
    var act = q.id===activeQueueId ? ' active' : '';
    var bg = act ? ('background:'+q.color+';color:#fff;border-color:transparent;') : '';
    var badge = q.dataStatus==='loaded'
      ? '<span class="q-badge badge-blue">'+q.data.length+'天</span>'
      : '<span class="q-badge badge-amber">待接入</span>';
    return '<div class="queue-tab'+act+'" onclick="switchQueue(\''+q.id+'\')" style="'+bg+'">'
      +'<span class="q-dot" style="background:'+q.color+'"></span> '+q.icon+' '+q.name+badge+'</div>';
  }).join('');

  document.getElementById('overviewRow').innerHTML = QUEUES.map(function(q){
    var d = q.data || [];
    var mk = q.metricKeys[0];
    var ml = (q.metricLabels&&q.metricLabels[mk]) || mk;
    // Find latest non-zero value (walk backwards)
    var vs = '--', ds = d.length > 0 ? d[d.length-1].date : ml;
    for(var di=d.length-1; di>=0; di--){
      var dv = d[di][mk];
      if(dv != null && typeof dv==='number' && dv !== 0){
        vs = (dv*100).toFixed(1)+'%'; break;
      }
    }
    return '<div class="overview-card" onclick="switchQueue(\''+q.id+'\')">'
      +'<div class="overview-icon">'+q.icon+'</div>'
      +'<div class="overview-name">'+q.name+'</div>'
      +'<div class="overview-value" style="color:'+q.color+'">'+vs+'</div>'
      +'<div class="overview-sub">'+ds+'</div></div>';
  }).join('');

  var allDates = [];
  QUEUES.forEach(function(q){ (q.data||[]).forEach(function(r){ if(r.date) allDates.push(r.date); }); });
  allDates.sort();
  if(allDates.length > 0) {
    document.getElementById('dateFrom').value = allDates[0];
    document.getElementById('dateTo').value = allDates[allDates.length-1];
  }
  document.getElementById('dateFrom').addEventListener('change', applyFilter);
  document.getElementById('dateTo').addEventListener('change', applyFilter);
  document.getElementById('lastUpdate').textContent = new Date().toLocaleString('zh-CN');
}

// ============================================================
// ⑤ SWITCH / FILTER / PERIOD
// ============================================================
function switchQueue(id) { activeQueueId=id; initUI(); applyFilter(); }

function applyFilter() {
  var q = QUEUES.find(x=>x.id===activeQueueId);
  if(!q || !q.data || !q.data.length) { filteredData=[]; renderAll(q); return; }
  var f=document.getElementById('dateFrom').value;
  var t=document.getElementById('dateTo').value;
  filteredData=q.data.filter(r => (!f || r.date>=f) && (!t || r.date<=t));
  renderAll(q);
}

function setPeriod(p) {
  document.querySelectorAll('.period-toggle button').forEach(b=>b.classList.remove('active'));
  document.getElementById('btn-'+p).classList.add('active');
  var q=QUEUES.find(x=>x.id===activeQueueId); var d=(q&&q.data)||[];
  if(!d.length) return;
  var dates=d.map(r=>r.date).sort(); var last=dates[dates.length-1], start;
  if(p==='day') start=dates[0];
  else if(p==='week') {var dt=new Date(last);dt.setDate(dt.getDate()-6);start=dt.toISOString().split('T')[0];}
  else {var dt=new Date(last);dt.setMonth(dt.getMonth()-1);dt.setDate(dt.getDate()+1);start=dt.toISOString().split('T')[0];}
  document.getElementById('dateFrom').value=start; document.getElementById('dateTo').value=last; applyFilter();
}
function resetFilter() { setPeriod('day'); }

// ============================================================
// ⑥ RENDER STATS
// ============================================================
function renderStats(q) {
  if(!filteredData || !filteredData.length) {
    document.getElementById('statsGrid').innerHTML='<div class="empty-state"><div class="icon">⏳</div>该队列暂无数据。</div>'; return;
  }
  var keys=q.metricKeys, labels=q.metricLabels||{}, colors=q.metricColors||{}, CC=['#3b82f6','#22c55e','#ef4444','#f97316','#eab308','#a855f7','#06b6d4'];
  var cards=[{label:'数据天数', value:filteredData.length+' 天', sub:(filteredData[0]||{}).date+' ~ '+filteredData[filteredData.length-1].date, color:q.color}];
  keys.forEach(function(k,i){
    // Collect all non-null values with their original indices for trend calc
    var vals=[];
    filteredData.forEach(function(r,idx){ if(r[k]!=null&&!isNaN(r[k])) vals.push({v:r[k],i:idx}); });
    if(!vals.length) return;

    // Find latest non-zero value for display
    var lastVal=null;
    for(var vi=vals.length-1;vi>=0;vi--){if(vals[vi].v!==0){lastVal=vals[vi].v;break;}}
    if(lastVal==null) lastVal=0;

    // Average from ALL non-null values
    var rawVals=vals.map(function(x){return x.v;});
    var sum=rawVals.reduce((a,b)=>a+b,0);
    avgVal=sum/rawVals.length;

    // Trend: compare last non-zero vs previous non-zero
    var trendDir='flat', trendVal=0;
    if(vals.length>2){
      var lastIdx=-1; for(var ti=vals.length-1;ti>=0;ti--){if(vals[ti].v!==0){lastIdx=ti;break;}}
      if(lastIdx>0){
        var prevIdx=-1; for(var pi=lastIdx-1;pi>=0;pi--){if(vals[pi].v!==0){prevIdx=pi;break;}}
        if(prevIdx>=0 && vals[lastIdx].v!==0){
          var pV=vals[prevIdx].v, lV=vals[lastIdx].v;
          trendVal=((lV-pV)/Math.abs(pV))*100;
          trendDir=trendVal>0?'up':trendVal<0?'down':'flat';
        }
      }
    }

    // Format: always show % for rate/accuracy fields
    var isRate = k.includes('rate') || k.includes('accuracy');
    function fmt(v){ return isRate ? (v*100).toFixed(2)+'%' : Number(v).toFixed(2); }
    function fmtAvg(v){ return isRate ? (v*100).toFixed(1)+'%' : Number(v).toFixed(1); }

    cards.push({
      label:labels[k]||k,
      value:lastVal!=null?fmt(lastVal):'--',
      sub:'均值 '+fmtAvg(avgVal),
      trendVal:Number(trendVal), trendDir:trendDir,
      color:colors[k]||CC[i%CC.length]
    });
  });
  document.getElementById('statsGrid').innerHTML=cards.map(c=>{
    var arrow=c.trendDir==='up'?'↑':c.trendDir==='down'?'↓':'→';
    var t=c.trendVal!==undefined?'<div class="stat-trend trend-'+c.trendDir+'">'+arrow+' '+Math.abs(c.trendVal).toFixed(1)+'%</div>':'';
    return '<div class="stat-card"><div class="stat-label" style="color:'+c.color+'">'+c.label+'</div><div class="stat-value" style="color:'+c.color+'">'+c.value+'</div><div class="stat-sub">'+c.sub+'</div>'+t+'</div>';
  }).join('');
}

// ============================================================
// ⑦ RENDER CHARTS
// ============================================================
const CHART_COLORS=['#3b82f6','#22c55e','#ef4444','#f97316','#eab308','#a855f7','#06b6d4'];

function getChartOpts(isRate, yMin, yMax) {
  var yScale;
  if(isRate && yMin != null && yMax != null) {
    // Auto-scale with 5% padding
    var pad = Math.max((yMax-yMin)*0.05, 0.01);
    var lo = Math.max(0, yMin-pad), hi = Math.min(1, yMax+pad);
    yScale = {min:lo, max:hi, ticks:{color:'#94a3b8',callback:function(v){return(v*100).toFixed(v>=0.1?1:0)+'%';}}, grid:{color:'rgba(226,232,240,0.5)'}};
  } else {
    yScale = {min:0, max:1, ticks:{color:'#94a3b8',callback:function(v){return(v*100).toFixed(0)+'%';}}, grid:{color:'rgba(226,232,240,0.5)'}};
  }
  return {responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},
    plugins:{legend:{labels:{color:'#64748b',usePointStyle:true,font:{size:11},padding:16}},tooltip:{backgroundColor:'rgba(255,255,255,0.96)',titleColor:'#1e293b',bodyColor:'#475569',borderColor:'#e2e8f0',borderWidth:1,padding:10,cornerRadius:8,callbacks:{label:function(ctx){return ctx.dataset.label+': '+(ctx.parsed.y*100).toFixed(2)+'%';}}}},
    scales:{x:{ticks:{color:'#94a3b8',maxRotation:45,autoSkip:true,maxTicksLimit:12},grid:{display:false}},y:yScale}
  };
}

function destroyCharts() {[chartTrend,chartExtra1,chartExtra2].forEach(c=>{try{c&&c.destroy();}catch(e){}});chartTrend=chartExtra1=chartExtra2=null;}

// Helper: find latest row with at least one non-zero metric value
function findLatestNonZeroRow(data, keys) {
  if (!data || !data.length) return null;
  for (var di = data.length - 1; di >= 0; di--) {
    var r = data[di];
    for (var ki = 0; ki < keys.length; ki++) {
      var v = r[keys[ki]];
      if (v != null && typeof v === 'number' && v !== 0) return r;
    }
  }
  return data[data.length - 1]; // fallback
}

function renderCharts(q) {
  if(!filteredData||!filteredData.length) { destroyCharts(); return; }
  var keys=q.metricKeys, lMap=q.metricLabels||{}, cMap=q.metricColors||{};
  var isRate=keys.some(k=>k.includes('rate')||k.includes('accuracy')), hasPP=q.hasPrePost;
  destroyCharts();

  // --- Downsample: if > 30 points, use max 30 with even spacing + always include last ---
  var plotData = filteredData, labels;
  if(filteredData.length > 30) {
    var step = filteredData.length / 30;
    plotData = [];
    for(var si=0; si<30; si++){
      var idx = Math.floor(si * step);
      if(si===29) idx=filteredData.length-1; // always include last
      plotData.push(filteredData[idx]);
    }
  }
  labels = plotData.map(d=>d.date.slice(5));

  // --- Compute Y axis range for auto-scaling ---
  var yMin=null, yMax=null;
  if(isRate){
    var allVals=[];
    plotData.forEach(function(r){keys.forEach(function(k){if(r[k]!=null&&!isNaN(r[k])) allVals.push(r[k]);});});
    if(allVals.length>0){yMin=Math.min.apply(null,allVals);yMax=Math.max.apply(null,allVals);}
  }

  // Main trend chart
  var ctxT=document.getElementById('chartTrend').getContext('2d');
  document.getElementById('mainDot').style.background=q.color;
  document.getElementById('chartMainTitle').textContent=q.name+' — 指标走势';
  var datasets;
  if(hasPP) {
    datasets=[...['pre_violation_rate','pre_miss_rate','pre_accuracy'].filter(k=>keys.includes(k)).map(k=>({label:'申诉前·'+(lMap[k]||k),data:plotData.map(r=>r[k]),borderColor:'#ef4444',backgroundColor:'transparent',tension:0.3,pointRadius:3,borderDash:[5,3]})),
      ...['post_violation_rate','post_miss_rate','post_accuracy'].filter(k=>keys.includes(k)).map(k=>({label:'申诉后·'+(lMap[k]||k),data:plotData.map(r=>r[k]),borderColor:'#22c55e',backgroundColor:'transparent',tension:0.3,pointRadius:3}))];
  } else {
    datasets=keys.filter(k=>plotData.some(r=>r[k]!=null)).map((k,i)=>({label:lMap[k]||k,data:plotData.map(r=>r[k]),borderColor:cMap[k]||CHART_COLORS[i%CHART_COLORS.length],backgroundColor:(cMap[k]||CHART_COLORS[i%CHART_COLORS.length])+'15',fill:i<=1,tension:0.3,pointRadius:plotData.length<=15?4:1,pointHoverRadius:6}));
  }
  chartTrend=new Chart(ctxT,{type:'line',data:{labels,datasets},options:getChartOpts(isRate,yMin,yMax)});

  // Extra 1 - bar or doughnut (use latest non-zero row)
  var ctxE1=document.getElementById('chartExtra1').getContext('2d');
  document.getElementById('extraTitle1').textContent=hasPP?'申诉效果对比':'最新一日指标构成';
  // Use latest non-zero row instead of raw last row
  var lrNZ = findLatestNonZeroRow(filteredData, keys);
  if(hasPP) {
    var lr=lrNZ||{};
    chartExtra1=new Chart(ctxE1,{type:'bar',data:{labels:['违规准确率','漏率','准确率'],datasets:[
      {label:'申诉前',data:[lr.pre_violation_rate||0,lr.pre_miss_rate||0,lr.pre_accuracy||0],backgroundColor:'rgba(239,68,68,0.6)',borderRadius:4},
      {label:'申诉后',data:[lr.post_violation_rate||0,lr.post_miss_rate||0,lr.post_accuracy||0],backgroundColor:'rgba(34,197,94,0.6)',borderRadius:4}]},
      options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#64748b',usePointStyle:true,font:{size:11}}}},scales:{x:{ticks:{color:'#64748b'},grid:{display:false}},y:{ticks:{color:'#64748b'},grid:{color:'rgba(226,232,240,0.5)'}}}}});
  } else {
    var lr2=lrNZ||{};
    var dk=keys.filter(k=>lr2[k]!=null);
    chartExtra1=new Chart(ctxE1,{type:'doughnut',data:{labels:dk.map(k=>lMap[k]||k),datasets:[{data:dk.map(k=>lr2[k]),backgroundColor:dk.map((k,i)=>cMap[k]||CHART_COLORS[i%CHART_COLORS.length]),borderWidth:2,borderColor:'#fff'}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'bottom',labels:{color:'#64748b',usePointStyle:true,font:{size:11},padding:12}}}}});
  }

  // Extra 2 - radar
  var ctxE2 = document.getElementById('chartExtra2').getContext('2d');
  document.getElementById('extraTitle2').textContent = '指标雷达图';
  if(keys.length >= 2) {
    var avgs = keys.map(function(k){
      var v = filteredData.map(function(r){return r[k];}).filter(function(x){return x!=null && !isNaN(x);});
      return v.length ? v.reduce(function(a,b){return a+b;})/v.length : 0;
    });
    chartExtra2 = new Chart(ctxE2, {
      type: 'radar',
      data: {
        labels: keys.map(function(k){return lMap[k]||k;}),
        datasets: [{
          label: '均值',
          data: avgs,
          backgroundColor: 'rgba(59,130,246,0.15)',
          borderColor: '#3b82f6',
          pointBackgroundColor: '#3b82f6'
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          r: {
            angleLines: {color: 'rgba(148,163,184,0.2)'},
            grid: {color: 'rgba(148,163,184,0.2)'},
            pointLabels: {color: '#64748b', font: {size: 11}},
            ticks: {display: false}
          }
        },
        plugins: {legend: {display: false}}
      }
    });
  } else {
    try { document.getElementById('chartCardExtra2').style.display='none'; } catch(e){}
  }
}

// ============================================================
// ⑧ TABLE
// ============================================================
function renderTable(q) {
  if(!filteredData||!filteredData.length) {
    document.getElementById('tableHead').innerHTML='';
    document.getElementById('tableBody').innerHTML='<tr><td colspan="20">暂无数据</td></tr>';
    document.getElementById('recordCount').textContent='0 条记录';return;
  }
  var keys=q.metricKeys, labels=q.metricLabels||{};
  var th='<th onclick="sortTable(\'date\')">日期</th>';
  keys.forEach(k=>th+='<th onclick="sortTable(\''+k+'\')">'+(labels[k]||k)+'</th>');
  document.getElementById('tableHead').innerHTML=th;

  var isRateField = keys.some(k=>k.includes('rate')||k.includes('accuracy'));
  var rows=filteredData.slice().reverse().map(r=>{
    var td='<td>'+r.date+'</td>';
    keys.forEach(k=>{
      var v=r[k], cls='', txt='--';
      if(v!=null){
        if(typeof v==='number'){
          txt=(v*100).toFixed(2)+'%';
          if(v<0.9)cls=' val-bad'; else if(v>=0.98)cls=' val-good';
        } else { txt=v; }
      }
      td+='<td class="'+cls+'">'+txt+'</td>';
    });
    return '<tr>'+td+'</tr>';
  });
  document.getElementById('tableBody').innerHTML=rows.join('');
  document.getElementById('recordCount').textContent=filteredData.length+' 条记录';
}

function sortTable(col) {
  if(sortCol===col) sortDir=sortDir==='desc'?'asc':'desc';
  else {sortCol=col;sortDir='desc';}
  filteredData.sort((a,b)=>{var va=a[col],vb=b[col];if(va==null)return 1;if(vb==null)return -1;if(typeof va==='string')return sortDir==='desc'?vb.localeCompare(va):va.localeCompare(vb);return sortDir==='desc'?vb-va:va-vb;});
  renderAll(QUEUES.find(x=>x.id===activeQueueId));
}

// ============================================================
// ⑨ EXPORT
// ============================================================
function exportCSV() {
  if(!filteredData || !filteredData.length) { alert('暂无数据可导出'); return; }
  var q = QUEUES.find(x=>x.id===activeQueueId);
  var keys=q.metricKeys, labels=q.metricLabels||{};
  var head=['日期'].concat(keys.map(k=>labels[k]||k));
  var rows=filteredData.slice().reverse().map(r=>{
    return [r.date].concat(keys.map(k=>{
      var v=r[k];
      return (v!=null&&typeof v==='number')?(v*100).toFixed(2)+'%':(v!=null?v:'--');
    }));
  });
  var csv=[head.join(',')].concat(rows.map(r=>r.map(function(c){return '"'+String(c).replace(/"/g,'""')+'"';}).join(','))).join('\n');
  var BOM='\uFEFF';
  var blob=new Blob([BOM+csv],{type:'text/csv;charset=utf-8'});
  var a=document.createElement('a'); a.href=URL.createObjectURL(blob);
  a.download=(q?q.name:'data')+'_质检数据_'+new Date().toISOString().slice(0,10)+'.csv';
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  URL.revokeObjectURL(a.href);
}

// ============================================================
// ⑩ ALL
// ============================================================
function renderAll(q) {
  try{renderStats(q);}catch(e){console.error('STATS:',e);}
  try{renderCharts(q);}catch(e){console.error('CHARTS:',e);}
  try{renderTable(q);}catch(e){console.error('TABLE:',e);}
}
