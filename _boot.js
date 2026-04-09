try {
  hydrateQueuesFromDB();
  initUI();
  // Default to last 30 days for cleaner charts
  try { setPeriod('day'); } catch(e){/* fallback: show all */ }
  var q=QUEUES.find(x=>x.id===activeQueueId); var d=(q&&q.data)||[];
  if(d.length>30){
    var dates=d.map(function(r){return r.date;}).sort(); var last=dates[dates.length-1];
    var dt=new Date(last); dt.setDate(dt.getDate()-29);
    document.getElementById('dateFrom').value=dt.toISOString().split('T')[0];
    document.getElementById('dateTo').value=last;
  }
  applyFilter();
  console.log('OK loaded:', QUEUES.filter(q=>q.dataStatus==='loaded').length+'/'+QUEUES.length);
} catch(e) {
  document.body.innerHTML='<div style="padding:40px;color:#dc2626;font-family:monospace;font-size:14px;white-space:pre-wrap;background:#fef2f2;margin:20px;border-radius:12px">ERROR: '+e.message+'<br><pre>'+(e.stack||'')+'</pre></div>';
}
