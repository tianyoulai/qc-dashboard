"""Generate clean light-theme dashboard from SQLite data."""
import json, sys
from datetime import datetime

sys.path.insert(0, 'src')
from db_helper import get_db

DB_PATH = 'data/metrics.db'
OUT_PATH = 'dashboard/dashboard_light.html'

conn = get_db(DB_PATH)
cur = conn.cursor()
queue_ids = ['q1_toufang','q2_erjiansimple','q3_erjian_4qi','q4_jubao_4qi','q5_lahei','q6_shangqiang']
all_data = {}
for qid in queue_ids:
    cur.execute('SELECT date, raw_data FROM daily_metrics WHERE queue_id=? ORDER BY date', (qid,))
    rows = cur.fetchall()
    by_date = {}
    for date, raw in rows:
        if date in by_date: continue
        try: rd = json.loads(raw) if raw else {}
        except: rd = {}
        row_data = {'date': date}
        for k,v in rd.items(): row_data[k] = v
        by_date[date] = row_data
    all_data[qid] = sorted(by_date.values(), key=lambda x: x['date'])
conn.close()

total = sum(len(v) for v in all_data.values())
db_json = json.dumps(all_data, ensure_ascii=False)
ts = datetime.now().strftime('%m-%d %H:%M')

html_template = r'''<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>质检数据看板</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
:root{--bg:#f5f7fa;--card:#fff;--card-h:#f0f3f7;--bd:#e0e4e8;--tx:#1a202c;--dim:#718096;--ac:#3182ce;
--sh:0 1px 3px rgba(0,0,0,.06);--sh2:0 4px 12px rgba(0,0,0,.1)}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,'PingFang SC',sans-serif;background:var(--bg);color:var(--tx);min-height:100vh;padding:16px}
.hd{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:14px;margin-bottom:20px;padding-bottom:14px;border-bottom:1px solid var(--bd)}
.hd h1{font-size:22px;font-weight:700}.hd .sub{color:var(--dim);font-size:12px}
.hd-r{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
select,input,button{background:var(--card);border:1px solid var(--bd);color:var(--tx);padding:7px 12px;border-radius:8px;font-size:12px;cursor:pointer}
.pt{display:flex;background:var(--card);border-radius:8px;overflow:hidden;border:1px solid var(--bd)}
.pt button{border:none;border-radius:0;padding:7px 16px;font-size:12px;color:var(--dim)}.pt .on{background:var(--ac);color:#fff}
.tabs{display:flex;gap:6px;margin-bottom:18px;overflow-x:auto}
.tab{padding:9px 18px;border-radius:10px;font-size:13px;font-weight:600;cursor:pointer;white-space:nowrap;background:var(--card);border:1px solid var(--bd);color:var(--dim);display:flex;align-items:center;gap:6px;transition:.25s;box-shadow:var(--sh)}
.tab:hover{transform:translateY(-1px);box-shadow:var(--sh2)} .tab.on{color:#fff;box-shadow:var(--sh2)}
.dot{width:8px;height:8px;border-radius:50%} .bge{font-size:10px;padding:1px 6px;border-radius:8px;opacity:.7}
.sg{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:14px;margin-bottom:20px}
.sc{background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:16px 18px;box-shadow:var(--sh)}
.sc:hover{transform:translateY(-2px);box-shadow:var(--sh2)}
.sl{font-size:11px;color:var(--dim);margin-bottom:6px} .sv{font-size:26px;font-weight:700} .ss{font-size:11px;color:var(--dim);margin-top:5px}
.cg{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:20px}
.cc{background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:18px;box-shadow:var(--sh)}
.cfw{grid-column:1/-1} .ct{font-size:14px;font-weight:600;margin-bottom:14px;display:flex;align-items:center;gap:7px}
.ch{position:relative;height:260px} .ch.t{height:320px}
.tc{background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:18px;margin-bottom:20px;box-shadow:var(--sh)}
table{width:100%;border-collapse:collapse;font-size:12px}
th{background:rgba(49,130,206,.06);color:var(--ac);font-weight:600;padding:9px 10px;text-align:right;position:sticky;top:0;cursor:pointer;font-size:11px}
td{padding:8px 10px;text-align:right;border-top:1px solid var(--bd)} td:first-child{text-align:left;font-weight:500;color:#2b6cb0}tr:hover{background:var(--card-h)}
.vb{color:#e53e3e;font-weight:600} .vg{color:#38a169;font-weight:600} .vw{color:#d69e2e;font-weight:600}
.ft{text-align:center;color:var(--dim);font-size:11px;padding:14px 0;border-top:1px solid var(--bd)}
.orow{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:18px}@media(max-width:900px){.orow{grid-template-columns:repeat(3,1fr)}.cg{grid-template-columns:1fr}}
.oc{background:var(--card);border:1px solid var(--bd);border-radius:10px;padding:14px;text-align:center;cursor:pointer;box-shadow:var(.sh)}
.oc:hover{transform:translateY(-2px);box-shadow:var(--sh2)}
.bg{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600}
.bbl{background:rgba(49,130,206,.15);color:#3182ce} .bam{background:rgba(214,158,46,.15);color:#d69e2e}
.es{grid-column:1/-1;text-align:center;padding:60px 20px;color:var(--dim);font-size:14px}
</style></head>
<body>
<div class="hd">
<div><h1>质检数据统一看板</h1><div class="sub">6队列 · ''' + str(total) + r'''天 · <span id="lu"></span></div></div>
<div class="hd-r"><input type=date id=df> ~ <input type=date id=dt>
<div class="pt"><button onclick="sp('day')" class="on" id=bd>日</button><button onclick="sp('week')" id=bw>周</button><button onclick="sp('month')" id=bm>月</button></div>
<button onclick="reset()">重置</button></div></div>
<div class="orow" id=or></div>
<div class="tabs" id=tabs></div>
<div class="sg" id=sg></div>
<div class="cg">
<div class="cc cfw"><div class="ct"><span id=md style=display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--ac)></span> <span id=cmt>趋势</span></div><div class="ch t"><canvas id=ct></canvas></div></div>
<div class="cc"><div class="ct">📊 指标构成</div><div class="ch"><canvas id=c1></canvas></div></div>
<div class="cc"><div class="ct">📈 周均对比</div><div class="ch"><canvas id=c2></canvas></div></div></div>
<div class="tc"><div class="ct">明细 <span class="bg bbl" id=rc>--</span></div><div style=overflow-x:auto><table><thead><tr id=th></tr></thead><tbody id=tb></tbody></table></div></div>
<div class="ft">质检看板 · 6队列 · ''' + ts + r'''</div>

<script>
var Q=[
{id:'q1',n:'投放误漏',i:'📢',c:'#3182ce',mk:['violation_rate','miss_rate'],ml:{'violation_rate':'违规准确率','miss_rate':'漏率'},ds:'pending',d:[]},
{id:'q2',n:'简单二审',i:'📋',c:'#38a169',mk:['violation_rate','miss_rate'],ml:{'violation_rate':'违规准确率','miss_rate':'漏率'},ds:'pending',d:[]},
{id:'q3',n:'四期-二审',i:'🔄',c:'#dd6b20',mk:['violation_rate','miss_rate'],ml:{'violation_rate':'违规准确率','miss_rate':'漏率'},ds:'pending',d:[]},
{id:'q4',n:'四期-举报',i:'🚨',c:'#805ad5',pp:true,mk:['pre_violation_rate','pre_miss_rate','pre_accuracy','post_violation_rate','post_miss_rate','post_accuracy'],
ml:{'pre_violation_rate':'申前·违规准','pre_miss_rate':'申前·漏率','pre_accuracy':'申前·准确率',
'post_violation_rate':'申后·违规准','post_miss_rate':'申后·漏率','post_accuracy':'申后·准确率'},ds:'pending',d:[]},
{id:'q5',n:'拉黑误漏',i:'🚫',c:'#e53e3e',mk:['violation_rate','miss_rate'],ml:{'violation_rate':'违规准确率','miss_rate':'漏率'},ds:'pending',d:[]},
{id:'q6',n:'上墙文本',i:'📝',c:'#319795',mk:['audit_accuracy'],ml:{'audit_accuracy':'审核准确度'},ds:'pending',d:[]}
];
var DD=''' + db_json + r''';
var aQ='q6',fd=[],sc=null,sd=null,cc=null;

function hydrate(){if(!DD)return;Q.forEach(function(q){var r=DD[q.id];if(!r||!r.length)return;var bd={};r.sort(function(a,b){return(a.date||'').localeCompare(b.date||'')});
r.forEach(function(x){var d=x.date;if(!bd[d])bd[d]={date:d};
Object.keys(x).forEach(function(k){if(k!=='_queueName'&&k!=='date'&&x[k]!=null)bd[d][k]=x[k];})});
q.d=Object.values(bd).sort(function(a,b){return a.date.localeCompare(b.date)});
q.ds=q.d.length?'loaded':'pending';});}
hydrate();

function init(){$('#tabs').html(Q.map(function(q){return'<div class="tab '+(q.id===aQ?'on':'')+'" onclick="sw(\''+q.id+'\')" style="'+(q.id===aQ?'background:'+c:'')+'"><span class="dot" style="background:'+q.c+'"></span>'+q.i+' '+q.n+'<span class="bge '+(q.ds==='loaded'?'bbl':'bam')+'">'+(q.ds==='loaded'?q.d.length+'天':'待接入')+'</span></div>').join(''));
$('#or').html(Q.map(function(q){var d=q.d||[],l=d.length,v=l?d[l-1]:null,mk=q.mk[0],vs=v&&v[mk]!=null?v[mk].toFixed(1)+'%':'--';
return'<div class="oc" onclick="sw(\''+q.id+'\')"><div style="font-size:24px">'+q.i+'</div><div>'+q.n+'</div><div style="font-size:20px;font-weight:700;color:'+q.c+'">'+vs+'</div><div style="color:var(--dim)">'+(l?d[l-1].date:'')+'</div></div>';}).join(''));
var ds=Q.flatMap(function(q){return(q.d||[]).map(function(r){return r.date})}).filter(Boolean).sort();
if(ds.length)$('#df').val(ds[0]),$('#dt').val(ds[ds.length-1]);
$('#lu').text(new Date().toLocaleString('zh-CN'));}
function sw(id){aQ=id;init();af();}
function af(){var q=Q.find(function(x){return x.id===aQ});
if(!q||!q.d||!q.d.length){fd=[];ra(q);return;}
var f=$('#df').val(),t=$('#dt').val();
fd=q.d.filter(function(r){return(!f||r.date>=f)&&(!t||r.date<=t)});
ra(q);}
function sp(p){$('.pt button').removeClass('on');$('#b'+p).addClass('on');
var q=Q.find(function(x){return x.id===aQ}),d=q?q.d:[];
if(!d.length)return;var ds=d.map(function(r){return r.date}).sort(),last=ds[ds.length-1],st;
if(p==='day')st=ds[0];else if(p==='week'){var dt=new Date(last);dt.setDate(dt.getDate()-6);st=dt.toISOString().split('T')[0]}
else{var dt=new Date(last);dt.setMonth(dt.getMonth()-1);dt.setDate(dt.getDate()+1);st=dt.toISOString().split('T')[0]'}
$('#df').val(st);$('#dt').val(last);af();}
function reset(){sp('day');}

function rs(q){if(!fd||!fd.length){$('#sg').html('<div class="es">该队列暂无数据</div>');return;}
var keys=q.mk,ml=q.ml||{},cs=['#3182ce','#38a169','#e53e3e','#dd6b20','#d69e2e','#805ad5','#319795'];
var h='<div class="sc"><div class="sl" style="color:'+q.c+'">天数</div><div class="sv">'+fd.length+'</div><div class="ss">'+(fd[0]?fd[0].date:'')+'~'+(fd[fd.length-1]?fd[fd.length-1].date:'')+'</div></div>';
keys.forEach(function(k,i){
var vals=fd.map(function(r){return r[k]}).filter(function(v){return v!=null&&!isNaN(v)});
if(!vals.length)return;var s=vals.reduce(function(a,b){return a+b},0),avg=(s/vals.length).toFixed(2),last=vals[vals.length-1],prev=vals.length>1?vals[vals.length-2]:last,ch=prev!==0?(((last-prev)/Math.abs(prev))*100).toFixed(1):0,c=cs[i%cs.length],cl=ch>0?'↑':ch<0?'↓':'→';
h+='<div class="sc"><div class="sl" style="color:'+c+'">'+(ml[k]||k)+'</div><div class="sv" style="color:'+c+'">'+(typeof last==='number'?last.toFixed(2):last)+'</div><div class="ss">均值 '+avg+' | 累计 '+Math.round(s)+'</div><div style="font-size:11px;color:'+(ch>0?'#e53e3e':ch<0?'#38a169':'var(--dim)')+'">'+cl+' '+Math.abs(ch)+'%</div></div>';
});
$('#sg').html(h);}

function dc(){try{sc&&sc.destroy();}catch(e){}try{sd&&sd.destroy();}catch(e){}try{cc&&cc.destroy();}catch(e){}sc=null;sd=null;cc=null;}
var CC=['#3182ce','#38a169','#e53e3e','#dd6b20','#d69e2e','#805ad5','#319795'];
function rc(q){if(!fd||!fd.length){dc();return;}
var lbls=fd.map(function(d){return d.date.slice(5)}),keys=q.mk,ml=q.ml||{},isR=keys.some(function(k){return k.includes('rate')}),hp=q.pp;dc();
var ctx=document.getElementById('ct').getContext('2d');document.getElementById('md').style.background=q.c;document.getElementById('cmt').textContent=q.n+' — 走势';
var ds;if(hp){
ds=[].concat(keys.filter(function(k){return k.startsWith('pre')}).map(function(k){return{label:(ml[k]||k),data:fd.map(function(r){return r[k]}),borderColor:'#e53e3e',backgroundColor:'transparent',tension:.3,pointRadius:3,borderDash:[5,3]}}),
keys.filter(function(k){return k.startsWith('post')}).map(function(k){return{label:(ml[k]||k),data:fd.map(function(r){return r[k]}),borderColor:'#38a169',backgroundColor:'transparent',tension:.3,pointRadius:3}}));
}else{
ds=keys.filter(function(k){return fd.some(function(r){return r[k]!=null})}).map(function(k,i){return{label:(ml[k]||k),data:fd.map(function(r){return r[k]}),borderColor:CC[i%CC.length],backgroundColor:CC[i%CC.length]+'18',fill:i<=1,tension:.3,pointRadius:keys.length<=4?4:2}});
}
sc=new Chart(ctx,{type:'line',data:{labels:lbls,datasets:ds},options:gco(isR,hp)});

var ctx1=document.getElementById('c1').getContext('2d'),lst=fd[fd.length-1]||{},dk=keys.filter(function(k){return lst[k]!=null});
sd=new Chart(ctx1,{type:'bar',data:{labels:dk.map(function(k){return ml[k]||k}),datasets:[{data:dk.map(function(k){return lst[k]}),backgroundColor:dk.map(function(k,i){return CC[i%CC.length]+'90'}),borderRadius:6}]},
options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:'var(--dim)',font:{size:10}},grid:{color:'rgba(224,228,232,.5)'}},y:{ticks:{color:'var(--dim)',font:{size:10}},grid:{display:false}}}}});

var ctx2=document.getElementById('c2').getContext('2d'),wg={};
fd.forEach(function(r){var d=new Date(r.date),ws=new Date(d);ws.setDate(d.getDate()-d.getDay());var k=ws.toISOString().split('T')[0];if(!wg[k])wg[k]={sums:{},n:0};keys.forEach(function(m){wg[k].sums[m]=(wg[k].sums[m]||0)+(r[m]||0);});wg[k].n++;});
var wks=Object.values(wg);
cc=new Chart(ctx2,{type:'radar',data:{labels:keys.map(function(k){return ml[k]||k}),datasets:wks.slice(-4).map(function(w,i){return{label:'W'+i,data:keys.map(function(k){return w.n?((w.sums[k]||0)/w.n).toFixed(2):0}),borderColor:CC[i%CC.length],backgroundColor:CC[i%CC.length]+'12',pointRadius:3}})},
options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'var(--dim)',usePointStyle:true,font:{size:10},padding:8}}},
scales:{r:{angleLines:{color:'rgba(224,228,232,.5)'},{grid:{color:'rgba(224,228,232,.5)'},{pointLabels:{color:'var(--dim)',font:{size:10}}},{ticks:{display:false,backdropColor:'var(--bg)'}}}}}}});}

function gco(ir,hp){var dy=!ir&&!hp;return{responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},
plugins:{legend:{labels:{color:'var(--dim)',usePointStyle:true,padding:14,font:{size:11}}},
tooltip:{backgroundColor:'#fff',titleColor:'var(--tx)',bodyColor:'var(--dim)',borderColor:'var(--bd)',borderWidth:1,padding:10,callbacks:{label:function(c){return ' '+c.dataset.label+': '+c.parsed.y}}}},
scales:{x:{ticks:{color:'var(--dim)',font:{size:10}},grid:{color:'rgba(224,228,232,.5)'}},
...(dy?{
y:{type:'linear',position:'left',ticks:{color:'#3182ce',font:{size:10}},{grid:{color:'rgba(224,228,232,.5)'},{title:{display:true,text:'绝对值',color:'var(--dim)',font:{size:10}}}},
y1:{type:'linear',position:'right',ticks:{color:'var(--dim)',font:{size:10}},{grid:{drawOnChartArea:false},{title:{display:true,text:'比率',color:'var(--dim)',font:{size:10}}}}}}
:{y:{ticks:{color:'var(--dim)',font:{size:10}},{grid:{color:'rgba(224,228,232,.5)'}}}})}}};}

function rt(q){var keys=q.mk||[],ml=q.ml||{};
if(!keys.length){$('#th').html('<th>日期</th>'),$('#tb').html('<tr><td colspan=99 style=text-align:center;padding:30px>无数据</td></tr>');return;}
$('#th').html('<th>日期</th>'+keys.map(function(k){return'<th style=cursor:pointer onclick="sb(\''+k+'\')">'+(ml[k]||k)+'</th>'}).join(''));
var sr=[].concat(fd).sort(function(a,b){var va=a[sc],vb=b[sc];if(va==null)va=0;if(vb==null)vb=0;if(typeof va==='string')return sdr==='asc'?va.localeCompare(vb):vb.localeCompare(va);return sdr==='asc'?va-vb:vb-va;});
$('#tb').html(sr.map(function(r){return'<tr><td>'+r.date+'</td>'+keys.map(function(k){var v=r[k];if(v==null)v='-';var cls=(typeof v==='number'&&(v>=5?'vb':v<=1?'vg':''));return '<td class="'+cls+'">'+(typeof v==='number'&&k.includes('rate')?v.toFixed(2):v)+'</td>'}).join('')+'</tr>'}).join(''));
$('#rc').text(sr.length+' 条');}
var sbk=null,sdr='desc';
function sb(col){if(sbk===col)sdr=sdr==='asc'?'desc':'asc';else{sbk=col;sdr='desc';}rt(Q.find(function(q){return q.id===aQ}));}

function ra(q){rs(q);rc(q);rt(q);}
init();af();
</script></body></html>'''

with open(OUT_PATH, 'w', encoding='utf-8') as f:
    f.write(html_template)
print(f"OK: {OUT_PATH} ({total} records)")
