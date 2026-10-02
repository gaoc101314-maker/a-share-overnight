"""PM03盲生成Reality供给：保留认证来源，不排序选股、不调用AI。"""
import json
import math
from datetime import datetime
from pm01 import get, symbol, fanout, eligible, now_cn, CN

UNKNOWN = 'UNKNOWN'
FIELDS = ['股票','代码','SOURCE_3D_RANK','SOURCE_5D_RANK','SOURCE_10D_RANK',
          'SECOND_BOARD','LEADER_POOL','CURRENT_PRICE','CURRENT_RETURN','DAY_HIGH',
          'DAY_HIGH_RETURN','DAY_LOW','DAY_LOW_RETURN','PRICE_1400','RETURN_1400',
          'PRICE_1430','RETURN_1430','CURRENT_TO_HIGH','TURNOVER','AMOUNT',
          'ACCOUNT_ELIGIBLE','ACCOUNT_INELIGIBLE_REASON']
PATH_FIELDS = ['DAY_HIGH','DAY_HIGH_RETURN','DAY_LOW','DAY_LOW_RETURN','PRICE_1400',
               'RETURN_1400','PRICE_1430','RETURN_1430','CURRENT_TO_HIGH','TURNOVER','AMOUNT']


def finite(value, positive=False):
    try:
        n=float(value)
        return n if math.isfinite(n) and (not positive or n>0) else UNKNOWN
    except (ValueError,TypeError):
        return UNKNOWN


def percent(price, base):
    a,b=finite(price,True),finite(base,True)
    return (a/b-1)*100 if a!=UNKNOWN and b!=UNKNOWN else UNKNOWN


def distance(price,high):
    p=percent(price,high)
    return p/100 if p!=UNKNOWN else UNKNOWN


def merge(s):
    """按代码稳定排序只为展示；不使用账户资格删减或重排认证池。"""
    combined={}
    # 已完整生成的50行产品，其名单外身份可以确认；底层全市场历史认证缺口另行报告。
    output_known={n:s['pool_status'][n]=='PASS' or (
        len(s['top'][n])==50 and len({r['code'] for r in s['top'][n]})==50 and
        {r['rank'] for r in s['top'][n]}==set(range(1,51))) for n in ('3','5','10')}
    def add(source, rank=None):
        c=source['code']
        if c not in combined:
            q=s['rows'].get(c,source)
            acc,reason=eligible(c,q['name'])
            combined[c]={**{f:UNKNOWN for f in FIELDS},'股票':q['name'],'代码':c,
                'SECOND_BOARD':'NO' if s['second_status']=='PASS' else UNKNOWN,
                'LEADER_POOL':'NO' if s['leader_status']=='PASS' else UNKNOWN,
                'CURRENT_PRICE':q['price'],'CURRENT_RETURN':q['day_pct'],
                'ACCOUNT_ELIGIBLE':acc,'ACCOUNT_INELIGIBLE_REASON':reason,
                'DATA_SOURCE':q.get('quote_source',UNKNOWN),'SOURCE_TIMESTAMP':q['source_timestamp'],
                'SNAPSHOT_ID':s['bundle_id'],'SOURCE_TRACE':{'CURRENT':{'source':q.get('quote_source',UNKNOWN),'timestamp':q['source_timestamp']}},
                'PATH_ERRORS':[]}
            for n in ('3','5','10'):
                combined[c]['SOURCE_'+n+'D_RANK']='-' if output_known[n] else UNKNOWN
        if rank:
            combined[c]['SOURCE_'+rank+'D_RANK']=source['rank']
    for n in ('3','5','10'):
        for r in s['top'][n]:
            add(r,n)
    for key,label in [('second','SECOND_BOARD'),('leader','LEADER_POOL')]:
        for r in s[key]:
            add(r)
            combined[r['code']][label]='YES'
            if r.get('POOL_ASSET'):
                combined[r['code']]['SOURCE_TRACE'][label]={'source':'AUTO_POOL_ASSET',
                    'version_id':r['POOL_VERSION_ID'],'asof_date':r['POOL_ASOF_DATE'],
                    'asset':r['POOL_ASSET']}
    return [combined[c] for c in sorted(combined)]


def path_reality(q,d0):
    """只认分钟精确匹配，不插值，不用隔日分时，不用15:00价代替14:30。"""
    fields={f:UNKNOWN for f in PATH_FIELDS}
    errors=[]
    trace={}
    prev=finite(q.get('prev_close'),True)
    high,low=finite(q.get('day_high'),True),finite(q.get('day_low'),True)
    quote_source=q.get('quote_source',UNKNOWN)
    def quote_fields():
        fields.update(DAY_HIGH=high,DAY_HIGH_RETURN=percent(high,prev),DAY_LOW=low,
            DAY_LOW_RETURN=percent(low,prev),CURRENT_TO_HIGH=distance(q['price'],high),
            TURNOVER=finite(q.get('turnover')),AMOUNT=finite(q.get('amount')))
    quote_fields()
    if high==UNKNOWN or low==UNKNOWN:
        errors.append('DAY_EXTREMA_UNAVAILABLE'+(':ZERO_AMOUNT_NO_TRADED_EXTREMA' if finite(q.get('amount'))==0 else ''))
    trace['DAY_QUOTE']={'source':quote_source,'timestamp':q['source_timestamp'],'amount_unit':'CNY','return_unit':'PERCENT'}
    data=None
    source=None
    for endpoint in ('https://web.ifzq.gtimg.cn/appstock/app/minute/query',
                     'https://proxy.finance.qq.com/ifzqgtimg/appstock/app/minute/query'):
        try:
            candidate=get(endpoint,{'code':symbol(q['code'])}).json()['data'][symbol(q['code'])]
            date=candidate['data']['date']
            if str(date).replace('-','')!=d0.replace('-',''):
                raise ValueError('MINUTE_DATE_MISMATCH')
            data,source=candidate,endpoint
            break
        except Exception as e:
            errors.append(endpoint+':'+type(e).__name__)
    fetched=now_cn().isoformat()
    if data is None:
        return {'values':fields,'trace':trace,'errors':errors,'fetched_at':fetched}
    # 兼容旧冻结文件：补充字段只能来自与原报价完全相同的源时间和价格。
    a=data.get('qt',{}).get(symbol(q['code']),[])
    if len(a)>38:
        stamp=datetime.strptime(a[30],'%Y%m%d%H%M%S').replace(tzinfo=CN).isoformat()
        if stamp==q['source_timestamp'] and finite(a[3],True)==q['price']:
            prev=finite(a[4],True)
            high,low=finite(a[33],True),finite(a[34],True)
            fields.update(DAY_HIGH=high,DAY_HIGH_RETURN=percent(high,prev),DAY_LOW=low,
                DAY_LOW_RETURN=percent(low,prev),CURRENT_TO_HIGH=distance(q['price'],high),
                TURNOVER=finite(a[38]),AMOUNT=finite(a[37])*10000 if finite(a[37])!=UNKNOWN else UNKNOWN)
            trace['DAY_QUOTE']={'source':source+'#qt','timestamp':stamp,'amount_unit':'CNY','return_unit':'PERCENT'}
    bars={}
    for line in data['data'].get('data',[]):
        parts=line.split()
        if len(parts)>=2 and len(parts[0])==4:
            bars[parts[0]]=finite(parts[1],True)
    cutoff=q['source_timestamp'][11:16].replace(':','')
    for minute in ('1400','1430'):
        if minute>cutoff:
            errors.append('NOT_YET_OCCURRED:'+minute)
            continue
        price=bars.get(minute,UNKNOWN)
        fields['PRICE_'+minute]=price
        fields['RETURN_'+minute]=percent(price,prev)
        if price==UNKNOWN:
            errors.append('EXACT_MINUTE_MISSING:'+minute)
        trace[minute]={'source':source,'timestamp':d0+'T'+minute[:2]+':'+minute[2:]+':00+08:00','fetched_at':fetched,'price':price,'prev_close':prev,'minute_count':len(bars)}
    return {'values':fields,'trace':trace,'errors':errors,'fetched_at':fetched}


def build(s,paths):
    rows=merge(s)
    for r in rows:
        p=paths.get(r['代码'])
        if p:
            r.update(p['values'])
            r['SOURCE_TRACE'].update(p['trace'])
            r['PATH_ERRORS']=p['errors']
            r['PATH_FETCHED_AT']=p['fetched_at']
            sources={v['source'] for v in r['SOURCE_TRACE'].values()}
            r['DATA_SOURCE']=';'.join(sorted(sources))
        else:
            r['PATH_ERRORS']=['PATH_FETCH_FAILED']
    gaps=s.get('history_failures',{})
    required_path=['CURRENT_PRICE','CURRENT_RETURN']+PATH_FIELDS
    missing=[{'code':r['代码'],'fields':[f for f in required_path if r[f]==UNKNOWN]} for r in rows if any(r[f]==UNKNOWN for f in required_path)]
    status='PASS' if rows and not missing else ('PARTIAL' if rows else 'FAIL')
    identity_fields=['SOURCE_'+n+'D_RANK' for n in ('3','5','10')]+['SECOND_BOARD','LEADER_POOL']
    identity_missing=[{'code':r['代码'],'fields':[f for f in identity_fields if r[f]==UNKNOWN]} for r in rows if any(r[f]==UNKNOWN for f in identity_fields)]
    five_source='PASS' if rows and not identity_missing and all(s['pool_status'][n]=='PASS' for n in ('3','5','10')) and s['second_status']=='PASS' and s['leader_status']=='PASS' and s['same_snapshot']=='PASS' and s['input_freeze']=='PASS' else 'FAIL'
    unknown_detail=[{'code':r['代码'],'name':r['股票'],'fields':[f for f in FIELDS if r[f]==UNKNOWN]} for r in rows if any(r[f]==UNKNOWN for f in FIELDS)]
    return {'d0':s['d0'],'snapshot_time':s['snapshot_time'],'snapshot_id':s['bundle_id'],
        'data_source':s['data_source']+';PM03_PATH='+(';'.join(sorted({r['DATA_SOURCE'] for r in rows})) or 'NONE'),
        'research_mode':s['research_mode'],'rows':rows,'unique_count':len(rows),
        'account_count':sum(r['ACCOUNT_ELIGIBLE']=='YES' for r in rows),'path_status':status,
        'top_output':{n:f"{len(s['top'][n])}/50" for n in ('3','5','10')},
        'pool_status':s['pool_status'],'history_gap':len(gaps),'history_gap_detail':gaps,
        'path_gap_detail':missing,'second_status':s['second_status'],'leader_status':s['leader_status'],
        'second_count':len(s['second']),'leader_count':len(s['leader']),
        'five_source_status':five_source,'identity_gap_detail':identity_missing,
        'unknown_detail':unknown_detail,'unknown_count':sum(len(r['fields']) for r in unknown_detail),
        'unknown_stock_count':len(unknown_detail),
        'blind_ready':'YES' if status=='PASS' and five_source=='PASS' else 'NO'}


def export_text(p):
    lines=['PM03_BLIND_UNIVERSE',f"D0_DATE={p['d0']}",f"SNAPSHOT_TIME={p['snapshot_time']}",
        f"SNAPSHOT_ID={p['snapshot_id']}",f"DATA_SOURCE={p['data_source']}",
        f"RESEARCH_MODE={p['research_mode']}",f"UNIQUE_COUNT={p['unique_count']}",f"FIVE_SOURCE_STATUS={p['five_source_status']}",f"PATH_DATA_STATUS={p['path_status']}",
        f"SECOND_BOARD_POOL_COUNT={p['second_count']}",f"LEADER_POOL_COUNT={p['leader_count']}",
        f"UNKNOWN_FIELD_COUNT={p['unknown_count']}",f"UNKNOWN_STOCK_COUNT={p['unknown_stock_count']}",
        'STATE_SEMANTICS=-:confirmed outside Top50; YES:in pool; NO:confirmed outside pool; UNKNOWN:data not obtained',
        'TOP50_ABSENCE_SCOPE=Outside the emitted complete 50-row product; historical market certification is reported separately',
        'RETURN_UNIT=PERCENT','AMOUNT_UNIT=CNY','CURRENT_TO_HIGH_UNIT=DECIMAL_RATIO','CURRENT_TO_HIGH_FORMULA=CURRENT_PRICE / DAY_HIGH - 1',
        'MINUTE_RULE=Exact source minute 14:00/14:30; no interpolation; D0 date verified; full intraday chart not included',
        'UNIVERSE_ORDER=CODE_ASC; not a selection ranking',f"SECOND_BOARD_POOL_STATUS={p['second_status']}",f"LEADER_POOL_STATUS={p['leader_status']}"]
    for n in ('3','5','10'):
        lines += [f"{n}D_TOP50_OUTPUT={p['top_output'][n]}",f"{n}D_CERTIFICATION_STATUS={p['pool_status'][n]}"]
    lines += [f"HISTORY_DATA_GAP={p['history_gap']}",'HISTORY_GAP_DETAIL='+json.dumps(p['history_gap_detail'],ensure_ascii=False),
        'PATH_GAP_DETAIL='+json.dumps(p['path_gap_detail'],ensure_ascii=False),
        'UNKNOWN_DETAIL='+json.dumps(p['unknown_detail'],ensure_ascii=False),f"BLIND_TEST_READY={p['blind_ready']}"]
    def value(v):
        if isinstance(v,float): return f'{v:.6f}'
        return str(v).replace('\n',' ').replace('\r',' ').replace('｜','/')
    for i,r in enumerate(p['rows'],1):
        lines += [f'{i:02d}｜']+[f'{f}={value(r[f])}' for f in FIELDS]
        lines += [f"DATA_SOURCE={r['DATA_SOURCE']}",f"SOURCE_TIMESTAMP={r['SOURCE_TIMESTAMP']}",f"SNAPSHOT_ID={r['SNAPSHOT_ID']}",
            'SOURCE_TRACE='+json.dumps(r['SOURCE_TRACE'],ensure_ascii=False),'PATH_ERRORS='+json.dumps(r['PATH_ERRORS'],ensure_ascii=False)]
    return '\n'.join(lines+['END_PM03_BLIND_UNIVERSE'])


def render(st,s,progress=None):
    import pandas as pd
    import streamlit.components.v1 as components
    st.subheader('市场认证去重宇宙')
    st.caption('V0.1.2 · PM03五候选盲生成 Reality；网页只供给数据，不生成五候选。')
    identity=s['snapshot_id']
    cache=st.session_state.get('pm03_paths',{})
    if cache.get('snapshot_id')!=identity:
        cache={'snapshot_id':identity,'paths':{},'attempted':[]}
    rows=merge(s)
    needed=[s['rows'][r['代码']] for r in rows if r['代码'] not in cache['attempted']]
    if needed and s['d0']:
        with st.status('正在取得认证宇宙D0分时Reality…',expanded=True) as status:
            bar=st.progress(0)
            def update(done,total,elapsed):
                bar.progress(done/max(total,1),text=f'分时 {done}/{total} · {elapsed:.0f} 秒')
            result,failed=fanout(needed,lambda q:path_reality(q,s['d0']),update,budget=120,workers=6)
            cache['paths'].update(result)
            cache['attempted'] += [q['code'] for q in needed]
            status.update(label='分时Reality取得完成；缺失字段保留UNKNOWN',state='complete',expanded=False)
    st.session_state.pm03_paths=cache
    p=build(s,cache['paths'])
    st.session_state.pm03_product=p
    st.write('D0_DATE='+p['d0']+' · SNAPSHOT_TIME='+p['snapshot_time'])
    st.write(f"UNIQUE_COUNT={p['unique_count']} · ACCOUNT_ELIGIBLE_COUNT={p['account_count']} · PATH_DATA_STATUS={p['path_status']}")
    st.caption(' · '.join(n+'D_TOP50_OUTPUT='+p['top_output'][n] for n in ('3','5','10')))
    st.caption(f"HISTORY_DATA_GAP={p['history_gap']} · BLIND_TEST_READY={p['blind_ready']}")
    st.write(f"FIVE_SOURCE_STATUS={p['five_source_status']} · 二板={p['second_count']} · 龙头={p['leader_count']}")
    st.caption(f"UNKNOWN_FIELD_COUNT={p['unknown_count']} · UNKNOWN_STOCK_COUNT={p['unknown_stock_count']}")
    st.caption('排名“-”表示已确认不在Top50；YES/NO表示池身份；UNKNOWN仅表示未取得或未通过完整性核验。')
    if p['blind_ready']=='NO':
        st.warning('当前产品有未齐输入或数据缺口，可用于缺口评审，不标记为完整五源盲测就绪。')
    if rows:
        columns={'股票':'股票','SOURCE_3D_RANK':'3D排名','SOURCE_5D_RANK':'5D排名','SOURCE_10D_RANK':'10D排名',
            'SECOND_BOARD':'二板','LEADER_POOL':'龙头','CURRENT_RETURN':'当前涨幅%','RETURN_1400':'14:00涨幅%',
            'RETURN_1430':'14:30涨幅%','CURRENT_TO_HIGH':'距高点%','TURNOVER':'换手率%','ACCOUNT_ELIGIBLE':'账户资格'}
        table=pd.DataFrame([{label:(str(r[f]) if not isinstance(r[f],float) else f'{r[f]*100 if f=="CURRENT_TO_HIGH" else r[f]:.3f}') for f,label in columns.items()} for r in p['rows']])
        event=st.dataframe(table,hide_index=True,use_container_width=True,on_select='rerun',selection_mode='single-row',key='pm03_table_'+s['bundle_id'])
        selected=event.selection.rows
        chosen=st.selectbox('查看单只完整Reality（也可点击表格行）',range(len(rows)),format_func=lambda i:p['rows'][i]['代码']+' '+p['rows'][i]['股票'],key='pm03_detail_'+s['bundle_id'])
        index=selected[0] if selected else chosen
        with st.expander('完整Reality · '+p['rows'][index]['股票'],expanded=True):
            st.json(p['rows'][index])
    text=export_text(p)
    encoded=json.dumps(text,ensure_ascii=True).replace('<','\\u003c')
    components.html('''<button id="copy" style="width:100%;padding:14px;border-radius:10px;background:#172b45;color:white;border:0;font-size:16px">复制盲生成Reality</button><div id="msg" role="status"></div><textarea id="fallback" readonly style="display:none;width:100%;height:120px"></textarea><script>const payload='''+encoded+''';document.getElementById('copy').onclick=async()=>{try{await navigator.clipboard.writeText(payload);document.getElementById('msg').textContent='已复制完整PM03_BLIND_UNIVERSE';}catch(e){const t=document.getElementById('fallback');t.style.display='block';t.value=payload;t.select();document.getElementById('msg').textContent=document.execCommand('copy')?'已复制完整PM03_BLIND_UNIVERSE':'请长按文本全选复制';}};</script>''',height=200)
    st.download_button('下载盲生成Reality文本',text.encode('utf-8-sig'),file_name='PM03_BLIND_UNIVERSE_'+p['d0']+'.txt',mime='text/plain')
    with st.expander('盲生成复制全文与数据缺口'):
        st.code(text,language=None)
