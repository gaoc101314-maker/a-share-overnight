"""五源自动维护：确定性池资产，不调用AI，不连接券商。

正式版本只在数据完整且已有正式继承起点时发布；其余保存为候选版本。
历史涨停专题源不是全A股完整覆盖，不用9.8%阈值补造涨停记录。
"""
import copy
import hashlib
import json
import math
import threading
from datetime import datetime,timedelta
from pathlib import Path
import pandas as pd
import pm01

ROOT=Path(__file__).parent
DATA=ROOT/'five_source_versions'
CACHE=ROOT/'.auto_pool_cache'
WINDOW_DAYS=30
RAW_LOCK=threading.Lock()
RAW_CODE_LOCKS={}


def raw_closes(code,anchor):
    with RAW_LOCK:
        lock=RAW_CODE_LOCKS.setdefault((code,anchor),threading.Lock())
    with lock:
        path=CACHE/'raw_closes'/(code+'-'+anchor+'.json')
        try:
            return json.loads(path.read_text(encoding='utf8'))
        except (OSError,ValueError):
            pass
        errors=[]
        for endpoint in ('https://web.ifzq.gtimg.cn/appstock/app/fqkline/get','https://proxy.finance.qq.com/ifzqgtimg/appstock/app/fqkline/get'):
            try:
                d=pm01.get(endpoint,{'param':pm01.symbol(code)+',day,,'+anchor+',100,'}).json()['data'][pm01.symbol(code)]
                closes={r[0]:float(r[2]) for r in d.get('day',[]) if r[0]<=anchor and float(r[2])>0}
                if not closes:raise ValueError('DAILY_EMPTY')
                atomic_json(path,closes)
                return closes
            except Exception as e:
                errors.append(type(e).__name__)
        raise ValueError('RAW_CLOSE_UNAVAILABLE:'+','.join(errors))


def trading_dates(start,end):
    days=[]
    cursor=datetime.fromisoformat(start).date()
    stop=datetime.fromisoformat(end).date()
    while cursor<=stop:
        status=pm01.trading_day(cursor.isoformat())
        if status is None:
            raise ValueError('EXCHANGE_CALENDAR_YEAR_UNVERIFIED')
        if status:
            days.append(cursor.isoformat())
        cursor+=timedelta(days=1)
    return days


def atomic_json(path,value):
    path=Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix(path.suffix+'.tmp')
    temp.write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf8')
    temp.replace(path)


def limit_history(day,prices=None,anchor=None):
    """核验响应真实日期、总数；None不是无涨停，不能当成空池PASS。"""
    path=CACHE/'limit_up'/(day+'.json')
    try:
        cached=json.loads(path.read_text(encoding='utf8'))
        if cached['date']==day and cached['response_date']==day and cached['source'] in ('EASTMONEY_ZT_HISTORY','TENCENT_OBSERVED_CLOSE_LIMIT'):
            return {**cached,'cache':'EXACT_DATE_HISTORY_CACHE'}
    except (OSError,ValueError,KeyError):
        pass
    params={'ut':'7eea3edcaed734bea9cbfc24409ed989','dpt':'wz.ztzt','Pageindex':0,
            'pagesize':10000,'sort':'fbt:asc','date':day.replace('-','')}
    d=pm01.get('https://push2ex.eastmoney.com/getTopicZTPool',params).json()
    data=d.get('data')
    if d.get('rc')!=0 or not data:
        raise ValueError('LIMIT_UP_HISTORY_UNAVAILABLE')
    qdate=str(data.get('qdate'))
    if len(qdate)!=8 or qdate<day.replace('-',''):
        raise ValueError('LIMIT_UP_RESPONSE_MARKET_DATE_INVALID')
    rows=data.get('pool')
    if not isinstance(rows,list) or len(rows)!=data.get('tc') or len({r['c'] for r in rows})!=len(rows):
        raise ValueError('LIMIT_UP_HISTORY_INCOMPLETE')
    if not rows and qdate!=day.replace('-',''):
        raise ValueError('EMPTY_HISTORICAL_POOL_DATE_NOT_VERIFIED')
    validated=[]
    for row in rows:
        code=row['c']
        price=prices.get(code,{}).get(day) if prices is not None else None
        proof='PROVIDER_SAME_DAY_QDATE' if qdate==day.replace('-','') else 'UNKNOWN'
        if proof=='UNKNOWN':
            try:
                if price is None:
                    price=raw_closes(code,anchor or day).get(day)
                if price is not None and abs(float(price)-float(row['p'])/1000)<0.000001:
                    proof='HISTORICAL_CLOSE_MATCH'
            except Exception:
                pass
        validated.append({'code':code,'name':row['n'],'consecutive':int(row['lbc']),
            'reported_close':float(row['p'])/1000,'date_validation':proof})
    result={'date':day,'response_date':day,'response_market_qdate':qdate,'source':'EASTMONEY_ZT_HISTORY',
        'source_timestamp':day+'T15:00:00+08:00','source_time_precision':'DATE_CLOSE; not provider tick timestamp',
        'fetched_at':pm01.now_cn().isoformat(),'coverage':'PROVIDER_TOPIC_SCOPE_NOT_ALL_A_VERIFIED',
        'rows':validated,'date_validation':'PER_ROW_HISTORICAL_CLOSE_MATCH; qdate is latest market date'}
    atomic_json(path,result)
    return result


def observed_limit_history(snapshot):
    """当日采集用源涨停价；主板、ST、20%/30%股票均不套统一涨幅。
    只接受当天已收盘报价；无涨跌幅限制的0值不构造涨停。
    """
    day=snapshot['d0']
    rows=[]
    missing=[]
    for q in snapshot['rows'].values():
        ceiling=q.get('limit_up_price')
        if not isinstance(q.get('price'),(float,int)) or not math.isfinite(q['price']) or q['price']<=0:
            missing.append(q['code']);continue
        if not isinstance(ceiling,(float,int)) or not math.isfinite(ceiling):
            missing.append(q['code']);continue
        if q['source_timestamp'][:10]!=day or q['source_timestamp'][11:16]<'15:00':
            missing.append(q['code']);continue
        if ceiling>0 and abs(q['price']-ceiling)<0.000001:
            rows.append({'code':q['code'],'name':q['name'],'consecutive':None,
                'close':q['price'],'limit_up_price':ceiling,'source_timestamp':q['source_timestamp']})
    complete=not missing and len(snapshot['rows'])==snapshot['market_count']
    result={'date':day,'response_date':day,'source':'TENCENT_OBSERVED_CLOSE_LIMIT',
        'source_timestamp':snapshot['source_timestamp'],'fetched_at':pm01.now_cn().isoformat(),
        'coverage':'ALL_A_OBSERVED' if complete else 'INCOMPLETE_OBSERVATION',
        'rows':rows,'missing_codes':missing}
    if complete:
        atomic_json(CACHE/'limit_up'/(day+'.json'),result)
    return result


def histories(snapshot):
    """重用已有真实历史收盘价，加上D0真实收盘；不生成缺失日期的价格。"""
    d0=snapshot['d0']
    bases=list(snapshot['base_dates'].values())
    result={}
    gaps=dict(snapshot.get('history_failures',{}))
    for code,q in snapshot['rows'].items():
        key=hashlib.sha256((code+d0+','.join(bases)).encode()).hexdigest()
        path=ROOT/'.pm01_history'/(key+'.json')
        try:
            entry=json.loads(path.read_text(encoding='utf8'))
            closes={day:float(price) for day,price in entry['closes'].items() if day<d0 and float(price)>0}
            if q['price']>0 and q['source_timestamp'][:10]==d0:
                closes[d0]=q['price']
            result[code]=closes
        except (OSError,ValueError,KeyError,TypeError):
            gaps.setdefault(code,'HISTORY_UNAVAILABLE')
    return result,gaps


def top50(day,prices,market_rows,universe_asof):
    dates=trading_dates((datetime.fromisoformat(day)-timedelta(days=80)).date().isoformat(),day)
    base={str(n):dates[-1-n] for n in (3,5,10)}
    top={}
    missing={}
    for n in ('3','5','10'):
        ranked=[]
        gaps=[]
        for code,q in market_rows.items():
            bars=prices.get(code,{})
            if day not in bars or base[n] not in bars:
                gaps.append(code);continue
            price=bars[day]
            prev=bars.get(dates[-2])
            ranked.append({**q,'price':price,'day_pct':(price/prev-1)*100 if prev else 'UNKNOWN',
                'period_pct':(price/bars[base[n]]-1)*100,'price_date':day,'name_metadata_asof':universe_asof,
                'source_timestamp':q['source_timestamp'] if day==universe_asof else day+' (DAILY_CLOSE; exact tick time UNKNOWN)',
                'quote_source':q.get('quote_source','UNKNOWN') if day==universe_asof else 'HISTORICAL_UNADJUSTED_DAILY_CLOSE'})
        ranked.sort(key=lambda r:(-r['period_pct'],r['code']))
        top[n]=[{**r,'rank':i+1} for i,r in enumerate(ranked[:50])]
        missing[n]=gaps
    complete=day==universe_asof and all(len(top[n])==50 and not missing[n] for n in top)
    return {'date':day,'top':top,'base_dates':base,'missing':missing,
        'certified':complete,'universe_asof':universe_asof,
        'status':'PASS' if complete else 'PARTIAL'}


def second_board(day,feeds,calendar,prior):
    """最近二板日期指一段连板的第2个交易日，不因3/4/5板反复延后。"""
    cutoff=(datetime.fromisoformat(day)-timedelta(days=WINDOW_DAYS)).date().isoformat()
    index={d:i for i,d in enumerate(calendar)}
    latest={}
    unresolved=[]
    for date in calendar:
        if date>day:
            break
        feed=feeds.get(date)
        if not feed:
            continue
        for q in feed['rows']:
            if q.get('date_validation')=='UNKNOWN':
                unresolved.append(q['code']+':UNVERIFIED_HISTORY_DATE:'+date)
                continue
            run=q.get('consecutive')
            event=None
            if isinstance(run,int) and run>=2 and index[date]>=run-2:
                event=calendar[index[date]-(run-2)]
            elif feed['source']=='TENCENT_OBSERVED_CLOSE_LIMIT':
                i=index[date]
                prev=feeds.get(calendar[i-1]) if i else None
                prev_row=next((r for r in prev['rows'] if r['code']==q['code'] and r.get('date_validation')!='UNKNOWN'),None) if prev else None
                if prev_row and isinstance(prev_row.get('consecutive'),int):
                    run=prev_row['consecutive']+1
                    if i>=run-2: event=calendar[i-(run-2)]
                elif prev_row and prev['coverage']=='ALL_A_OBSERVED':
                    # 沿已确认完整的连续交易日向前追溯本段连板开始。
                    start=i-1
                    while start>0:
                        earlier=feeds.get(calendar[start-1])
                        if not earlier or earlier['coverage']!='ALL_A_OBSERVED' or q['code'] not in {r['code'] for r in earlier['rows']}:
                            break
                        start-=1
                    earlier=feeds.get(calendar[start-1]) if start>0 else None
                    if earlier and earlier['coverage']=='ALL_A_OBSERVED' and q['code'] not in {r['code'] for r in earlier['rows']}:
                        event=calendar[start+1]
                    else:
                        unresolved.append(q['code']+':'+date)
            if event and cutoff<=event<=day:
                row={'code':q['code'],'name':q['name'],'latest_second_board_date':event,
                    'age_trading_days':index[day]-index[event],'age_calendar_days':(datetime.fromisoformat(day)-datetime.fromisoformat(event)).days,
                    'pool_status':'CURRENT','evidence_date':date,'evidence_source':feed['source']}
                if q['code'] not in latest or event>latest[q['code']]['latest_second_board_date']:
                    latest[q['code']]=row
    required=[d for d in calendar if cutoff<=d<=day]
    absent=[d for d in required if d not in feeds]
    limited=[d for d in required if d in feeds and feeds[d]['coverage']!='ALL_A_OBSERVED']
    before={r['code'] for r in prior}
    return {'rows':[latest[c] for c in sorted(latest)],'new':sorted(set(latest)-before),
        'removed':sorted(before-set(latest)) if not absent and not limited and not unresolved else [],
        'removed_status':'VERIFIED' if not absent and not limited and not unresolved else 'UNKNOWN_DATA_GAP',
        'status':'PASS' if not absent and not limited and not unresolved else 'PARTIAL',
        'missing_dates':absent,'limited_coverage_dates':limited,'unresolved_run_start':unresolved,'window_calendar_days':WINDOW_DAYS}


def leaders(day,top,prices,prior,seed_complete,ledger=None):
    common=set.intersection(*({r['code'] for r in top['top'][n]} for n in ('3','5','10')))
    state={r['code']:copy.deepcopy(r) for r in prior}
    ledger=copy.deepcopy(ledger or {})
    for code,row in state.items():
        ledger.setdefault(code,{'first_entry_date':row.get('first_entry_date','UNKNOWN'),'last_common_certification_date':row.get('last_common_certification_date','UNKNOWN')})
    old=set(state)
    names={r['code']:r['name'] for rows in top['top'].values() for r in rows}
    for code in common:
        ledger.setdefault(code,{'first_entry_date':day})
        ledger[code]['last_common_certification_date']=day
        state.setdefault(code,{'code':code,'name':names[code],'first_entry_date':ledger[code]['first_entry_date'],
            'entry_date_basis':'OFFICIAL_SEED_OR_RUNNING_VERSION' if seed_complete else 'REPLAY_FIRST_SEEN_FROM_EMPTY_SEED'})
        state[code]['last_common_certification_date']=day
    removed=[]
    gaps=[]
    exits=[]
    for code in list(state):
        bars=prices.get(code,{})
        ordered=pd.Series({d:p for d,p in sorted(bars.items()) if d<=day},dtype=float)
        if day not in bars or len(ordered)<20:
            state[code].update(close=bars.get(day,'UNKNOWN'),ma20='UNKNOWN',pool_status='UNKNOWN_MA20')
            gaps.append(code);continue
        close=bars[day]
        ma=float(ordered.iloc[-20:].mean())
        if close<ma:
            removed.append(code)
            exits.append({**state.pop(code),'exit_date':day,'close':close,'ma20':ma,'exit_reason':'CLOSE_LT_MA20'})
        else:
            state[code].update(close=close,ma20=ma,pool_status='CURRENT',price_mode='UNADJUSTED_DAILY_CLOSE',ma_observations=20)
    metadata_gaps=[r['code'] for r in state.values() if r.get('first_entry_date','UNKNOWN')=='UNKNOWN']
    status='PASS' if top['certified'] and seed_complete and not gaps and not metadata_gaps else 'PARTIAL'
    return {'rows':[state[c] for c in sorted(state)],'new':sorted(set(state)-old),
        'removed_ma20':sorted(removed),'exit_records':exits,'common_codes':sorted(common),
        'ma20_gaps':gaps,'metadata_gaps':metadata_gaps,'seed_complete':seed_complete,'status':status,'entry_ledger':ledger,
        'warning':None if seed_complete else 'COLD_START; earlier surviving members are not known'}


def daily_version(day,top,second,leader,snapshot,previous=None):
    previous_day=previous.get('d0') if previous else None
    timeline=trading_dates((datetime.fromisoformat(day)-timedelta(days=20)).date().isoformat(),day)
    continuity=not previous or previous_day==timeline[-2]
    valid=top['certified'] and second['status']=='PASS' and leader['status']=='PASS' and continuity
    result={'product':'FIVE_SOURCE_AUTO_V0.1','d0':day,'generated_at':pm01.now_cn().isoformat(),
        'version_id':day+'-'+hashlib.sha256(json.dumps([snapshot['snapshot_id'],top,second,leader],sort_keys=True,ensure_ascii=False).encode()).hexdigest()[:16],
        'five_source_status':'PASS' if valid else 'FAIL','mode':'FORMAL' if valid else 'RESEARCH_CANDIDATE',
        'continuity_status':'PASS' if continuity else 'GAP','previous_date':previous_day,
        'top':top,'second_board':second,'leader_pool':leader,'price_mode':'UNADJUSTED_DAILY_CLOSE',
        'manual_pool_comparison':'NOT_VERIFIED'}
    # 保存同D0行情与池版本，未来页面不把旧日期伪装为当天。
    if day==snapshot['d0']:
        copy_snapshot=copy.deepcopy(snapshot)
        codes={r['code'] for n in top['top'].values() for r in n}|{r['code'] for r in second['rows']}|{r['code'] for r in leader['rows']}
        copy_snapshot['rows']={c:q for c,q in snapshot['rows'].items() if c in codes}
        result['snapshot']=copy_snapshot
    return result


def publish(version,folder=DATA):
    """失败候选不能覆盖上一份已验证正式池；每天留版本用于继承/审计。"""
    folder=Path(folder)
    archive=folder/'archive'/(version['version_id']+'.json')
    if not archive.exists():
        atomic_json(archive,version)
    atomic_json(folder/(version['d0']+'.json'),version)
    atomic_json(folder/'latest_candidate.json',version)
    if version['five_source_status']=='PASS':
        atomic_json(folder/'latest_verified.json',version)


def report(v):
    return '\n'.join(['FIVE_SOURCE_AUTO_V0.1','D0_DATE='+v['d0'],
        *[n+'D_TOP50='+str(len(v['top']['top'][n]))+'/50' for n in ('3','5','10')],
        'SECOND_BOARD_POOL_COUNT='+str(len(v['second_board']['rows'])),
        'SECOND_BOARD_NEW='+str(len(v['second_board']['new'])),
        'SECOND_BOARD_REMOVED='+('UNKNOWN' if v['second_board']['removed_status']!='VERIFIED' else str(len(v['second_board']['removed']))),
        'LEADER_POOL_COUNT='+str(len(v['leader_pool']['rows'])),
        'LEADER_POOL_NEW='+str(len(v['leader_pool']['new'])),
        'LEADER_POOL_REMOVED_MA20='+str(len(v['leader_pool']['removed_ma20'])),
        'FIVE_SOURCE_STATUS='+v['five_source_status'],'VERSION_ID='+v['version_id'],
        'MODE='+v['mode'],'LEADER_SEED_COMPLETE='+str(v['leader_pool']['seed_complete']),
        'CONTINUITY_STATUS='+v['continuity_status'],'MANUAL_POOL_COMPARISON='+v['manual_pool_comparison']])


def connect(s,v):
    """同D0绑定自动池与当前报价；失败候选可显示但不发正式PASS。"""
    s=copy.deepcopy(s)
    if not v or v['d0']!=s['d0']:
        s['second']=[];s['leader']=[]
        s['second_status']='WAITING_VERSION';s['leader_status']='WAITING_VERSION'
        s['missing']=['AUTO_POOL_D0_VERSION_MISSING']
        s['ready']='NO';s['input_freeze']='FAIL'
        return s
    for key,asset in [('second','second_board'),('leader','leader_pool')]:
        rows=[]
        missing=[]
        for asset_row in v[asset]['rows']:
            c=asset_row['code']
            q=s['rows'].get(c)
            if not q or q['source_timestamp'][:10]!=s['d0']:
                missing.append(c)
                q={'code':c,'name':asset_row['name'],'price':'UNKNOWN','day_pct':'UNKNOWN',
                    'source_timestamp':'UNKNOWN','quote_source':'UNKNOWN'}
                s['rows'][c]=q
            acc,reason=pm01.eligible(c,q['name'])
            if c in missing:
                acc,reason='NO',reason+',NO_CURRENT_QUOTE'
            rows.append({**q,'account_eligible':acc,'account_reason':reason,'market_universe':'YES',
                'POOL_VERSION_ID':v['version_id'],'POOL_ASOF_DATE':v['d0'],'POOL_ASSET':asset_row,
                **s['pool_metadata']['SECOND_BOARD_POOL' if key=='second' else 'LEADER_POOL']})
        s[key]=rows
        s[key+'_status']='PASS' if v[asset]['status']=='PASS' and not missing else 'PARTIAL'
    s['trace']['AUTO_POOL']={'DATA_SOURCE':'FIVE_SOURCE_AUTO_V0.1','PRIMARY_SOURCE':'PASS' if v['five_source_status']=='PASS' else 'FAIL',
        'VERSION_ID':v['version_id'],'ASOF_DATE':v['d0'],'MODE':v['mode'],'FIVE_SOURCE_STATUS':v['five_source_status']}
    s['bundle_id']=s['bundle_id']+'-'+v['version_id'].split('-')[-1]
    for n,rows in s['top'].items():
        for r in rows:
            r['SNAPSHOT_ID']=s['bundle_id']
    for key in ('second','leader'):
        for r in s[key]:
            r['SNAPSHOT_ID']=s['bundle_id']
    for metadata in s['pool_metadata'].values():
        metadata['SNAPSHOT_ID']=s['bundle_id']
    s['missing']=[n+'D_TOP50' for n in ('3','5','10') if s['pool_status'][n]!='PASS']
    s['missing'] += [key.upper()+'_POOL:'+s[key+'_status'] for key in ('second','leader') if s[key+'_status']!='PASS']
    if s['same_snapshot']!='PASS':s['missing'].append('SAME_SNAPSHOT_STATUS')
    if v['five_source_status']!='PASS':s['missing'].append('AUTO_POOL_VERSION_NOT_VERIFIED')
    s['ready']='NO' if s['missing'] else 'YES'
    s['input_freeze']='FAIL' if s['missing'] else 'PASS'
    return s


def load_latest():
    for path in (DATA/'latest_candidate.json',ROOT/'latest_candidate.json'):
        try:
            value=json.loads(path.read_text(encoding='utf8'))
            if value['product']=='FIVE_SOURCE_AUTO_V0.1':
                return value
        except (OSError,ValueError,KeyError):
            pass
    return None


def render_status(st,v):
    with st.expander('五源自动维护｜盘后版本与继承状态',expanded=False):
        st.caption('最近一个月按30个自然日；最近二板日期是本段连板的第2个交易日；MA20使用未复权日收盘价。')
        if not v:
            st.warning('自动版本尚未取得。正式版本只在历史回放与数据完整性验证通过后启用。')
            return
        st.code(report(v),language=None)
        if v['mode']!='FORMAL':
            st.warning('当前是自动生成的研究候选池；历史涨停覆盖、全市场历史或正式继承起点尚未通过，不是最新正式五源。')
        st.caption('定时云端维护尚未启用：先完成历史回放验收，再进入下一交易日前向运行。')
        st.write('二板总池')
        st.dataframe(pd.DataFrame(v['second_board']['rows']),hide_index=True,use_container_width=True)
        st.write('总龙头池')
        st.dataframe(pd.DataFrame(v['leader_pool']['rows']),hide_index=True,use_container_width=True)
        st.download_button('下载自动池版本JSON',json.dumps(v,ensure_ascii=False,indent=2).encode('utf8'),file_name='FIVE_SOURCE_AUTO_'+v['d0']+'.json',mime='application/json')
