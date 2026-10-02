"""PM01 Reality: deterministic data supply; no strategy/AI/database writes."""
import hashlib
import copy
import html
import json
import math
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timedelta, time as walltime
from pathlib import Path
from zoneinfo import ZoneInfo
from urllib.parse import urlencode
import requests

CN = ZoneInfo('Asia/Shanghai')
LOCAL = threading.local()
CACHE_LOCK = threading.Lock()
UNIVERSE_CACHE = None
HISTORY_MEMORY = {}
PERIODS = (3, 5, 10)
SSE_CALENDAR = 'https://www.sse.com.cn/disclosure/dealinstruc/closed/c/c_20251222_10802510.shtml'
HOLIDAYS_2026 = [('01-01','01-03'),('02-15','02-23'),('04-04','04-06'),('05-01','05-05'),('06-19','06-21'),('09-25','09-27'),('10-01','10-07')]


def now_cn():
    return datetime.now(CN)


def get(url, params=None):
    if not hasattr(LOCAL, 'session'):
        LOCAL.session = requests.Session()
    for attempt in range(2):
        try:
            target = url + ('?' + urlencode(params, safe=',:') if params else '')
            r = LOCAL.session.get(target, timeout=(3, 7), headers={'User-Agent':'Mozilla/5.0','Referer':'https://gu.qq.com/'})
            r.raise_for_status()
            r.encoding = "utf-8"
            return r
        except Exception:
            if attempt:
                raise
            time.sleep(0.35)


def fanout(items, fn, progress=None, budget=240, workers=6):
    """Bounded requests and submission; no thousands of orphaned tasks after timeout."""
    started = time.monotonic()
    results, failures = {}, {}
    iterator = iter(items)
    pool = ThreadPoolExecutor(max_workers=workers)
    pending = {}
    def submit():
        try:
            item = next(iterator)
        except StopIteration:
            return False
        pending[pool.submit(fn, item)] = item
        return True
    for _ in range(workers):
        submit()
    try:
        while pending and time.monotonic()-started < budget:
            done, _ = wait(pending, timeout=0.5, return_when=FIRST_COMPLETED)
            for future in done:
                item = pending.pop(future)
                key = item['code'] if isinstance(item, dict) else str(item)
                try:
                    results[key] = future.result()
                except Exception as e:
                    failures[key] = type(e).__name__
                submit()
            if progress:
                progress(len(results)+len(failures), len(items), time.monotonic()-started)
        for item in pending.values():
            failures[item['code'] if isinstance(item,dict) else str(item)] = 'TIMEOUT'
        for item in iterator:
            failures[item['code'] if isinstance(item,dict) else str(item)] = 'TIMEOUT'
    finally:
        for future in pending:
            future.cancel()
        pool.shutdown(wait=False, cancel_futures=True)
    return results, failures


def eligible(code, name):
    reasons = []
    if code.startswith(('300','688','92')):
        reasons.append('ACCOUNT_PREFIX')
    if 'ST' in name.upper():
        reasons.append('ST')
    if '退' in name or name.upper().startswith('PT'):
        reasons.append('DELISTING')
    return ('NO' if reasons else 'YES'), ','.join(reasons) or 'NONE'


def symbol(code):
    return ('bj' if code.startswith(('4','8','92')) else 'sh' if code.startswith('6') else 'sz') + code


def em_universe(progress=None):
    base = 'https://82.push2.eastmoney.com/api/qt/clist/get'
    params = dict(pn=1,pz=100,fs='m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048',fields='f12,f14,f2,f3,f124,f13',fltt=2)
    def page(n):
        data = get(base, {**params,'pn':n}).json()['data']
        diff = data['diff']
        return data['total'], list(diff.values()) if isinstance(diff,dict) else diff
    count, first = page(1)
    pages, errors = fanout(list(range(2, math.ceil(count/100)+1)),page,progress,budget=100,workers=4)
    if errors or any(x[0]!=count for x in pages.values()):
        raise ValueError('EM_UNIVERSE_INCOMPLETE')
    rows = first + [r for _, p in pages.values() for r in p]
    if len(rows)!=count or len({x['f12'] for x in rows})!=count:
        raise ValueError('EM_COUNT_MISMATCH')
    return [{'code':x['f12'],'name':x['f14']} for x in rows]


def sina_universe(progress=None):
    base = 'https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.'
    count = int(get(base+'getHQNodeStockCount',{'node':'hs_a'}).text.strip().strip('"'))
    if not 4000 <= count <= 15000:
        raise ValueError('UNIVERSE_COUNT')
    def page(n):
        return get(base+'getHQNodeData',dict(node='hs_a',page=n,num=80,sort='symbol',asc=1)).json()
    pages, errors = fanout(list(range(1,math.ceil(count/80)+1)),page,progress,budget=100,workers=4)
    rows = [r for p in pages.values() for r in p]
    if errors or len(rows)!=count or len({x['code'] for x in rows})!=count:
        raise ValueError('SINA_UNIVERSE_INCOMPLETE')
    if not any(x['symbol'].startswith('bj') for x in rows):
        raise ValueError('BJ_UNIVERSE_MISSING')
    return [{'code':x['code'],'name':x['name']} for x in rows]


def tencent_quotes(codes):
    errors=[]
    for host in ('https://qt.gtimg.cn','https://web.sqt.gtimg.cn'):
        try:
            r=get(host+'/q='+','.join(symbol(c) for c in codes))
            rows=[]
            for sym,body in re.findall(r'v_((?:sh|sz|bj)\d{6})="([^"]*)"',r.content.decode('gb18030')):
                a=body.split('~')
                stamp=datetime.strptime(a[30],'%Y%m%d%H%M%S').replace(tzinfo=CN)
                price,pct=float(a[3]),float(a[32])
                def number(i):
                    try:
                        value=float(a[i])
                        return value if math.isfinite(value) else 'UNKNOWN'
                    except (ValueError,IndexError):
                        return 'UNKNOWN'
                if not all(math.isfinite(v) for v in (price,pct)):
                    raise ValueError('INVALID_QUOTE')
                rows.append(dict(code=sym[2:],name=a[1],price=price,day_pct=pct,prev_close=number(4),day_high=number(33),day_low=number(34),turnover=number(38),amount=number(37)*10000 if isinstance(number(37),float) else 'UNKNOWN',source_timestamp=stamp.isoformat(),quote_source=host,primary_source='PASS' if host=='https://qt.gtimg.cn' else 'FAIL'))
            if {r['code'] for r in rows}!=set(codes):
                raise ValueError('QUOTE_BATCH_INCOMPLETE')
            return rows
        except Exception as e:
            errors.append(type(e).__name__)
    # Independent provider, explicitly carried into the audit trail.
    rows=[]
    for code in codes:
        secid=('1' if code.startswith('6') else '0')+'.'+code
        d=get('https://push2.eastmoney.com/api/qt/stock/get',dict(secid=secid,fields='f43,f57,f58,f170,f86',fltt=2)).json()['data']
        rows.append(dict(code=d['f57'],name=d['f58'],price=float(d['f43']),day_pct=float(d['f170']),source_timestamp=datetime.fromtimestamp(d['f86'],CN).isoformat(),quote_source='EASTMONEY_FALLBACK',primary_source='FAIL'))
    return rows


def em_history(code, d0):
    d=get('https://push2his.eastmoney.com/api/qt/stock/kline/get',dict(secid=('1' if code.startswith('6') else '0')+'.'+code,klt=101,fqt=0,beg=(datetime.fromisoformat(d0)-timedelta(days=90)).strftime('%Y%m%d'),end=d0.replace('-',''),fields1='f1,f2,f3,f4,f5,f6',fields2='f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61')).json()['data']
    if not d or 'klines' not in d:
        raise ValueError('HISTORY_EMPTY')
    return {a[0]:float(a[2]) for a in (line.split(',') for line in d['klines'])}


def history(code, d0, bases):
    # Cache immutable prior-day closes only, keyed by required D0 and dates.
    key=hashlib.sha256((code+d0+','.join(bases)).encode()).hexdigest()
    # 只复用已经核验的历史基准收盘价；当前行情始终重新请求。
    with CACHE_LOCK:
        if key in HISTORY_MEMORY:
            return copy.deepcopy({**HISTORY_MEMORY[key], 'cache':'PRIOR_CLOSE_MEMORY'})
    folder=Path(__file__).parent/'.pm01_history'
    path=folder/(key+'.json')
    try:
        cached=json.loads(path.read_text(encoding='utf8'))
        if all(day in cached['closes'] for day in bases):
            with CACHE_LOCK:
                if len(HISTORY_MEMORY)>=6000:
                    HISTORY_MEMORY.pop(next(iter(HISTORY_MEMORY)))
                HISTORY_MEMORY[key]=cached
            return {**cached,'cache':'PRIOR_CLOSE_CACHE'}
    except (OSError,ValueError,KeyError):
        pass
    closes=None
    source=None
    endpoints=[('https://web.ifzq.gtimg.cn/appstock/app/fqkline/get','TENCENT_RAW_DAILY',symbol(code)+',day,,,80,'),('https://web.ifzq.gtimg.cn/appstock/app/kline/kline','TENCENT_RAW_KLINE_BACKUP',symbol(code)+',day,,,80'),('https://proxy.finance.qq.com/ifzqgtimg/appstock/app/fqkline/get','TENCENT_PROXY_RAW_BACKUP',symbol(code)+',day,,,80,')]
    rejected=getattr(LOCAL,'history_rejected',{})
    fallback=False
    for i,(endpoint,label,param) in enumerate(endpoints):
        if time.monotonic()-rejected.get(endpoint,-100000)<120:
            fallback=True
            continue
        try:
            d=get(endpoint,{'param':param}).json()['data'][symbol(code)]
            candidate={a[0]:float(a[2]) for a in d.get('day',[])}
            if not all(day in candidate for day in bases):
                raise ValueError('EXACT_BASE_DATE_MISSING')
            closes,source=candidate,label
            fallback=i>0
            break
        except requests.RequestException:
            rejected[endpoint]=time.monotonic()
            LOCAL.history_rejected=rejected
            fallback=True
        except (ValueError,KeyError,TypeError):
            fallback=True
    if closes is None:
        try:
            raw=get('https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20pm01=/CN_MarketDataService.getKLineData',dict(symbol=symbol(code),scale=240,ma='no',datalen=80)).text
            match=re.search(r'var pm01=\((\[.*\])\);?\s*$',raw,re.S)
            if not match:
                raise ValueError('SINA_DAILY_FORMAT')
            bars=json.loads(match.group(1))
            candidate={r['day'][:10]:float(r['close']) for r in bars}
            if not all(day in candidate for day in bases):
                raise ValueError('SINA_EXACT_BASE_MISSING')
            closes,source=candidate,'SINA_RAW_DAILY_BACKUP'
            fallback=True
        except Exception:
            closes=em_history(code,d0)
            source='EASTMONEY_RAW_DAILY'
            fallback=True
    # Never replace an exchange base date by the stock's Nth available bar.
    valid={day:value for day,value in closes.items() if day<d0 and math.isfinite(value) and value>0}
    if not all(day in valid for day in bases):
        raise ValueError('EXACT_BASE_DATE_MISSING')
    result=dict(closes=valid,source=source,primary_source='FAIL' if fallback else 'PASS',fetched_at=now_cn().isoformat())
    with CACHE_LOCK:
        if len(HISTORY_MEMORY)>=6000:
            HISTORY_MEMORY.pop(next(iter(HISTORY_MEMORY)))
        HISTORY_MEMORY[key]=result
    try:
        folder.mkdir(exist_ok=True)
        temp=path.with_suffix('.'+uuid.uuid4().hex+'.tmp')
        temp.write_text(json.dumps(result),encoding='utf8')
        temp.replace(path)
    except OSError:
        pass
    return result


def trading_day(day):
    d=datetime.fromisoformat(day)
    if d.year!=2026:
        return None
    mmdd=d.strftime('%m-%d')
    return d.weekday()<5 and not any(a<=mmdd<=b for a,b in HOLIDAYS_2026)


def time_mode(now, d0, fresh):
    trade=trading_day(now.date().isoformat())
    t=now.time().replace(tzinfo=None)
    if trade is None:
        phase='CALENDAR_UNVERIFIED'
    elif not trade:
        phase='非交易日'
    elif walltime(14)<=t<walltime(15):
        phase='尾盘Production窗口'
    elif walltime(9,30)<=t<walltime(11,30) or walltime(13)<=t<walltime(15):
        phase='交易时段'
    elif t>=walltime(15):
        phase='收盘后'
    else:
        phase='交易日非交易时段'
    live=phase=='尾盘Production窗口' and d0==now.date().isoformat() and fresh
    return phase, 'NO' if live else 'YES'


def blank_snapshot(error='NOT_REFRESHED'):
    return dict(snapshot_id=uuid.uuid4().hex,d0='',snapshot_time=now_cn().isoformat(),data_source='NONE',source_timestamp='UNKNOWN',market_count=0,rows={},top={str(n):[] for n in PERIODS},pool_status={str(n):'FAIL' for n in PERIODS},errors=[error],same_snapshot='FAIL',fresh=False,trace={})


def refresh(progress=None):
    global UNIVERSE_CACHE
    s=blank_snapshot()
    def notify(done,total,elapsed):
        if progress: progress(done,total,elapsed)
    try:
        with CACHE_LOCK:
            cached=copy.deepcopy(UNIVERSE_CACHE)
        if cached and time.monotonic()-cached['at']<3600:
            universe=cached['rows']
            s['trace']['UNIVERSE']={**cached['trace'],'CACHE':'ONE_HOUR_UNIVERSE_CACHE'}
        else:
            try:
                universe=sina_universe(notify)
                trace={'PRIMARY_SOURCE':'PASS','DATA_SOURCE':'SINA_HS_A_INCLUDES_BJ'}
            except Exception:
                universe=em_universe(notify)
                trace={'PRIMARY_SOURCE':'FAIL','FALLBACK_SOURCE':'EASTMONEY','DATA_SOURCE':'EASTMONEY'}
            trace['FETCHED_AT']=now_cn().isoformat()
            s['trace']['UNIVERSE']=trace
            with CACHE_LOCK:
                UNIVERSE_CACHE={'at':time.monotonic(),'rows':universe,'trace':trace}
        s['market_count']=len(universe)
        codes=sorted(x['code'] for x in universe)
        batches=[codes[i:i+60] for i in range(0,len(codes),60)]
        q,failed=fanout(batches,tencent_quotes,notify,budget=180,workers=4)
        quotes={r['code']:r for batch in q.values() for r in batch}
        s['rows']=quotes
        s['data_source']=s['trace']['UNIVERSE']['DATA_SOURCE']+';QUOTES='+','.join(sorted({r['quote_source'] for r in quotes.values()}))
        s['errors']=[]
        if failed or set(quotes)!=set(codes):
            s['errors'].append('MARKET_QUOTES_INCOMPLETE:'+str(len(set(codes)-set(quotes))))
        if not quotes:
            raise ValueError('ALL_QUOTES_FAILED')
        s['d0']=max(r['source_timestamp'][:10] for r in quotes.values())
        d0=s['d0']
        s['source_timestamp']=max(r['source_timestamp'] for r in quotes.values())
        s['snapshot_time']=now_cn().isoformat()
        # Official exchange calendar, never natural-day offsets or per-stock bar counts.
        if trading_day(d0) is not True:
            raise ValueError('D0_CALENDAR_UNVERIFIED')
        days=[]
        cursor=datetime.fromisoformat(d0)
        while len(days)<11:
            state=trading_day(cursor.date().isoformat())
            if state is None:
                raise ValueError('CALENDAR_YEAR_UNVERIFIED')
            if state:
                days.append(cursor.date().isoformat())
            cursor-=timedelta(days=1)
        days.sort()
        s['trace']['CALENDAR']={'PRIMARY_SOURCE':'PASS','DATA_SOURCE':'SSE_OFFICIAL_2026','SOURCE_URL':SSE_CALENDAR,'VERIFIED_ON':'2026-10-02'}
        s['base_dates']={str(n):days[-1-n] for n in PERIODS}
        bases=list(s['base_dates'].values())
        # A stale positive quote could change the Top50: fail certification, never silently omit it.
        current=[r for r in quotes.values() if r['price']>0 and r['source_timestamp'][:10]==d0]
        stale=[r['code'] for r in quotes.values() if r['price']>0 and r['source_timestamp'][:10]!=d0]
        s['stale_quotes']=stale
        s['unpriced_codes']=[r['code'] for r in quotes.values() if r['price']<=0]
        if stale or s['unpriced_codes']:
            s['errors'].append('NON_CURRENT_OR_UNPRICED_MARKET_QUOTES:'+str(len(stale)+len(s['unpriced_codes'])))
        h,hfail=fanout(current,lambda r:history(r['code'],d0,bases),notify,budget=900,workers=6)
        s['history_failures']=hfail
        history_sources={x['source'] for x in h.values()}
        quote_sources={x['quote_source'] for x in quotes.values()}
        s['data_source']=s['trace']['UNIVERSE']['DATA_SOURCE']+';QUOTES='+','.join(sorted(quote_sources))+';HISTORY='+','.join(sorted(history_sources))
        s['trace']['QUOTES']={'PRIMARY_SOURCE':'FAIL' if any(r.get('primary_source')=='FAIL' for r in quotes.values()) else 'PASS','FALLBACK_SOURCE':','.join(sorted(x for x in quote_sources if x!='https://qt.gtimg.cn')) or 'NONE','DATA_SOURCE':sorted(quote_sources)}
        s['trace']['HISTORY']={'PRIMARY_SOURCE':'FAIL' if any(x['primary_source']=='FAIL' for x in h.values()) else 'PASS','FALLBACK_SOURCE':','.join(sorted(x for x in history_sources if x!='TENCENT_RAW_DAILY')) or 'NONE','DATA_SOURCE':sorted(history_sources),'FAILED_COUNT':len(hfail)}
        if hfail:
            s['errors'].append('HISTORY_MISSING:'+str(len(hfail)))
        for n in PERIODS:
            ranked=[]
            for r in current:
                if r['code'] not in h:
                    continue
                acc,reason=eligible(r['code'],r['name'])
                ranked.append({**r,'period_pct':(r['price']/h[r['code']]['closes'][s['base_dates'][str(n)]]-1)*100,'account_eligible':acc,'account_reason':reason,'market_universe':'YES','history_source':h[r['code']]['source']})
            ranked.sort(key=lambda x:(-x['period_pct'],x['code']))
            s['top'][str(n)]=[{**r,'rank':i+1} for i,r in enumerate(ranked[:50])]
            s['pool_status'][str(n)]='PASS' if len(ranked)>=50 and not s['errors'] else 'FAIL'
        quote_times=[datetime.fromisoformat(r['source_timestamp']) for r in current]
        s['fresh']=bool(quote_times) and all(t.date()==now_cn().date() and -30<=(now_cn()-t).total_seconds()<=300 for t in quote_times)
        s['same_snapshot']='PASS' if not s['errors'] else 'FAIL'
        s['history_complete_at']=now_cn().isoformat()
    except Exception as e:
        s['errors'].append('DATA_SOURCE_FAILURE:'+type(e).__name__)
    return s


def parse_pool(text, snapshot):
    if not text.strip():
        return [],'WAITING_INPUT',[]
    codes=list(dict.fromkeys(re.findall(r'(?<!\d)\d{6}(?!\d)',text)))
    malformed=re.findall(r'(?<!\d)\d+(?!\d)',re.sub(r'(?<!\d)\d{6}(?!\d)','',text))
    if not codes or malformed:
        return [],'FAIL',['INVALID_CODE_INPUT']
    rows,errors=[],[]
    for code in codes:
        r=snapshot['rows'].get(code)
        if not r or r['price']<=0 or r['source_timestamp'][:10]!=snapshot['d0']:
            errors.append('UNRESOLVED_OR_STALE:'+code)
            continue
        acc,reason=eligible(code,r['name'])
        rows.append({**r,'account_eligible':acc,'account_reason':reason,'market_universe':'YES'})
    return rows,'FAIL' if errors else 'PASS',errors


def freeze(snapshot, second='', leader=''):
    s=dict(snapshot)
    s['second'],s['second_status'],se=parse_pool(second,s)
    s['leader'],s['leader_status'],le=parse_pool(leader,s)
    # Each input revision gets a new immutable bundle identity, quoting the same captured market map.
    s['bundle_id']=s['snapshot_id']+'-'+hashlib.sha256((second+'\0'+leader).encode()).hexdigest()[:12]
    missing=[f'{n}D_TOP50' for n in PERIODS if s['pool_status'][str(n)]!='PASS']
    if s['second_status']!='PASS': missing.append('SECOND_BOARD_POOL:'+s['second_status'])
    if s['leader_status']!='PASS': missing.append('LEADER_POOL:'+s['leader_status'])
    if s['same_snapshot']!='PASS': missing.append('SAME_SNAPSHOT_STATUS')
    s['input_freeze']='PASS' if not missing else 'FAIL'
    s['ready']='YES' if not missing else 'NO'
    s['missing']=missing+se+le
    s['pool_metadata']={label:dict(SNAPSHOT_ID=s['bundle_id'],D0_DATE=s['d0'],SNAPSHOT_TIME=s['snapshot_time']) for label in ('3D_TOP50','5D_TOP50','10D_TOP50','SECOND_BOARD_POOL','LEADER_POOL')}
    s['top']={n:[{**r,**s['pool_metadata'][n+'D_TOP50']} for r in rows] for n,rows in s['top'].items()}
    for key,label in [('second','SECOND_BOARD_POOL'),('leader','LEADER_POOL')]:
        s[key]=[{**r,**s['pool_metadata'][label]} for r in s[key]]
    s['phase'],s['research_mode']=time_mode(now_cn(),s['d0'],s['fresh'])
    # Freshness is rechecked when copying/displaying, not just at acquisition.
    if s['research_mode']=='NO' and (now_cn()-datetime.fromisoformat(s['snapshot_time'])).total_seconds()>300:
        s['research_mode']='YES'
    return s


def export_text(s):
    lines=['PM01_REALITY',f"D0_DATE={s['d0']}",f"SNAPSHOT_TIME={s['snapshot_time']}",f"SNAPSHOT_ID={s['bundle_id']}",f"DATA_SOURCE={s['data_source']}",f"SOURCE_TIMESTAMP={s['source_timestamp']}",f"MARKET_COUNT={s['market_count']}",f"RESEARCH_MODE={s['research_mode']}",f"TIME_BOUNDARY={s['phase']}"]
    if s['research_mode']=='YES': lines.append('NOT_LIVE_PRODUCTION')
    primary='FAIL' if any(t.get('PRIMARY_SOURCE')=='FAIL' for t in s['trace'].values()) or not s['trace'] else 'PASS'
    fallback=','.join(t.get('FALLBACK_SOURCE','') for t in s['trace'].values() if t.get('FALLBACK_SOURCE') not in (None,'NONE')) or 'NONE'
    lines += ['PRIMARY_SOURCE='+primary,'FALLBACK_SOURCE='+fallback]
    lines += ['MARKET_UNIVERSE=原始市场认证；账户资格不参与排序','RETURN_FORMULA=D0_PRICE / UNADJUSTED_CLOSE(D0-N_EXCHANGE_TRADING_DAYS) - 1','BASE_DATES='+json.dumps(s.get('base_dates',{}),ensure_ascii=False),'SOURCE_TRACE='+json.dumps(s['trace'],ensure_ascii=False)]
    for n in PERIODS: lines.append(f"{n}D_POOL_STATUS={s['pool_status'][str(n)]}")
    lines += [f"SECOND_BOARD_POOL_STATUS={s['second_status']}",f"LEADER_POOL_STATUS={s['leader_status']}",f"SAME_SNAPSHOT_STATUS={s['same_snapshot']}",f"INPUT_FREEZE_STATUS={s['input_freeze']}"]
    def clean(v): return str(v).replace('｜','/').replace('\n',' ').replace('\r',' ')
    def row(r, ranked=False):
        values=([f"{r['rank']:02d}"] if ranked else [])+[r['code'],clean(r['name'])]+([f"{r['period_pct']:.4f}%"] if ranked else [])+[f"{r['price']:.3f}",f"{r['day_pct']:.4f}%",r['account_eligible']]
        return '｜'.join(values)
    for n in PERIODS:
        lines += [f'{n}D_TOP50=']+[row(r,True) for r in s['top'][str(n)]]
    for key,label in [('second','SECOND_BOARD_POOL'),('leader','LEADER_POOL')]:
        lines += [label+'=']+[row(r) for r in s[key]]
    lines += ['DATA_ERRORS='+(','.join(s['errors']) or 'NONE'),f"PM01_READY={s['ready']}",'MISSING_INPUT='+(','.join(s['missing']) or 'NONE'),'END_PM01_REALITY']
    return '\n'.join(lines)


def render():
    import streamlit as st
    import streamlit.components.v1 as components
    st.markdown('<style>h1 {font-size:28px !important;line-height:1.2 !important} .block-container {padding-top:2.5rem}</style>',unsafe_allow_html=True)
    st.title('A股 Production Reality')
    st.caption('PM01 五源输入')
    st.caption('只供给市场Reality；正式二板池和总龙头池由公司提供。')
    # Render placeholders first so inputs remain below the main refresh control.
    card=st.empty()
    refresh_clicked=st.button('刷新PM01 Reality',type='primary',use_container_width=True)
    expanders=[st.expander(label) for label in ('3D Top50','5D Top50','10D Top50','二板总池','总龙头池')]
    def bind_input_day(key):
        st.session_state['pm01_input_day_'+key]=st.session_state.get('pm01_snapshot',{}).get('d0') or None
    with expanders[3]:
        second=st.text_area('二板总池｜粘贴本次D0正式池代码',key='pm01_second',on_change=bind_input_day,args=('second',),placeholder='600000\n000001')
    with expanders[4]:
        leader=st.text_area('总龙头池｜粘贴本次D0正式池代码',key='pm01_leader',on_change=bind_input_day,args=('leader',),placeholder='600000\n000001')
    if refresh_clicked:
        # Remove previous live map before any request, including errors.
        st.session_state['pm01_snapshot']=blank_snapshot('REFRESH_IN_PROGRESS')
        with st.status('获取市场快照与历史收盘价…',expanded=True) as status:
            st.write('首次需全市场历史下载；最长约20分钟。后续只复用已确认的历史收盘价，每次重新取得当前行情。')
            bar=st.progress(0)
            def progress(done,total,elapsed):
                bar.progress(min(done/max(total,1),1),text=f'本阶段 {done}/{total} · {elapsed:.0f} 秒')
            s=refresh(progress)
            st.session_state['pm01_snapshot']=s
            status.update(label='数据已取得' if not s['errors'] else '数据不完整；请查看缺失信息',state='complete' if not s['errors'] else 'error',expanded=False)
    snapshot=st.session_state.get('pm01_snapshot',blank_snapshot())
    inputs={'second':second,'leader':leader}
    for key in inputs:
        date_key='pm01_input_day_'+key
        if snapshot['d0'] and st.session_state.get(date_key) is None:
            st.session_state[date_key]=snapshot['d0']
        if snapshot['d0'] and st.session_state.get(date_key)!=snapshot['d0']:
            inputs[key]=''
            st.warning(('二板总池' if key=='second' else '总龙头池')+'日期已变化：请重新粘贴本次D0的公司正式池。')
    second,leader=inputs['second'],inputs['leader']
    s=freeze(snapshot,second,leader)
    with card.container(border=True):
        obtained=datetime.fromisoformat(s['snapshot_time']).strftime('%Y-%m-%d %H:%M:%S')
        st.write('行情日期：'+(s['d0'] or '等待刷新'))
        st.caption('取得时间：'+obtained+' · UTC+8')
        universe_source=s['trace'].get('UNIVERSE',{}).get('DATA_SOURCE','NONE')
        st.caption('数据源：'+universe_source+'；行情/日线详情见来源审计')
        st.write('全市场 '+str(s['market_count'])+' 只 · PM01_READY='+s['ready'])
        if any(t.get('PRIMARY_SOURCE')=='FAIL' for t in s['trace'].values()):
            st.caption('PRIMARY_SOURCE=FAIL · 已使用备用源，见来源审计')
        st.caption('当前：'+s['phase'])
        if s['research_mode']=='YES': st.warning('RESEARCH_MODE=YES · NOT_LIVE_PRODUCTION')
        if s['missing']: st.caption('五源尚未齐备；缺失明细随复制文本输出。')
        if s['errors'] and s['errors']!=['NOT_REFRESHED']: st.error('数据状态=FAIL；当前数据不完整，请查看来源审计。')
    def display_rows(rows,ranked=False):
        for r in rows:
            prefix=(f"{r['rank']:02d} · " if ranked else '')+r['code']+' '+r['name']
            st.write(prefix)
            st.caption((f"周期 {r['period_pct']:+.4f}% · " if ranked else '')+f"当前价 {r['price']:.3f} · 当日 {r['day_pct']:+.4f}%")
            st.caption('MARKET_UNIVERSE=YES · ACCOUNT_ELIGIBLE='+r['account_eligible']+' · '+r['account_reason'])
    for i,n in enumerate(PERIODS):
        with expanders[i]:
            st.write(f"{n}D_POOL_STATUS={s['pool_status'][str(n)]} · {len(s['top'][str(n)])}/50")
            st.caption('基准交易日：'+s.get('base_dates',{}).get(str(n),'未知')+'；未复权收盘价口径')
            if s['pool_status'][str(n)]!='PASS' and s['top'][str(n)]: st.warning('以下是不完整数据候选，未通过全市场认证，不可作为正式Top50。')
            display_rows(s['top'][str(n)],True)
    for i,key in ((3,'second'),(4,'leader')):
        with expanders[i]:
            st.write(('SECOND_BOARD_POOL_STATUS=' if key=='second' else 'LEADER_POOL_STATUS=')+s[key+'_status'])
            display_rows(s[key])
    from pm03 import render as render_blind
    render_blind(st,s,progress if refresh_clicked else None)
    text=export_text(s)
    # Entire payload stays in the browser iframe; no server or third-party clipboard service.
    encoded=json.dumps(text,ensure_ascii=True).replace('<','\\u003c')
    components.html('''<button id="copy" style="width:100%;padding:14px;border-radius:10px;background:#172b45;color:white;border:0;font-size:16px">复制给高级助理</button><div id="msg" role="status" style="font:14px sans-serif;padding:6px"></div><textarea id="fallback" readonly style="display:none;width:100%;height:120px"></textarea><script>const payload='''+encoded+''';document.getElementById('copy').onclick=async()=>{try{await navigator.clipboard.writeText(payload);document.getElementById('msg').textContent='已复制完整PM01 Reality';}catch(e){const t=document.getElementById('fallback');t.style.display='block';t.value=payload;t.select();try{if(!document.execCommand('copy'))throw Error();document.getElementById('msg').textContent='已复制完整PM01 Reality';}catch(e){document.getElementById('msg').textContent='浏览器限制复制，请长按下方文本全选复制';}}};</script>''',height=225)
    with st.expander('完整复制输出与来源审计'):
        st.caption('SNAPSHOT_ID='+s['bundle_id'])
        st.caption('SOURCE_TIMESTAMP='+s['source_timestamp'])
        st.caption('冻结采集批次，不承诺交易所原子同秒快照；保留逐股源时间。')
        st.json(s['trace'],expanded=False)
        st.code(text,language=None)
        st.download_button('下载本次冻结JSON',json.dumps(s,ensure_ascii=False,indent=2),file_name='pm01-'+s['bundle_id']+'.json',mime='application/json')
        st.caption('2026交易日历来源：'+SSE_CALENDAR+'；其他年份未核验时仅研究且不认证。')
