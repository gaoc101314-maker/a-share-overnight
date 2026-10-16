"""Trusted integration: immutable raw history, official calendar, fresh quotes.

No publication, company-pool replacement or quote-time rewriting here.
"""
import copy
import hashlib
import json
import gzip
import base64
from datetime import timedelta
import pm01
from pm03_quote_diagnostic import adapt_refresh, rank_evidence_provider
from verified_history_seed import load_seed


def calendar(day):
    cursor = pm01.datetime.fromisoformat(day).date().replace(month=1, day=1)
    end = pm01.datetime.fromisoformat(day).date()
    result = []
    while cursor <= end:
        verified = pm01.trading_day(cursor.isoformat())
        if verified is None:
            raise ValueError('CALENDAR_YEAR_UNVERIFIED')
        if verified:
            result.append(cursor.isoformat())
        cursor += timedelta(days=1)
    if not result or result[-1] != day:
        raise ValueError('D0_CALENDAR_UNVERIFIED')
    return result


def context(seed, codes, dates, bases):
    metadata = copy.deepcopy(seed['metadata'])
    metadata.setdefault('verified_historical_suspensions',{}).update(RECOVERED_OCT12['verified_historical_suspensions'])
    metadata.setdefault('listing',{}).update(RECOVERED_OCT12['listing'])
    metadata['verified_historical_suspensions'].update(RECOVERED_OCT13['verified_historical_suspensions'])
    metadata['listing'].update(RECOVERED_OCT13['listing'])
    listings = {code: {'code': code, 'listing_date': value[0],
        'source': value[1], 'evidence': 'Issuer listing announcement independently verified; actual listing date '+value[0]}
        for code, value in metadata['listing'].items() if code in codes}
    # New D0 listing absent from the immutable Oct08 seed. The issuer's actual
    # listing announcement proves that no prior exchange-day close exists.
    # This is period-specific data eligibility, never an account filter.
    if '920157' in codes:
        listings['920157']={'code':'920157','listing_date':'2026-10-09',
            'source':'https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12628834&stockid=920157',
            'evidence':'Issuer listing announcement independently read 2026-10-09; first Beijing exchange trading day 2026-10-09'}
    if '301660' in codes:
        listings['301660']={'code':'301660','listing_date':'2026-10-13',
            'source':'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12636489&stockid=301660',
            'evidence':'Issuer listing announcement independently read 2026-10-13; first trading day 2026-10-13; no prior-day closes invented'}
    suspensions = {}
    # Open-ended halts are proven only through the actual verification date.
    halts={**metadata['halts'],
        '300527':('2026-09-29','2026-09-30','https://static.cninfo.com.cn/finalpage/2026-09-29/1225585910.PDF'),
        '300096':('2026-09-24','2026-09-28','https://static.cninfo.com.cn/finalpage/2026-09-24/1225579834.PDF'),
        '600363':('2026-09-29','2026-09-30','https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12623290&stockid=600363'),
        '600293':('2026-09-29',None,'https://static.cninfo.com.cn/finalpage/2026-09-29/1225586063.PDF')}
    for code, (start, resume, url) in halts.items():
        if code not in codes:
            continue
        bars = seed['daily'].get(code, {}).get('closes', {})
        for base in bases.values():
            prior = sorted(d for d in bars if d < base)
            if not prior:
                continue
            needed = [d for d in dates if prior[-1] < d <= base]
            if needed and all(start <= d <= seed['date'] and (resume is None or d < resume) for d in needed):
                suspensions.setdefault(code, {})[base] = {'code': code,
                    'requested_base_date': base, 'source': url,
                    'evidence': 'Official suspension interval independently verified through ' + seed['date'],
                    'sessions': [{'date': d, 'status': 'SUSPENDED'} for d in needed]}
    # Exact Sep30 halt sessions verified before Oct12. Do not infer a later
    # suspension or resumption from an initial announcement's expected period.
    exact_halts={
        '002667':'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12626556&stockid=002667',
        '002813':'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12633328&stockid=002813',
        '603183':'https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12627725&stockid=603183'}
    for code,url in exact_halts.items():
        if code not in codes:continue
        bars=seed['daily'].get(code,{}).get('closes',{})
        base='2026-09-30';prior=sorted(d for d in bars if d<base)
        needed=[d for d in dates if prior and prior[-1]<d<=base]
        if base in bases.values() and needed==[base]:
            suspensions.setdefault(code,{})[base]={'code':code,'requested_base_date':base,
                'source':url,'evidence':'Issuer announcement independently read 2026-10-09 proves only Sep30 halt; actual prior close date retained',
                'sessions':[{'date':base,'status':'SUSPENDED'}]}
    # Exact historical sessions confirmed in issuer announcements. These are
    # bounded facts, never an open-ended assumption about future suspension.
    oct08_halts={
        '000582':(['2026-10-08'],'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12636392&stockid=000582'),
        '000668':(['2026-10-08'],'https://static.cninfo.com.cn/finalpage/2026-10-08/1225591961.PDF'),
        '002388':(['2026-10-08'],'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12631200&stockid=002388'),
        '300773':(['2026-10-08'],'https://file.finance.sina.com.cn/211.154.219.97:9494/MRGG/CNSESZ_STOCK/2026/2026-10/2026-10-01/12630709.PDF'),
        '002667':(['2026-09-30','2026-10-08'],'https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12636388&stockid=002667'),
        '002813':(['2026-09-30','2026-10-08'],'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12633328&stockid=002813'),
        '603183':(['2026-09-30','2026-10-08'],'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12633365&stockid=603183')}
    for code,(confirmed,url) in oct08_halts.items():
        if code not in codes:continue
        bars=seed['daily'].get(code,{}).get('closes',{})
        for base in bases.values():
            prior=sorted(d for d in bars if d<base)
            needed=[d for d in dates if prior and prior[-1]<d<=base]
            if needed and all(d in confirmed for d in needed):
                suspensions.setdefault(code,{})[base]={'code':code,'requested_base_date':base,
                    'source':url,'evidence':'Issuer historical suspension session independently verified 2026-10-13; actual prior close date retained',
                    'sessions':[{'date':d,'status':'SUSPENDED'} for d in needed]}
    # Newly independently read historical sessions have an explicit end bound.
    # Original open-ended seed halts remain bounded by their original date.
    for code, fact in metadata.get('verified_historical_suspensions', {}).items():
        if code not in codes:
            continue
        bars = seed['daily'].get(code, {}).get('closes', {})
        for base in bases.values():
            prior = sorted(d for d in bars if d < base)
            needed = [d for d in dates if prior and prior[-1] < d <= base]
            if needed and all(fact['start'] <= d <= fact['through'] for d in needed):
                suspensions.setdefault(code, {})[base] = {'code': code,
                    'requested_base_date': base, 'source': fact['source'],
                    'evidence': fact['evidence'] + ' Corroboration: ' + fact['corroboration'],
                    'sessions': [{'date': d, 'status': 'SUSPENDED'} for d in needed]}
    return {'universe_codes': sorted(codes), 'trading_dates': dates,
        'listing_evidence': listings, 'suspension_evidence': suspensions}


def refresh_certified(mode='PRODUCTION', progress=None, stage=None):
    seed = load_seed()
    today = pm01.now_cn().date().isoformat()
    dates = calendar(today)
    bases = {str(n): dates[-1-n] for n in (3, 5, 10)}
    records = [{**v, 'code': code, 'adjustment': 'UNADJUSTED',
        'closes': {d: p for d, p in v['closes'].items() if d < today}}
        for code, v in seed['daily'].items()]
    # Populate the existing exact-date cache only; never synthesize a halt price.
    for r in records:
        if all(base in r['closes'] for base in bases.values()):
            key = hashlib.sha256((r['code']+today+','.join(bases.values())).encode()).hexdigest()
            with pm01.CACHE_LOCK:
                pm01.HISTORY_MEMORY[key] = {**r, 'primary_source': 'PASS' if r['source']=='TENCENT_RAW_DAILY' else 'FAIL'}
    preliminary = context(seed, set(seed['daily'])|{'920157'}, dates, bases)
    original_history = pm01.history
    def history(code, d0, required):
        if d0 == today:
            listing = preliminary['listing_evidence'].get(code)
            if listing and all(listing['listing_date'] > base for base in required):
                raise ValueError('PER_PERIOD_LISTING_EXCLUSION')
            closes = seed['daily'].get(code, {}).get('closes', {})
            if all(base in closes or base in preliminary['suspension_evidence'].get(code, {}) or
                    (listing and listing['listing_date'] > base) for base in required):
                if not all(base in closes for base in required):
                    raise ValueError('PER_PERIOD_REAL_DATE_EVIDENCE')
        return original_history(code, d0, required)
    pm01.history = history
    try:
        snapshot = pm01.refresh(progress=progress, stage=stage, certified=True)
    finally:
        pm01.history = original_history
    if snapshot.get('d0') != today:
        stage=snapshot.get('trace',{}).get('COLLECTION_FAILURE',{}).get('stage','DATE')
        code=snapshot.get('trace',{}).get('UNIVERSE_PRIMARY_FAILURE',{}).get('code','UNAVAILABLE')
        raise ValueError('TODAY_QUOTE_REQUIRED:'+stage+':'+code)
    with pm01.CACHE_LOCK:
        universe = copy.deepcopy(pm01.UNIVERSE_CACHE)
        acquired_history = copy.deepcopy(pm01.HISTORY_MEMORY)
    if not universe:
        raise ValueError('INDEPENDENT_UNIVERSE_REQUIRED')
    codes = {r['code'] for r in universe['rows']}
    actual_context = context(seed, codes, dates, snapshot['base_dates'])
    records = [r for r in records if r['code'] in codes]
    # Reuse only exact-date acquisitions with raw response provenance.
    by_code = {r['code']:r for r in records}
    for code in codes:
        key = hashlib.sha256((code+today+','.join(snapshot['base_dates'].values())).encode()).hexdigest()
        acquired = acquired_history.get(key)
        if acquired and acquired.get('adjustment')=='UNADJUSTED' and acquired.get('evidence'):
            by_code[code] = {**acquired,'code':code}
    records = list(by_code.values())
    if mode == 'CLOSE_MAINTENANCE':
        for code, q in snapshot['rows'].items():
            proof = hashlib.sha256(json.dumps(q, sort_keys=True).encode()).hexdigest()
            q['close_evidence'] = {'kind': 'PROVIDER_REPORTED_CLOSE', 'code': code,
                'date': today, 'price': q['price'], 'source_timestamp': q['source_timestamp'],
                'source': q['quote_source'], 'evidence': 'Original live pm01 collection row sha256=' + proof}
    asof = pm01.now_cn().isoformat()
    evidence = rank_evidence_provider(snapshot, pm01.get, actual_context, records,
        mode=mode, asof=asof)
    result = adapt_refresh(snapshot, evidence, asof, mode=mode)
    # Keep every acquisition event losslessly, within the existing handoff limit.
    adapter=result['trace']['PER_PERIOD_ADAPTER']
    events=adapter['acquisition_diagnostics']
    raw=json.dumps(events,ensure_ascii=False,separators=(',',':')).encode('utf8')
    if len(raw)>3*1024*1024:
        adapter['acquisition_diagnostics']={'encoding':'gzip+base64-json',
            'event_count':len(events),'uncompressed_bytes':len(raw),
            'uncompressed_sha256':hashlib.sha256(raw).hexdigest(),
            'data':base64.b64encode(gzip.compress(raw,mtime=0)).decode('ascii'),
            'readback_instruction':'Decode base64, decompress gzip, verify uncompressed_sha256 and event_count; all original acquisition events retained'}
    # Original freeze/export contract retains account eligibility fields.
    for ranked in result['top'].values():
        for row in ranked:
            account, reason = pm01.eligible(row['code'], row['name'])
            row.update(account_eligible=account, account_reason=reason, market_universe='YES')
    return result

# Bounded source facts shared with the restored original MCP collector.
RECOVERED_OCT12={"verified_historical_suspensions":{"300082":{"start":"2026-09-21","through":"2026-10-12","source":"https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12638736&stockid=300082","corroboration":"https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29","verified_on":"2026-10-15","evidence":"Issuer suspension/resumption original independently read. Continuous interval corroborated against provider observation on Oct15 (not a historical snapshot); observed interval response SHA256=29dd64c96caf91dff61e94e408e5754fc0b6ade4af7bb285e6ca6f40e973d80b. Only already occurred sessions through Oct12 certified; raw prior close date retained. No synthetic base quote, no future halt extension."},"300773":{"start":"2026-10-08","through":"2026-10-12","source":"https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12630709&stockid=300773","corroboration":"https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29","verified_on":"2026-10-15","evidence":"Issuer suspension/resumption original independently read. Continuous interval corroborated against provider observation on Oct15 (not a historical snapshot); observed interval response SHA256=29dd64c96caf91dff61e94e408e5754fc0b6ade4af7bb285e6ca6f40e973d80b. Only already occurred sessions through Oct12 certified; raw prior close date retained. No synthetic base quote, no future halt extension."},"600293":{"start":"2026-09-29","through":"2026-10-12","source":"https://epaper.cs.com.cn/zgzqb/html/2026-10/13/nw.D110000zgzqb_20261013_3-A08.htm","corroboration":"https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29","verified_on":"2026-10-15","evidence":"Issuer suspension/resumption original independently read. Continuous interval corroborated against provider observation on Oct15 (not a historical snapshot); observed interval response SHA256=29dd64c96caf91dff61e94e408e5754fc0b6ade4af7bb285e6ca6f40e973d80b. Only already occurred sessions through Oct12 certified; raw prior close date retained. No synthetic base quote, no future halt extension."},"601059":{"start":"2026-09-15","through":"2026-10-12","source":"https://paper.cnstock.com/html/2026-09/09/content_2266558.htm","corroboration":"https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29","verified_on":"2026-10-15","evidence":"Issuer suspension/resumption original independently read. Continuous interval corroborated against provider observation on Oct15 (not a historical snapshot); observed interval response SHA256=29dd64c96caf91dff61e94e408e5754fc0b6ade4af7bb285e6ca6f40e973d80b. Only already occurred sessions through Oct12 certified; raw prior close date retained. No synthetic base quote, no future halt extension."},"601198":{"start":"2026-09-15","through":"2026-10-12","source":"https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12588671&stockid=601198","corroboration":"https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29","verified_on":"2026-10-15","evidence":"Issuer suspension/resumption original independently read. Continuous interval corroborated against provider observation on Oct15 (not a historical snapshot); observed interval response SHA256=29dd64c96caf91dff61e94e408e5754fc0b6ade4af7bb285e6ca6f40e973d80b. Only already occurred sessions through Oct12 certified; raw prior close date retained. No synthetic base quote, no future halt extension."},"688496":{"start":"2026-09-11","through":"2026-10-12","source":"https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12595558&stockid=688496","corroboration":"https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29","verified_on":"2026-10-15","evidence":"Issuer suspension/resumption original independently read. Continuous interval corroborated against provider observation on Oct15 (not a historical snapshot); observed interval response SHA256=29dd64c96caf91dff61e94e408e5754fc0b6ade4af7bb285e6ca6f40e973d80b. Only already occurred sessions through Oct12 certified; raw prior close date retained. No synthetic base quote, no future halt extension."},"000016":{"start":"2026-09-04","through":"2026-10-12","source":"https://epaper.cs.com.cn/zgzqb/html/2026-09/28/nw.D110000zgzqb_20260928_5-B021.htm","corroboration":"https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29","verified_on":"2026-10-15","evidence":"Issuer suspension/resumption original independently read. Continuous interval corroborated against provider observation on Oct15 (not a historical snapshot); observed interval response SHA256=29dd64c96caf91dff61e94e408e5754fc0b6ade4af7bb285e6ca6f40e973d80b. Only already occurred sessions through Oct12 certified; raw prior close date retained. No synthetic base quote, no future halt extension."},"000668":{"start":"2026-10-08","through":"2026-10-12","source":"https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12634944","corroboration":"https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29","verified_on":"2026-10-15","evidence":"Issuer suspension/resumption original independently read. Continuous interval corroborated against provider observation on Oct15 (not a historical snapshot); observed interval response SHA256=29dd64c96caf91dff61e94e408e5754fc0b6ade4af7bb285e6ca6f40e973d80b. Only already occurred sessions through Oct12 certified; raw prior close date retained. No synthetic base quote, no future halt extension."},"002388":{"start":"2026-10-08","through":"2026-10-12","source":"https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12631200&stockid=002388","corroboration":"https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29","verified_on":"2026-10-15","evidence":"Issuer suspension/resumption original independently read. Continuous interval corroborated against provider observation on Oct15 (not a historical snapshot); observed interval response SHA256=29dd64c96caf91dff61e94e408e5754fc0b6ade4af7bb285e6ca6f40e973d80b. Only already occurred sessions through Oct12 certified; raw prior close date retained. No synthetic base quote, no future halt extension."},"002731":{"start":"2026-09-01","through":"2026-10-12","source":"https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12586056&stockid=002731","corroboration":"https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29","verified_on":"2026-10-15","evidence":"Issuer suspension/resumption original independently read. Continuous interval corroborated against provider observation on Oct15 (not a historical snapshot); observed interval response SHA256=29dd64c96caf91dff61e94e408e5754fc0b6ade4af7bb285e6ca6f40e973d80b. Only already occurred sessions through Oct12 certified; raw prior close date retained. No synthetic base quote, no future halt extension."},"002813":{"start":"2026-09-30","through":"2026-10-12","source":"https://disc.static.szse.cn/download/disc/disk03/finalpage/2026-10-14/6b255775-6b1f-4f4a-aa78-37830de08dc2.PDF","corroboration":"https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29","verified_on":"2026-10-15","evidence":"Issuer suspension/resumption original independently read. Continuous interval corroborated against provider observation on Oct15 (not a historical snapshot); observed interval response SHA256=29dd64c96caf91dff61e94e408e5754fc0b6ade4af7bb285e6ca6f40e973d80b. Only already occurred sessions through Oct12 certified; raw prior close date retained. No synthetic base quote, no future halt extension."},"002860":{"start":"2026-09-22","through":"2026-10-12","source":"https://static.cninfo.com.cn/finalpage/2026-10-14/1225604019.PDF","corroboration":"https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29","verified_on":"2026-10-15","evidence":"Issuer suspension/resumption original independently read. Continuous interval corroborated against provider observation on Oct15 (not a historical snapshot); observed interval response SHA256=29dd64c96caf91dff61e94e408e5754fc0b6ade4af7bb285e6ca6f40e973d80b. Only already occurred sessions through Oct12 certified; raw prior close date retained. No synthetic base quote, no future halt extension."}},"listing":{"301569":["2026-10-15","https://static.cninfo.com.cn/finalpage/2026-10-14/1225604190.PDF"],"920162":["2026-10-15","https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12639443&stockid=920162"]},"scope":"DATE_BOUNDED_SOURCE_FACTS_NOT_RULE_CHANGE"}


# Independently read Oct16; bounded past facts, not algorithm changes.
RECOVERED_OCT13={'listing': {'920196': ['2026-10-16', 'https://www.cfi.net.cn/p20261014001870.html']},
 'scope': 'EXACT_PAST_SOURCE_FACTS_ONLY',
 'verified_historical_suspensions': {'000016': {'corroboration': 'https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29',
                                                'evidence': 'Issuer original independently read; raw '
                                                            'SHA256=d3aa51c16722bb51f110b1bfd24522daf0642c0741770c928c45d05525864967. '
                                                            'Exact already occurred sessions through Oct13 only. '
                                                            'Continuous interval corroborated by Oct16 provider '
                                                            'observation '
                                                            'SHA256=00b65f1ac8794f13e5516cf0ac607e6ae21df621cc306cd3a782a34efaaa307d; '
                                                            'query date filter ignored, not a historical '
                                                            'snapshot. Retain actual prior close date and price; '
                                                            'do not synthesize Oct13 bars or extend to a future '
                                                            'date.',
                                                'source': 'https://epaper.cs.com.cn/zgzqb/html/2026-09/28/nw.D110000zgzqb_20260928_5-B021.htm',
                                                'start': '2026-09-04',
                                                'through': '2026-10-13',
                                                'verified_on': '2026-10-16'},
                                     '002388': {'corroboration': 'https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29',
                                                'evidence': 'Issuer original independently read; raw '
                                                            'SHA256=a9040f28e289795640074c00402fe17c552c6f572dc7cc65ee0d14045ab34b4c. '
                                                            'Exact already occurred sessions through Oct13 only. '
                                                            'Continuous interval corroborated by Oct16 provider '
                                                            'observation '
                                                            'SHA256=00b65f1ac8794f13e5516cf0ac607e6ae21df621cc306cd3a782a34efaaa307d; '
                                                            'query date filter ignored, not a historical '
                                                            'snapshot. Retain actual prior close date and price; '
                                                            'do not synthesize Oct13 bars or extend to a future '
                                                            'date.',
                                                'source': 'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12631200&stockid=002388',
                                                'start': '2026-10-08',
                                                'through': '2026-10-13',
                                                'verified_on': '2026-10-16'},
                                     '002717': {'corroboration': 'https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29',
                                                'evidence': 'Issuer original independently read; raw '
                                                            'SHA256=ab6c8834315556b9557e6cab23615d6f8ff0c6f8503973be61abdee677a89246. '
                                                            'Exact already occurred sessions through Oct13 only. '
                                                            'Continuous interval corroborated by Oct16 provider '
                                                            'observation '
                                                            'SHA256=00b65f1ac8794f13e5516cf0ac607e6ae21df621cc306cd3a782a34efaaa307d; '
                                                            'query date filter ignored, not a historical '
                                                            'snapshot. Retain actual prior close date and price; '
                                                            'do not synthesize Oct13 bars or extend to a future '
                                                            'date.',
                                                'source': 'https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12638692&stockid=002717',
                                                'start': '2026-10-13',
                                                'through': '2026-10-13',
                                                'verified_on': '2026-10-16'},
                                     '002731': {'corroboration': 'https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29',
                                                'evidence': 'Issuer original independently read; raw '
                                                            'SHA256=35ae9c69425220bf2e365466659583202c94e5565370c68f39101c6c21ca127a. '
                                                            'Exact already occurred sessions through Oct13 only. '
                                                            'Continuous interval corroborated by Oct16 provider '
                                                            'observation '
                                                            'SHA256=00b65f1ac8794f13e5516cf0ac607e6ae21df621cc306cd3a782a34efaaa307d; '
                                                            'query date filter ignored, not a historical '
                                                            'snapshot. Retain actual prior close date and price; '
                                                            'do not synthesize Oct13 bars or extend to a future '
                                                            'date.',
                                                'source': 'https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12586056&stockid=002731',
                                                'start': '2026-09-01',
                                                'through': '2026-10-13',
                                                'verified_on': '2026-10-16'},
                                     '002813': {'corroboration': 'https://disc.static.szse.cn/download/disc/disk03/finalpage/2026-10-14/6b255775-6b1f-4f4a-aa78-37830de08dc2.PDF',
                                                'evidence': 'Issuer original independently read; raw '
                                                            'SHA256=7b96242a1ccf84f1e5b3aebcacca50b02a44bd0aa7be2d81f7edcc30fffdd6e2. '
                                                            'Exact already occurred sessions through Oct13 only. '
                                                            'Issuer resumption announcement proves restart Oct14; '
                                                            'no later halt is inferred. Retain actual prior close '
                                                            'date and price; do not synthesize Oct13 bars or '
                                                            'extend to a future date.',
                                                'source': 'https://disc.static.szse.cn/download/disc/disk03/finalpage/2026-10-14/6b255775-6b1f-4f4a-aa78-37830de08dc2.PDF',
                                                'start': '2026-09-30',
                                                'through': '2026-10-13',
                                                'verified_on': '2026-10-16'},
                                     '002860': {'corroboration': 'https://static.cninfo.com.cn/finalpage/2026-10-14/1225604019.PDF',
                                                'evidence': 'Issuer original independently read; raw '
                                                            'SHA256=4c9fe6862db1119fbe4090f590ff7c00c76d791ece3be58543559a5c82238eee. '
                                                            'Exact already occurred sessions through Oct13 only. '
                                                            'Issuer resumption announcement proves restart Oct14; '
                                                            'no later halt is inferred. Retain actual prior close '
                                                            'date and price; do not synthesize Oct13 bars or '
                                                            'extend to a future date.',
                                                'source': 'https://static.cninfo.com.cn/finalpage/2026-10-14/1225604019.PDF',
                                                'start': '2026-09-22',
                                                'through': '2026-10-13',
                                                'verified_on': '2026-10-16'},
                                     '300773': {'corroboration': 'https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29',
                                                'evidence': 'Issuer original independently read; raw '
                                                            'SHA256=9d48e0dc1b03e02355e02688468ade9e821bbcdc386ccb1e382ee8caaedb6585. '
                                                            'Exact already occurred sessions through Oct13 only. '
                                                            'Continuous interval corroborated by Oct16 provider '
                                                            'observation '
                                                            'SHA256=00b65f1ac8794f13e5516cf0ac607e6ae21df621cc306cd3a782a34efaaa307d; '
                                                            'query date filter ignored, not a historical '
                                                            'snapshot. Retain actual prior close date and price; '
                                                            'do not synthesize Oct13 bars or extend to a future '
                                                            'date.',
                                                'source': 'https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12630709&stockid=300773',
                                                'start': '2026-10-08',
                                                'through': '2026-10-13',
                                                'verified_on': '2026-10-16'},
                                     '301314': {'corroboration': 'https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29',
                                                'evidence': 'Issuer original independently read; raw '
                                                            'SHA256=40b3ad9eb7b8c2b864562b3ef7d4e7e5e11d56f2f11e6fa14b45169e7163b463. '
                                                            'Exact already occurred sessions through Oct13 only. '
                                                            'Continuous interval corroborated by Oct16 provider '
                                                            'observation '
                                                            'SHA256=00b65f1ac8794f13e5516cf0ac607e6ae21df621cc306cd3a782a34efaaa307d; '
                                                            'query date filter ignored, not a historical '
                                                            'snapshot. Retain actual prior close date and price; '
                                                            'do not synthesize Oct13 bars or extend to a future '
                                                            'date.',
                                                'source': 'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12638305&stockid=301314',
                                                'start': '2026-10-13',
                                                'through': '2026-10-13',
                                                'verified_on': '2026-10-16'},
                                     '600293': {'corroboration': 'https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29',
                                                'evidence': 'Issuer original independently read; raw '
                                                            'SHA256=cf168d2fd1a931172c3febbc5981ee71ed8af9db6e7526b581e8883d201c6fdf. '
                                                            'Exact already occurred sessions through Oct13 only. '
                                                            'Continuous interval corroborated by Oct16 provider '
                                                            'observation '
                                                            'SHA256=00b65f1ac8794f13e5516cf0ac607e6ae21df621cc306cd3a782a34efaaa307d; '
                                                            'query date filter ignored, not a historical '
                                                            'snapshot. Retain actual prior close date and price; '
                                                            'do not synthesize Oct13 bars or extend to a future '
                                                            'date.',
                                                'source': 'https://epaper.cs.com.cn/zgzqb/html/2026-10/13/nw.D110000zgzqb_20261013_3-A08.htm',
                                                'start': '2026-09-29',
                                                'through': '2026-10-13',
                                                'verified_on': '2026-10-16'},
                                     '600825': {'corroboration': 'https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29',
                                                'evidence': 'Issuer original independently read; raw '
                                                            'SHA256=7ed3554376aa8c473180008e06d0f9420016d571d3cf0c5b54af3617b56947a2. '
                                                            'Exact already occurred sessions through Oct13 only. '
                                                            'Continuous interval corroborated by Oct16 provider '
                                                            'observation '
                                                            'SHA256=00b65f1ac8794f13e5516cf0ac607e6ae21df621cc306cd3a782a34efaaa307d; '
                                                            'query date filter ignored, not a historical '
                                                            'snapshot. Retain actual prior close date and price; '
                                                            'do not synthesize Oct13 bars or extend to a future '
                                                            'date.',
                                                'source': 'https://static.cninfo.com.cn/finalpage/2026-10-13/1225602155.PDF',
                                                'start': '2026-10-13',
                                                'through': '2026-10-13',
                                                'verified_on': '2026-10-16'},
                                     '601059': {'corroboration': 'https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29',
                                                'evidence': 'Issuer original independently read; raw '
                                                            'SHA256=791845df2a7b0a66c2d07832a18a8485bda7369e048981a3cc0a9750da5f18e3. '
                                                            'Exact already occurred sessions through Oct13 only. '
                                                            'Continuous interval corroborated by Oct16 provider '
                                                            'observation '
                                                            'SHA256=00b65f1ac8794f13e5516cf0ac607e6ae21df621cc306cd3a782a34efaaa307d; '
                                                            'query date filter ignored, not a historical '
                                                            'snapshot. Retain actual prior close date and price; '
                                                            'do not synthesize Oct13 bars or extend to a future '
                                                            'date.',
                                                'source': 'https://paper.cnstock.com/html/2026-09/09/content_2266558.htm',
                                                'start': '2026-09-15',
                                                'through': '2026-10-13',
                                                'verified_on': '2026-10-16'},
                                     '601198': {'corroboration': 'https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29',
                                                'evidence': 'Issuer original independently read; raw '
                                                            'SHA256=f759b1f9d4857fc6697705a5d43734e37eb58be5d39f1abcef279c549bcbc97f. '
                                                            'Exact already occurred sessions through Oct13 only. '
                                                            'Continuous interval corroborated by Oct16 provider '
                                                            'observation '
                                                            'SHA256=00b65f1ac8794f13e5516cf0ac607e6ae21df621cc306cd3a782a34efaaa307d; '
                                                            'query date filter ignored, not a historical '
                                                            'snapshot. Retain actual prior close date and price; '
                                                            'do not synthesize Oct13 bars or extend to a future '
                                                            'date.',
                                                'source': 'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12588671&stockid=601198',
                                                'start': '2026-09-15',
                                                'through': '2026-10-13',
                                                'verified_on': '2026-10-16'},
                                     '688496': {'corroboration': 'https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%272026-10-12%27%29',
                                                'evidence': 'Issuer original independently read; raw '
                                                            'SHA256=46ddc3bf25937ddbf4f9916ec5a84e2881312e911587f90292a0ccb35014c554. '
                                                            'Exact already occurred sessions through Oct13 only. '
                                                            'Continuous interval corroborated by Oct16 provider '
                                                            'observation '
                                                            'SHA256=00b65f1ac8794f13e5516cf0ac607e6ae21df621cc306cd3a782a34efaaa307d; '
                                                            'query date filter ignored, not a historical '
                                                            'snapshot. Retain actual prior close date and price; '
                                                            'do not synthesize Oct13 bars or extend to a future '
                                                            'date.',
                                                'source': 'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12595558&stockid=688496',
                                                'start': '2026-09-11',
                                                'through': '2026-10-13',
                                                'verified_on': '2026-10-16'}}}
