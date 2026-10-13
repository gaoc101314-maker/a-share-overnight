"""Offline diagnostic candidate. Inject existing pm01.get; no network on import.
collect returns (rows, diagnostics), not production certification. Retain original
refresh/freeze/server date, full-market, company-input and 300-second gates.
Logical request times include get's existing retries, not individual attempts.
"""
import datetime as dt
import hashlib
import json
import math
import re
import copy
import base64
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlencode
CN=dt.timezone(dt.timedelta(hours=8))

SITE_ORIGIN='https://a-share-reality-pm03.gaoc101314.chatgpt.site'

def provider_request_url(url,params=None):
    p={} if params is None else params
    if not isinstance(p,dict) or any(not isinstance(k,str) or type(v) not in (str,int) for k,v in p.items()):
        raise ValueError('PROVIDER_ENDPOINT_NOT_SUPPORTED')
    p={k:str(v) for k,v in p.items()}
    symbol_pattern=r'(?:sh|sz|bj)[0-9]{6}'
    quote=re.fullmatch(r'https://(?:qt|web\.sqt)\.gtimg\.cn/q=('+symbol_pattern+r'(?:,'+symbol_pattern+r')*)',url)
    ok=False
    if quote and not p:
        codes=quote[1].split(',');ok=len(codes)<=80 and len(set(codes))==len(codes)
    sina='https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.'
    if url==sina+'getHQNodeStockCount':ok=p=={'node':'hs_a'}
    if url==sina+'getHQNodeData':
        ok=set(p)=={'node','page','num','sort','asc'} and p['node']=='hs_a' and p['num']=='80' and p['sort']=='symbol' and p['asc']=='1' and re.fullmatch(r'[1-9][0-9]*',p['page']) is not None and 1<=int(p['page'])<=200
    raw={'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get':True,
         'https://web.ifzq.gtimg.cn/appstock/app/kline/kline':False,
         'https://proxy.finance.qq.com/ifzqgtimg/appstock/app/fqkline/get':True}
    if url in raw:ok=set(p)=={'param'} and re.fullmatch(symbol_pattern+r',day,,,(?:80|100|240)'+(',' if raw[url] else ''),p['param']) is not None
    if url=='https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20pm01=/CN_MarketDataService.getKLineData':
        ok=set(p)=={'symbol','scale','ma','datalen'} and re.fullmatch(symbol_pattern,p['symbol']) is not None and p['scale']=='240' and p['ma']=='no' and p['datalen'] in ('80','100','240')
    if url in ('https://web.ifzq.gtimg.cn/appstock/app/minute/query','https://proxy.finance.qq.com/ifzqgtimg/appstock/app/minute/query'):
        ok=set(p)=={'code'} and re.fullmatch(symbol_pattern,p['code']) is not None
    if not ok:raise ValueError('PROVIDER_ENDPOINT_NOT_SUPPORTED')
    return url+('?' + urlencode(sorted(p.items())) if p else '')

def site_transport(get_json, origin, token):
    """get_json(method,url,headers,json,timeout,allow_redirects)->decoded dict.
    Inject requests.Session.request wrapper that raises on gateway HTTP errors.
    Supports only provider_request_url's fixed quote/universe/raw/minute recipes;
    other calls stay on the original transport, never an arbitrary proxy.
    Token stays closure-local, never persisted or included in exception text.
    """
    if origin!=SITE_ORIGIN:raise ValueError('SITE_ORIGIN_REJECTED')
    if not isinstance(token,str) or not token or '\r' in token or '\n' in token:
        raise ValueError('SITE_TOKEN_INVALID')
    def get(url,params=None):
        request_url=provider_request_url(url,params)
        try:
            result=get_json('POST',origin+'/maintenance/provider-response',
                headers={'OAI-Sites-Authorization':'Bearer '+token},
                json={'url':url,'params':params or None},timeout=(3,12),allow_redirects=False)
            if not isinstance(result,dict) or set(result)!={'request_url','status','body_base64','sha256','source_fetched_at'}:
                raise ValueError()
            if result['request_url']!=request_url or type(result['status']) is not int or result['status']!=200:
                raise ValueError()
            encoded=result['body_base64']
            if not isinstance(encoded,str) or len(encoded)>2796204:raise ValueError()
            raw=base64.b64decode(encoded,validate=True)
            if len(raw)>2*1024*1024 or hashlib.sha256(raw).hexdigest()!=result['sha256']:raise ValueError()
            stamp=dt.datetime.fromisoformat(result['source_fetched_at'])
            if stamp.tzinfo is None:raise ValueError()
        except Exception:
            raise ValueError('SITE_PROVIDER_RESPONSE_REJECTED') from None
        class RawResponse:
            status_code=200
            content=raw
            encoding='utf-8'
            def json(self):return json.loads(self.content.decode(self.encoding))
            def raise_for_status(self):return None
            @property
            def text(self):return self.content.decode(self.encoding)
        return RawResponse()
    return get

SERVER_TRANSPORT_JS=r'''
// Fixed public source recipes only; existing private gateway auth required.
export function providerRequestURL(url,params) {
  if(typeof url!=='string'||(params!==null&&(typeof params!=='object'||Array.isArray(params))))throw Error('INPUT');
  const original=params??{},p={};
  for(const [k,v] of Object.entries(original)){if(typeof v!=='string'&&!(typeof v==='number'&&Number.isSafeInteger(v)))throw Error('PARAM');p[k]=String(v);}
  const keys=(names)=>Object.keys(p).sort().join(',')===names.split(',').sort().join(',');
  const sym=/^(?:sh|sz|bj)[0-9]{6}$/;
  const m=/^https:\/\/(?:qt|web\.sqt)\.gtimg\.cn\/q=((?:sh|sz|bj)[0-9]{6}(?:,(?:sh|sz|bj)[0-9]{6})*)$/.exec(url);
  let ok=false;
  if(m&&keys('')){const codes=m[1].split(',');ok=codes.length<=80&&new Set(codes).size===codes.length;}
  const sina='https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.';
  if(url===sina+'getHQNodeStockCount')ok=keys('node')&&p.node==='hs_a';
  if(url===sina+'getHQNodeData')ok=keys('node,page,num,sort,asc')&&p.node==='hs_a'&&p.num==='80'&&p.sort==='symbol'&&p.asc==='1'&&/^[1-9][0-9]*$/.test(p.page)&&Number(p.page)<=200;
  const raw={'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get':true,'https://web.ifzq.gtimg.cn/appstock/app/kline/kline':false,'https://proxy.finance.qq.com/ifzqgtimg/appstock/app/fqkline/get':true};
  if(Object.hasOwn(raw,url))ok=keys('param')&&(raw[url]?/^(?:sh|sz|bj)[0-9]{6},day,,,(?:80|100|240),$/:/^(?:sh|sz|bj)[0-9]{6},day,,,(?:80|100|240)$/).test(p.param);
  if(url==='https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20pm01=/CN_MarketDataService.getKLineData')ok=keys('symbol,scale,ma,datalen')&&sym.test(p.symbol)&&p.scale==='240'&&p.ma==='no'&&['80','100','240'].includes(p.datalen);
  if(['https://web.ifzq.gtimg.cn/appstock/app/minute/query','https://proxy.finance.qq.com/ifzqgtimg/appstock/app/minute/query'].includes(url))ok=keys('code')&&sym.test(p.code);
  if(!ok)throw Error('ENDPOINT');
  const query=new URLSearchParams();for(const k of Object.keys(p).sort())query.append(k,p[k]);
  return url+(query.size?'?'+query.toString():'');
}
export async function providerResponse(request,fetchImpl=fetch) {
  const reply=(data,status)=>new Response(JSON.stringify(data),{status,headers:{'Content-Type':'application/json','Cache-Control':'no-store'}});
  const bounded=async(stream,limit)=>{
    if(!stream)return new Uint8Array();
    const reader=stream.getReader(),chunks=[];let size=0;
    try {while(true){const {done,value}=await reader.read();if(done)break;size+=value.length;if(size>limit){await reader.cancel();throw Error('SIZE');}chunks.push(value);}}
    finally {reader.releaseLock();}
    const bytes=new Uint8Array(size);let at=0;for(const c of chunks){bytes.set(c,at);at+=c.length;}return bytes;
  };
  if(request.method!=='POST')return reply({error:'METHOD'},405);
  let input,requestURL;
  try {
    const bytes=await bounded(request.body,8192);
    input=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));
    if(!input||Array.isArray(input)||Object.keys(input).sort().join(',')!=='params,url')throw Error('INPUT');
    requestURL=providerRequestURL(input.url,input.params);
  } catch {return reply({error:'PROVIDER_REQUEST_REJECTED'},400);}
  const controller=new AbortController();const timer=setTimeout(()=>controller.abort(),8000);
  try {
    const response=await fetchImpl(requestURL,{method:'GET',redirect:'manual',signal:controller.signal,headers:{'User-Agent':'Mozilla/5.0','Referer':'https://gu.qq.com/'}});
    if(response.status!==200){await response.body?.cancel();return reply({error:'UPSTREAM_HTTP_STATUS',status:response.status},502);}
    const bytes=await bounded(response.body,2097152);
    const digest=await crypto.subtle.digest('SHA-256',bytes);
    const sha256=Array.from(new Uint8Array(digest),x=>x.toString(16).padStart(2,'0')).join('');
    let binary='';for(let i=0;i<bytes.length;i+=8192)binary+=String.fromCharCode(...bytes.subarray(i,i+8192));
    return reply({request_url:requestURL,status:response.status,body_base64:btoa(binary),sha256,source_fetched_at:new Date().toISOString()},200);
  } catch {return reply({error:controller.signal.aborted?'UPSTREAM_TIMEOUT':'UPSTREAM_FAILURE'},controller.signal.aborted?504:502);}
  finally {clearTimeout(timer);}
}
'''

RAW_HISTORY_SOURCES={'TENCENT_RAW_DAILY','TENCENT_RAW_KLINE_BACKUP',
    'TENCENT_PROXY_RAW_BACKUP','SINA_RAW_DAILY_BACKUP','EASTMONEY_RAW_DAILY'}

def rank_evidence_provider(snapshot, get, context, cached_history=(), mode='PRODUCTION', asof=None):
    """Explicit injected acquisition, never called on import.
    context={universe_codes,trading_dates,listing_evidence?,suspension_evidence?}.
    cached_history iterable: {code,source,adjustment:'UNADJUSTED',closes,
    evidence}; evidence references the original response or verified cache receipt.
    Caller reads original successful cache and attaches code from its verified
    cache key; do not infer identity or raw-price mode from an arbitrary file.
    Dates stay actual; source labels alone do not establish adjustment evidence.
    """
    d0=snapshot['d0']; codes=context['universe_codes']; daily={c:{} for c in codes}
    events=[]
    def retain(code, closes, source, proof):
        for date,price in closes.items():
            try:valid_date=dt.date.fromisoformat(date).isoformat()==date and date<d0
            except (ValueError,TypeError):valid_date=False
            if not valid_date or isinstance(price,bool) or not isinstance(price,(int,float)) or not math.isfinite(price) or price<=0:
                raise ValueError('RAW_CLOSE_INVALID:'+code+':'+str(date))
            fact={'close':price,'source_date':date,'adjustment':'UNADJUSTED',
                  'source':source,'evidence':proof}
            old=daily[code].get(date)
            if old and old['close']!=price:raise ValueError('RAW_CLOSE_CONFLICT:'+code+':'+date)
            daily[code][date]=fact
    for record in cached_history:
        c=record.get('code'); source=record.get('source')
        if c not in daily or source not in RAW_HISTORY_SOURCES or record.get('adjustment')!='UNADJUSTED':
            raise ValueError('CACHE_IDENTITY_SOURCE_OR_ADJUSTMENT_INVALID')
        proof=record.get('evidence')
        if not isinstance(proof,str) or not proof.strip() or proof in ('UNKNOWN','待验证'):
            raise ValueError('CACHE_EVIDENCE_REQUIRED')
        retain(c,record['closes'],source,proof)
    def covered(c):
        if all(b in daily[c] for b in snapshot['base_dates'].values()):return True
        # Reuse exact selection/evidence checks. A one-code probe is coverage
        # only, never Top50 certification; no synthetic quotes or timestamps.
        result=period_rankings([snapshot['rows'][c]] if c in snapshot.get('rows',{}) else [],
            {c:daily[c]},snapshot['base_dates'],context['trading_dates'],d0,
            asof or snapshot.get('snapshot_time') or dt.datetime.now(CN).isoformat(),[c],
            listing_evidence=context.get('listing_evidence'),
            suspension_evidence=context.get('suspension_evidence'),mode=mode)
        return all(not p['missing'] for p in result['periods'].values())
    def acquire(c):
        if covered(c):return
        recipes=[('https://web.ifzq.gtimg.cn/appstock/app/newfqkline/get','TENCENT_RAW_DAILY',{'param':symbol(c)+',day,,,80,'}),
          ('https://web.ifzq.gtimg.cn/appstock/app/kline/kline','TENCENT_RAW_KLINE_BACKUP',{'param':symbol(c)+',day,,,80'}),
          ('https://proxy.finance.qq.com/ifzqgtimg/appstock/app/fqkline/get','TENCENT_PROXY_RAW_BACKUP',{'param':symbol(c)+',day,,,80,'}),
          ('https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20pm01=/CN_MarketDataService.getKLineData','SINA_RAW_DAILY_BACKUP',{'symbol':symbol(c),'scale':240,'ma':'no','datalen':80})]
        for url,source,params in recipes:
            event={'code':c,'url':url,'params':copy.deepcopy(params),'source':source,
                   'started_at':dt.datetime.now(CN).isoformat()}
            try:
                response=get(url,params)
                event['http_status']=getattr(response,'status_code',None)
                event['response_sha256']=hashlib.sha256(response.content).hexdigest()
                if event['http_status']!=200:raise ValueError('HISTORY_HTTP_STATUS')
                if source=='SINA_RAW_DAILY_BACKUP':
                    text=response.content.decode('utf-8')
                    match=re.search(r'var pm01=\((\[.*\])\);?\s*$',text,re.S)
                    if not match:raise ValueError('SINA_DAILY_FORMAT')
                    bars=json.loads(match.group(1)); pairs=[(r['day'][:10],float(r['close'])) for r in bars]
                else:
                    data=response.json()['data'][symbol(c)]
                    # Raw day only: never substitute qfqday/hfqday.
                    pairs=[(r[0],float(r[2])) for r in data.get('day',[])]
                if len({d for d,p in pairs})!=len(pairs):raise ValueError('DAILY_DUPLICATE_DATE')
                event['response_dates']=[d for d,p in pairs]
                if any(d>d0 for d,p in pairs):raise ValueError('FUTURE_HISTORY_DATE')
                # Provider can include D0; it is not a prior-day basis cache.
                closes={d:p for d,p in pairs if d<d0}
                retain(c,closes,source,json.dumps({'url':url,'params':params,
                    'response_sha256':event['response_sha256']},sort_keys=True))
                event['retained_dates']=sorted(closes);event['status']='PARTIAL_RETAINED' if closes else 'EMPTY'
            except Exception as exc:
                if isinstance(exc,ValueError) and str(exc).startswith('RAW_CLOSE_CONFLICT:'):raise
                event.update(status='FAILED',exception_type=type(exc).__name__,exception_message=str(exc))
            finally:
                event['finished_at']=dt.datetime.now(CN).isoformat();events.append(event)
            if covered(c):break
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(acquire,codes))
    events.sort(key=lambda event:(event['code'],event['started_at']))
    return {**copy.deepcopy(context),'daily_prices':daily,'acquisition_diagnostics':events}

def refresh_with_evidence(pm01, context, cached_history=(), mode='PRODUCTION'):
    """run.py bridge: existing refresh/get/clock; explicit trusted context only."""
    snapshot=pm01.refresh()
    evidence=rank_evidence_provider(snapshot,pm01.get,context,cached_history,
        mode=mode,asof=pm01.now_cn().isoformat())
    return adapt_refresh(snapshot,evidence,pm01.now_cn().isoformat(),mode=mode)

def adapt_refresh(snapshot, evidence, asof, mode='PRODUCTION'):
    """Pure replacement for the ranking stage; never collects or publishes.

    evidence: {universe_codes, trading_dates, daily_prices,
               listing_evidence?, suspension_evidence?}.
    Supply the independently captured full universe, original parsed raw daily
    facts and verified exchange calendar. Never derive universe from successes.
    daily_prices and optional evidence use period_rankings' existing schema.
    Default PRODUCTION keeps original gates. Explicit CLOSE_MAINTENANCE is
    research-only and requires caller's original per-quote close_evidence.
    """
    s=copy.deepcopy(snapshot)
    codes=evidence['universe_codes']
    rows=s.get('rows',{})
    if len(codes)!=s.get('market_count') or set(rows)!=set(codes):
        raise ValueError('FULL_MARKET_QUOTE_INCOMPLETE')
    if any(c!=r.get('code') for c,r in rows.items()):
        raise ValueError('QUOTE_MAP_IDENTITY_INVALID')
    report=period_rankings(list(rows.values()),evidence['daily_prices'],
        s['base_dates'],evidence['trading_dates'],s['d0'],asof,codes,
        listing_evidence=evidence.get('listing_evidence'),
        suspension_evidence=evidence.get('suspension_evidence'),mode=mode)
    s.setdefault('trace',{})['PER_PERIOD_ADAPTER']={
        'acquisition_diagnostics':copy.deepcopy(evidence.get('acquisition_diagnostics',[])),
        'original_errors':copy.deepcopy(s.get('errors',[])),
        'original_history_failures':copy.deepcopy(s.get('history_failures',{})),
        'original_same_snapshot':s.get('same_snapshot'),
        'report':copy.deepcopy(report)}
    for n,p in report['periods'].items():
        s['top'][n]=[{**copy.deepcopy(rows[r['code']]),
            'period_pct':r['period_pct'],'rank':r['rank'],
            'history_source':r['base_source'],'base_price_evidence':copy.deepcopy(r)}
            for r in p['top']]
        s['pool_status'][n]='PASS' if p['certified'] else 'FAIL'
    # Only this precise aggregate history error is superseded by complete
    # per-period evidence. Transport/calendar/market failures remain blocking.
    complete=all(p['certified'] for p in report['periods'].values())
    if complete:
        expected='HISTORY_MISSING:'+str(len(s.get('history_failures',{})))
        s['errors']=[e for e in s.get('errors',[]) if e!=expected]
    s['history_failures']={c:{n:[m for m in p['missing'] if m['code']==c]
        for n,p in report['periods'].items() if any(m['code']==c for m in p['missing'])}
        for c in sorted({m['code'] for p in report['periods'].values() for m in p['missing']})}
    s['same_snapshot']='PASS' if complete and not s.get('errors') else 'FAIL'
    if mode=='CLOSE_MAINTENANCE':
        s['research_mode']='YES'
        s['not_live_production']=True
        s['fresh']=False
    # Do not renew capture time, fresh, snapshot_id, prices, or company pools.
    return s

def refresh_for_production(refresh, evidence_provider, clock):
    """Injected existing refresh and trusted evidence acquisition; no defaults.
    evidence_provider(snapshot) must return the above verified input contract.
    Exceptions propagate unchanged. Production caller still runs original freeze.
    """
    snapshot=refresh()
    return adapt_refresh(snapshot,evidence_provider(copy.deepcopy(snapshot)),clock().isoformat())

def symbol(code):
    return ('bj' if code.startswith(('4','8','92')) else 'sh' if code.startswith('6') else 'sz')+code

class QuoteFailure(ValueError):
    def __init__(self,diagnostics):
        self.diagnostics=diagnostics
        super().__init__('QUOTE_PROVIDERS_FAILED:'+json.dumps(diagnostics,ensure_ascii=False))

def collect(codes,get,expected_date=None,now=None):
    now=now or (lambda:dt.datetime.now(CN))
    if not codes or len(set(codes))!=len(codes) or any(not isinstance(c,str) or not re.fullmatch(r'[0-9]{6}',c) for c in codes):
        raise ValueError('INPUT_CODES_INVALID')
    diagnostics=[]
    def attempt(provider,url,params,parse,expected):
        e={'provider':provider,'url':url,'params':params,'started_at':now().isoformat(),
           'expected_date':expected_date,'requested_count':len(expected),'returned_count':0,
           'date_distribution':{},'raw_time_fields':[],'timing_scope':'LOGICAL_GET_INCLUDING_EXISTING_RETRIES'}
        try:
            response=get(url,params) if params is not None else get(url)
            e['http_status']=getattr(response,'status_code',None)
            e['response_sha256']=hashlib.sha256(response.content).hexdigest()
            rows=parse(response,e)
            e['returned_count']=len(rows)
            for r in rows:
                day=r['source_timestamp'][:10]
                e['date_distribution'][day]=e['date_distribution'].get(day,0)+1
            if not rows:raise ValueError('QUOTE_EMPTY_RETURN')
            if len(rows)!=len(set(r['code'] for r in rows)):raise ValueError('QUOTE_DUPLICATE_CODE')
            if {r['code'] for r in rows}!=set(expected):raise ValueError('QUOTE_BATCH_INCOMPLETE')
            e['status']='OLD_DATE' if expected_date and any(r['source_timestamp'][:10]!=expected_date for r in rows) else 'PARSED'
            return rows
        except Exception as exc:
            e.update(status='FAILED',exception_type=type(exc).__name__,exception_message=str(exc))
            raise
        finally:
            e['finished_at']=now().isoformat();diagnostics.append(e)
    def parse_tencent(response,e):
        rows=[]
        for sym,body in re.findall(r'v_((?:sh|sz|bj)[0-9]{6})="([^"]*)"',response.content.decode('gb18030')):
            a=body.split('~')
            e['raw_time_fields'].append({'code':sym[2:],'field':'30','value':a[30] if len(a)>30 else None})
            stamp=dt.datetime.strptime(a[30],'%Y%m%d%H%M%S').replace(tzinfo=CN)
            price,pct=float(a[3]),float(a[32])
            def number(i):
                try:
                    v=float(a[i]);return v if math.isfinite(v) else 'UNKNOWN'
                except (ValueError,IndexError):return 'UNKNOWN'
            if not all(math.isfinite(v) for v in (price,pct)):raise ValueError('INVALID_QUOTE')
            amount=number(37)
            rows.append(dict(code=sym[2:],name=a[1],price=price,day_pct=pct,prev_close=number(4),
                day_high=number(33),day_low=number(34),limit_up_price=number(47),turnover=number(38),
                amount=amount*10000 if isinstance(amount,float) else 'UNKNOWN',source_timestamp=stamp.isoformat(),
                quote_source=e['provider'],primary_source='PASS' if e['provider']=='https://qt.gtimg.cn' else 'FAIL'))
        return rows
    for host in ('https://qt.gtimg.cn','https://web.sqt.gtimg.cn'):
        try:return attempt(host,host+'/q='+','.join(symbol(c) for c in codes),None,parse_tencent,codes),diagnostics
        except Exception:pass
    rows=[]
    for code in codes:
        params={'secid':('1' if code.startswith('6') else '0')+'.'+code,'fields':'f43,f57,f58,f170,f86','fltt':2}
        def parse_em(response,e):
            d=response.json()['data']
            if not d:raise ValueError('QUOTE_EMPTY_RETURN')
            e['raw_time_fields'].append({'code':code,'field':'f86','value':d.get('f86')})
            price,pct=float(d['f43']),float(d['f170'])
            if not all(math.isfinite(v) for v in (price,pct)):raise ValueError('INVALID_QUOTE')
            return [dict(code=d['f57'],name=d['f58'],price=price,day_pct=pct,
                source_timestamp=dt.datetime.fromtimestamp(d['f86'],CN).isoformat(),
                quote_source='EASTMONEY_FALLBACK',primary_source='FAIL')]
        try:rows.extend(attempt('EASTMONEY_FALLBACK','https://push2.eastmoney.com/api/qt/stock/get',params,parse_em,[code]))
        except Exception:raise QuoteFailure(diagnostics)
    return rows,diagnostics

def history_probe(code,d0,bases,history_call,get,now=None):
    """Run the EXISTING history recipe with a tracing get supplied by the caller.
    history_call(code,d0,bases,traced_get) must use original endpoints, params,
    parsing and fallback order. This function chooses no provider or adjustment.
    Logical get attempts include original retries. No automatic network execution.
    Raw payload dates are observations; only original parsed closes are certified
    as available dates. Missing bases are never filled or inferred.
    """
    if not isinstance(code,str) or not re.fullmatch(r'[0-9]{6}',code):
        raise ValueError('CODE_INVALID')
    required=list(bases.values()) if isinstance(bases,dict) else list(bases)
    now=now or (lambda:dt.datetime.now(CN))
    requests=[]
    def traced_get(url,params=None):
        event={'code':code,'url':url,'params':copy.deepcopy(params),'started_at':now().isoformat(),
               'timing_scope':'LOGICAL_GET_INCLUDING_EXISTING_RETRIES','required_base_dates':required,
               'response_dates':[], 'response_dates_scope':'RAW_TEXT_DATE_TOKENS_NOT_CERTIFICATION'}
        try:
            r=get(url,params) if params is not None else get(url)
            event['http_status']=getattr(r,'status_code',None)
            event['response_sha256']=hashlib.sha256(r.content).hexdigest()
            # Observation only: unchanged raw payload is still parsed by original code.
            event['response_dates']=sorted(set(re.findall(r'(?<!\d)\d{4}-\d{2}-\d{2}(?!\d)',r.content.decode('utf-8',errors='replace'))))
            event['status']='RETURNED'
            return r
        except Exception as exc:
            event.update(status='FAILED',exception_type=type(exc).__name__,exception_message=str(exc))
            raise
        finally:
            event['finished_at']=now().isoformat();requests.append(event)
    report={'code':code,'d0':d0,'required_base_dates':copy.deepcopy(required),'requests':requests,
            'started_at':now().isoformat(),'available_dates':[],'missing_base_dates':copy.deepcopy(required),
            'result_source':'UNKNOWN','status':'FAILED','production_certified':False}
    try:
        result=history_call(code,d0,bases,traced_get)
        closes=result.get('closes') if isinstance(result,dict) else None
        if not isinstance(closes,dict):raise ValueError('ORIGINAL_HISTORY_CLOSES_REQUIRED')
        report['available_dates']=sorted(closes)
        report['missing_base_dates']=[day for day in required if day not in closes]
        report['result_source']=result.get('source','UNKNOWN')
        report['original_result']=copy.deepcopy(result)
        report['status']='MISSING_BASE_DATES' if report['missing_base_dates'] else 'PARSED'
    except Exception as exc:
        report.update(exception_type=type(exc).__name__,exception_message=str(exc))
    report['finished_at']=now().isoformat()
    return report

def summarize_refresh(snapshot,history_reports=()):
    """Preserve original failure codes/values and gates; never certify a snapshot."""
    reports=list(history_reports)
    counts={}
    for r in snapshot.get('rows',{}).values():
        day=str(r.get('source_timestamp','UNKNOWN'))[:10]
        counts[day]=counts.get(day,0)+1
    failures=copy.deepcopy(snapshot.get('history_failures',{}))
    probe_failures=[r['code'] for r in reports if r.get('status')!='PARSED']
    return {'scope':'DIAGNOSTIC_ONLY','d0':snapshot.get('d0'),'snapshot_time':snapshot.get('snapshot_time'),
            'market_count':snapshot.get('market_count'),'returned_quote_count':len(snapshot.get('rows',{})),
            'quote_date_distribution':counts,'errors':copy.deepcopy(snapshot.get('errors',[])),
            'history_failures':failures,'base_dates':copy.deepcopy(snapshot.get('base_dates',{})),
            'trace':copy.deepcopy(snapshot.get('trace',{})),'pool_status':copy.deepcopy(snapshot.get('pool_status',{})),
            'same_snapshot':snapshot.get('same_snapshot'),'fresh':snapshot.get('fresh'),
            'history_reports':copy.deepcopy(reports),
            'failed_codes':sorted(set(failures)|set(probe_failures)),
            'production_certified':False}

def period_rankings(quotes,daily_prices,base_dates,trading_dates,d0,asof,universe_codes,upper_bounds=None,
                    listing_evidence=None,suspension_evidence=None,mode='PRODUCTION'):
    """Pure per-period verification, not a company-pool maintenance action.
    daily_prices[code][day]={close, source_date, adjustment:'UNADJUSTED',
    source, evidence}; provenance is caller-supplied, never authenticated here.
    trading_dates is the caller's official verified calendar, ending at d0.
    No known upstream upper-bound contract was supplied: bounds cannot certify.
    """
    def day(v):
        if not isinstance(v,str) or dt.date.fromisoformat(v).isoformat()!=v:
            raise ValueError('DATE_INVALID')
        return v
    day(d0)
    dates=[day(x) for x in trading_dates]
    if len(dates)<11 or dates!=sorted(set(dates)) or dates[-1]!=d0:
        raise ValueError('VERIFIED_TRADING_CALENDAR_REQUIRED')
    if set(base_dates)!={'3','5','10'} or any(base_dates[str(n)]!=dates[-1-n] for n in (3,5,10)):
        raise ValueError('EXACT_TRADING_BASE_REQUIRED')
    if not universe_codes or len(set(universe_codes))!=len(universe_codes) or any(not isinstance(c,str) or not re.fullmatch(r'[0-9]{6}',c) for c in universe_codes):
        raise ValueError('UNIVERSE_CODES_INVALID')
    qmap={}
    for q in quotes:
        c=q.get('code')
        if c in qmap or c not in universe_codes:raise ValueError('QUOTE_IDENTITY_INVALID')
        qmap[c]=q
    clock=dt.datetime.fromisoformat(asof)
    if clock.tzinfo is None or clock.astimezone(CN).date().isoformat()!=d0:
        raise ValueError('ASOF_DATE_INVALID')
    if mode not in ('PRODUCTION','CLOSE_MAINTENANCE'):raise ValueError('MODE_INVALID')
    def provenance(x):
        return isinstance(x,dict) and all(isinstance(x.get(k),str) and x[k].strip() and x[k] not in ('UNKNOWN','待验证') for k in ('source','evidence'))
    def valid_fact(f,date):
        return provenance(f) and f.get('source_date')==date and f.get('adjustment')=='UNADJUSTED' and isinstance(f.get('close'),(int,float)) and not isinstance(f['close'],bool) and math.isfinite(f['close']) and f['close']>0
    out={}
    for n in ('3','5','10'):
        ranked=[];missing=[];excluded=[]
        for code in universe_codes:
            listing=(listing_evidence or {}).get(code)
            if listing is not None:
                try:
                    ld=day(listing.get('listing_date'))
                    good=provenance(listing) and listing.get('code')==code and ld<=d0
                except (ValueError,TypeError,AttributeError):good=False
                if not good:
                    missing.append({'code':code,'period':n,'base_date':base_dates[n],'reason':'LISTING_EVIDENCE_INVALID'});continue
                if ld>base_dates[n]:
                    excluded.append({'code':code,'period':n,'reason':'LISTED_AFTER_BASE_DATE','listing_evidence':copy.deepcopy(listing)});continue
            q=qmap.get(code)
            reason=None
            if not q:reason='CURRENT_QUOTE_MISSING'
            else:
                try:
                    t=dt.datetime.fromisoformat(q['source_timestamp'])
                    if t.tzinfo is None or t.astimezone(CN).date().isoformat()!=d0:
                        reason='CURRENT_DATE_OR_FRESHNESS_INVALID'
                    elif mode=='PRODUCTION' and not -30<=(clock-t).total_seconds()<=300:
                        reason='CURRENT_DATE_OR_FRESHNESS_INVALID'
                    elif mode=='CLOSE_MAINTENANCE':
                        ce=q.get('close_evidence')
                        if t.astimezone(CN).time()<dt.time(15) or t>clock or clock.astimezone(CN).time()<dt.time(15) or not provenance(ce) or ce.get('kind')!='PROVIDER_REPORTED_CLOSE' or ce.get('code')!=code or ce.get('date')!=d0 or ce.get('price')!=q.get('price') or ce.get('source_timestamp')!=q.get('source_timestamp'):
                            reason='SAME_DAY_CLOSE_EVIDENCE_REQUIRED'
                except (KeyError,TypeError,ValueError):reason='CURRENT_TIMESTAMP_INVALID'
                price=q.get('price')
                if not isinstance(price,(int,float)) or isinstance(price,bool) or not math.isfinite(price) or price<=0:
                    reason=reason or 'CURRENT_PRICE_INVALID'
            actual_date=base_dates[n];selection='EXACT_BASE_DATE'
            history=daily_prices.get(code,{})
            fact=history.get(actual_date)
            if fact is None and not reason:
                evidence=(suspension_evidence or {}).get(code,{}).get(base_dates[n])
                earlier=sorted(x for x in history if isinstance(x,str) and x<base_dates[n])
                if provenance(evidence) and evidence.get('code')==code and evidence.get('requested_base_date')==base_dates[n] and earlier:
                    candidate=earlier[-1]
                    sessions=evidence.get('sessions',[])
                    needed=[x for x in dates if candidate<x<=base_dates[n]]
                    if candidate in dates and valid_fact(history[candidate],candidate) and isinstance(sessions,list) and len(sessions)==len(needed) and {x.get('date') for x in sessions}==set(needed) and all(x.get('status')=='SUSPENDED' for x in sessions):
                        actual_date=candidate;fact=history[candidate];selection='LATEST_REAL_CLOSE_BEFORE_SUSPENDED_BASE'
            if not reason:
                if not isinstance(fact,dict):reason='BASE_DATE_MISSING'
                elif fact.get('source_date')!=actual_date or fact.get('adjustment')!='UNADJUSTED':reason='BASE_IDENTITY_OR_ADJUSTMENT_INVALID'
                elif not provenance(fact):reason='BASE_PROVENANCE_MISSING'
                elif not isinstance(fact.get('close'),(int,float)) or isinstance(fact['close'],bool) or not math.isfinite(fact['close']) or fact['close']<=0:reason='BASE_CLOSE_INVALID'
            if reason:
                missing.append({'code':code,'period':n,'base_date':base_dates[n],'reason':reason,
                                'upper_bound_status':'UNSUPPORTED_INTERFACE' if upper_bounds and code in upper_bounds else 'NOT_PROVIDED'})
                continue
            change=(q['price']/fact['close']-1)*100
            if not math.isfinite(change):
                missing.append({'code':code,'period':n,'base_date':base_dates[n],'reason':'RETURN_NON_FINITE'});continue
            ranked.append({'code':code,'name':q.get('name','UNKNOWN'),'period_pct':change,
                           'current_price':q['price'],'source_timestamp':q['source_timestamp'],
                           'current_price_evidence':copy.deepcopy(q.get('close_evidence')),
                           'base_date':base_dates[n],'base_close':fact['close'],
                           'base_source':fact['source'],'base_evidence':fact['evidence'],
                           'requested_base_date':base_dates[n],'actual_price_date':actual_date,
                           'price_selection_reason':selection,'provenance':copy.deepcopy(fact),
                           'suspension_evidence':copy.deepcopy((suspension_evidence or {}).get(code,{}).get(base_dates[n])) if selection!='EXACT_BASE_DATE' else None})
        ranked.sort(key=lambda r:(-r['period_pct'],r['code']))
        top=[dict(r,rank=i+1) for i,r in enumerate(ranked[:50])]
        out[n]={'certified':not missing and len(top)==50,'top':top,'missing':missing,'excluded':excluded,
                'eligible_data_count':len(ranked),'universe_count':len(universe_codes),
                'base_date':base_dates[n],'coverage_scope':'FULL_CALLER_UNIVERSE',
                'provenance_scope':'CALLER_VERIFIED_INPUT_NOT_NETWORK_AUTHENTICATED',
                'question':'UPPER_BOUND_SOURCE_CONTRACT_UNKNOWN' if upper_bounds else None}
    return {'d0':d0,'asof':asof,'mode':mode,'not_live_production':mode=='CLOSE_MAINTENANCE',
            'periods':out,'company_pool_modified':False,'maintenance_completed':False}
