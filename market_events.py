"""Acquire bounded listing/suspension facts; never create missing price bars."""
import hashlib
import json
import os
import re
from datetime import datetime
from pathlib import Path
from html.parser import HTMLParser

HALTS = 'https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SUSPEND_START_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB&filter=%28MARKET%3D%22%E5%85%A8%E9%83%A8%22%29%28DATETIME%3D%27{day}%27%29'
LISTINGS = 'https://stock.10jqka.com.cn/api/stock_diary.html'

class Table(HTMLParser):
    def __init__(self):
        super().__init__(); self.rows=[]; self.row=[]; self.cell=None; self.code=None
    def handle_starttag(self, tag, attrs):
        if tag=='tr': self.row=[]; self.code=None
        if tag in ('td','th'): self.cell=[]
        if tag=='a':
            match=re.search(r'[?&]stock=([0-9]{6})(?:&|$)',dict(attrs).get('href',''))
            if match:self.code=match.group(1)
    def handle_data(self, value):
        if self.cell is not None: self.cell.append(value)
    def handle_endtag(self, tag):
        if tag in ('td','th') and self.cell is not None:
            self.row.append(''.join(self.cell).strip()); self.cell=None
        if tag in ('tr','thead') and self.row:
            if tag=='tr' and self.code and len(self.row)>1:self.row[1]=self.code
            self.rows.append(self.row)

def validate_raw(url, raw):
    if url.startswith('https://datacenter-web.eastmoney.com/'):
        payload=json.loads(raw); result=payload.get('result')
        if payload.get('success') is not True or not isinstance(result,dict) or result.get('pages') not in (0,1) or len(result.get('data',[]))!=result.get('count'):raise ValueError('HALT_TABLE_INCOMPLETE')
    else:
        table=Table();table.feed(raw)
        if not any('上市日期' in r and '申购代码' in r for r in table.rows):raise ValueError('LISTING_HEADERS')

def acquire(get, url, day, now):
    cache=os.environ.get('PM03_HISTORY_CACHE_DIR')
    path=Path(cache)/('events-'+day+'-'+hashlib.sha256(url.encode()).hexdigest()[:12]+'.json') if cache else None
    if path and path.exists():
        try:
            value=json.loads(path.read_text(encoding='utf8'))
            if value['source']==url and value['date']==day and hashlib.sha256(value['raw'].encode()).hexdigest()==value['sha256']:
                validate_raw(url,value['raw'])
                return value
        except (ValueError,KeyError): pass
    response=get(url); response.raise_for_status()
    raw=response.content.decode('utf-8') if isinstance(getattr(response,'content',None),bytes) else response.text
    validate_raw(url,raw)
    value={'date':day,'source':url,'fetched_at':now,'raw':raw,'sha256':hashlib.sha256(raw.encode()).hexdigest()}
    if path:
        path.parent.mkdir(parents=True,exist_ok=True)
        temporary=path.with_suffix('.tmp'); temporary.write_text(json.dumps(value,ensure_ascii=False),encoding='utf8'); temporary.replace(path)
    return value

def apply_facts(evidence, bases, day, get, now):
    dates=evidence.get('trading_dates',[]); prices=evidence.get('daily_prices',{})
    codes=evidence.get('universe_codes',[])
    gaps={c:[b for b in bases.values() if b not in prices.get(c,{})] for c in codes}
    gaps={c:v for c,v in gaps.items() if v}
    if not gaps: return evidence
    diagnostics=evidence.setdefault('event_diagnostics',[])
    # Failure retains the original gap. A missing event is never permission to pass.
    try:
        receipt=acquire(get,LISTINGS,day,now); table=Table(); table.feed(receipt['raw'])
        if not any('上市日期' in r and '申购代码' in r for r in table.rows): raise ValueError('LISTING_HEADERS')
        for row in table.rows:
            if len(row)<4 or row[1] not in gaps: continue
            code,listed=row[1],row[3]
            if len(listed)==10 and listed[4]=='-' and listed[7]=='-' and '1990-01-01'<=listed<=day:
                evidence.setdefault('listing_evidence',{})[code]={'code':code,'listing_date':listed,'source':LISTINGS,'evidence':'Actual listing calendar captured '+receipt['fetched_at']+' sha256='+receipt['sha256']}
    except Exception as exc: diagnostics.append({'source':'LISTING_CALENDAR','status':'UNAVAILABLE','error':type(exc).__name__})
    try:
        intervals=[]
        # A resumed stock is absent from today's halt list. Query the actual
        # missing base dates too, while validating interval bounds independently.
        query_days=sorted({day}|{b for c,bs in gaps.items() for b in bs
            if evidence.get('listing_evidence',{}).get(c,{}).get('listing_date','')<=b})
        for query_day in query_days:
            halt_url=HALTS.format(day=query_day)
            try:
                receipt=acquire(get,halt_url,day,now); payload=json.loads(receipt['raw']); result=payload['result']
                if payload.get('success') is not True or result['pages'] not in (0,1) or len(result['data'])!=result['count']: raise ValueError('HALT_TABLE_INCOMPLETE')
                intervals.extend((row,receipt,halt_url) for row in result['data'])
                diagnostics.append({'source':'SUSPENSION_INTERVALS','query_date':query_day,'status':'ACQUIRED','sha256':receipt['sha256'],'count':result['count']})
            except Exception as exc:
                diagnostics.append({'source':'SUSPENSION_INTERVALS','query_date':query_day,'status':'UNAVAILABLE','error':type(exc).__name__})
        completed=max(d for d in dates if d<day)
        for row,receipt,halt_url in intervals:
            code=row.get('SECURITY_CODE')
            if code not in gaps: continue
            start=str(row.get('SUSPEND_START_TIME',''))
            try:
                if datetime.strptime(start,'%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')!=start:continue
            except ValueError:continue
            if start[11:]>'09:30:00': continue
            end=row.get('SUSPEND_END_TIME')
            if end:
                try:
                    if not isinstance(end,str) or datetime.strptime(end,'%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')!=end or end<start:continue
                except ValueError:continue
            elif row.get('SUSPEND_EXPIRE')!='连续停牌':
                continue
            # Finite full-session intervals are certified by actual start/end,
            # including one-day halts. Display labels may have source encoding
            # damage and cannot override numeric interval evidence. No end bound
            # still requires the original explicit continuous-halt marker.
            through=min(completed,str(end)[:10]) if end else completed
            if end and str(end)[11:]<'15:00:00':
                through=max((d for d in dates if d<str(end)[:10]),default='')
                through=min(completed,through)
            for base in gaps[code]:
                prior=sorted(d for d in prices.get(code,{}) if d<base)
                if not prior: continue
                needed=[d for d in dates if prior[-1]<d<=base]
                if needed and all(start[:10]<=d<=through for d in needed):
                    evidence.setdefault('suspension_evidence',{}).setdefault(code,{})[base]={'code':code,'requested_base_date':base,'source':halt_url,'evidence':'Current provider continuous interval captured '+receipt['fetched_at']+' sha256='+receipt['sha256']+'; query date is not a historical snapshot; past completed sessions only; predicted resumption ignored; actual prior price/date retained','sessions':[{'date':d,'status':'SUSPENDED'} for d in needed]}
    except Exception as exc: diagnostics.append({'source':'SUSPENSION_INTERVALS','status':'UNAVAILABLE','error':type(exc).__name__})
    return evidence