"""Reuse the original evidence adapter; no network or company-pool mutation."""
import copy
import hashlib
import json
from datetime import datetime, time
import pm01
from certified_refresh import calendar, context
from verified_history_seed import load_seed
from pm03_quote_diagnostic import rank_evidence_provider, adapt_refresh
from market_events import apply_facts


def prime(d0, bases, codes):
    """Prime exact known dates; proven special cases go to the evidence adapter."""
    seed = load_seed()
    ctx = context(seed, set(codes), calendar(d0), {str(n): b for n, b in zip((3,5,10), bases)})
    special = set()
    for code in codes:
        record = seed['daily'].get(code)
        if not record:
            listing=ctx['listing_evidence'].get(code)
            if listing and all(listing['listing_date']>b for b in bases):
                special.add(code)
            continue
        closes = {d:p for d,p in record['closes'].items() if d<d0}
        key = hashlib.sha256((code+d0+','.join(bases)).encode()).hexdigest()
        if all(b in closes for b in bases):
            with pm01.CACHE_LOCK:
                pm01.HISTORY_MEMORY[key] = {**record, 'closes':closes,
                    'primary_source':'PASS' if record['source']=='TENCENT_RAW_DAILY' else 'FAIL'}
        else:
            listing=ctx['listing_evidence'].get(code)
            if all(b in closes or b in ctx['suspension_evidence'].get(code,{}) or
                   (listing and listing['listing_date']>b) for b in bases):
                special.add(code)
    return special


def repair(snapshot):
    if not snapshot.get('d0') or not snapshot.get('history_failures'):
        return snapshot
    s = copy.deepcopy(snapshot)
    codes = set(s['rows'])
    seed = load_seed()
    records = [{**v, 'code': code, 'adjustment': 'UNADJUSTED',
                'closes': {d: p for d, p in v['closes'].items() if d < s['d0']}}
               for code, v in seed['daily'].items() if code in codes]
    # Prefer newly acquired exact-date originals, including codes present in old seed.
    by_code={r['code']:r for r in records}
    with pm01.CACHE_LOCK:
        cache = copy.deepcopy(pm01.HISTORY_MEMORY)
    for code in codes:
        key = hashlib.sha256((code+s['d0']+','.join(s['base_dates'].values())).encode()).hexdigest()
        r = cache.get(key)
        if r and r.get('adjustment')=='UNADJUSTED' and r.get('evidence'):
            by_code[code]={**r,'code':code}
    records=list(by_code.values())
    dates = calendar(s['d0'])
    ctx = context(seed, codes, dates, s['base_dates'])
    clock = pm01.now_cn()
    same_day = clock.date().isoformat() == s['d0']
    mode = 'PRODUCTION'
    asof = clock.isoformat()
    after_close = same_day and clock.time().replace(tzinfo=None) >= time(15)
    if not same_day or after_close:
        # Same-day after-close and historical close ranking are research-only.
        try:
            timestamps = [datetime.fromisoformat(q['source_timestamp']) for q in s['rows'].values()]
        except (KeyError, TypeError, ValueError):
            return snapshot
        if not timestamps or any(t.tzinfo is None or t.date().isoformat() != s['d0']
                or t.time().replace(tzinfo=None) < time(15) or t > clock for t in timestamps):
            return snapshot
        asof = clock.isoformat() if same_day else max(timestamps).isoformat()
        mode = 'CLOSE_MAINTENANCE'
        for code, q in s['rows'].items():
            q['close_evidence'] = {'kind': 'PROVIDER_REPORTED_CLOSE', 'code': code,
                'date': s['d0'], 'price': q['price'], 'source_timestamp': q['source_timestamp'],
                'source': q['quote_source'], 'evidence': 'Original provider quote sha256='+hashlib.sha256(json.dumps(q,sort_keys=True).encode()).hexdigest()}
    evidence = rank_evidence_provider(s, pm01.get, ctx, records, mode=mode, asof=asof, budget=90)
    evidence = apply_facts(evidence, s['base_dates'], s['d0'], pm01.get, clock.isoformat())
    result = adapt_refresh(s, evidence, asof, mode=mode)
    result['trace']['PER_PERIOD_ADAPTER']['event_diagnostics']=copy.deepcopy(evidence.get('event_diagnostics',[]))
    for ranked in result['top'].values():
        for row in ranked:
            account, reason = pm01.eligible(row['code'], row['name'])
            row.update(account_eligible=account, account_reason=reason, market_universe='YES')
    return result
