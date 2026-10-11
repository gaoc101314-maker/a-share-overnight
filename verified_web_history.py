"""Reuse the original evidence adapter; no network or company-pool mutation."""
import copy
import hashlib
import json
from datetime import datetime, time
import pm01
from certified_refresh import calendar, context
from verified_history_seed import load_seed
from pm03_quote_diagnostic import rank_evidence_provider, adapt_refresh


def repair(snapshot):
    if not snapshot.get('d0') or not snapshot.get('history_failures'):
        return snapshot
    s = copy.deepcopy(snapshot)
    codes = set(s['rows'])
    seed = load_seed()
    records = [{**v, 'code': code, 'adjustment': 'UNADJUSTED',
                'closes': {d: p for d, p in v['closes'].items() if d < s['d0']}}
               for code, v in seed['daily'].items() if code in codes]
    # Successful original cache entries retain code identity through their exact key.
    with pm01.CACHE_LOCK:
        cache = copy.deepcopy(pm01.HISTORY_MEMORY)
    for code in codes - set(seed['daily']):
        key = hashlib.sha256((code+s['d0']+','.join(s['base_dates'].values())).encode()).hexdigest()
        r = cache.get(key)
        if r:
            records.append({**r, 'code': code, 'adjustment': 'UNADJUSTED',
                            'evidence': 'Original exact-date cache sha256='+hashlib.sha256(json.dumps(r,sort_keys=True).encode()).hexdigest()})
    dates = calendar(s['d0'])
    ctx = context(seed, codes, dates, s['base_dates'])
    clock = pm01.now_cn()
    same_day = clock.date().isoformat() == s['d0']
    mode = 'PRODUCTION'
    asof = clock.isoformat()
    if not same_day:
        # Historical close ranking is explicitly research-only; capture time stays intact.
        timestamps = [datetime.fromisoformat(q['source_timestamp']) for q in s['rows'].values()]
        if any(t.date().isoformat() != s['d0'] or t.time().replace(tzinfo=None) < time(15) for t in timestamps):
            return snapshot
        asof = max(timestamps).isoformat()
        mode = 'CLOSE_MAINTENANCE'
        for code, q in s['rows'].items():
            q['close_evidence'] = {'kind': 'PROVIDER_REPORTED_CLOSE', 'code': code,
                'date': s['d0'], 'price': q['price'], 'source_timestamp': q['source_timestamp'],
                'source': q['quote_source'], 'evidence': 'Original provider quote sha256='+hashlib.sha256(json.dumps(q,sort_keys=True).encode()).hexdigest()}
    def no_extra_network(*args, **kwargs):
        raise ValueError('VERIFIED_HISTORY_NOT_COVERED')
    evidence = rank_evidence_provider(s, no_extra_network, ctx, records, mode=mode, asof=asof)
    result = adapt_refresh(s, evidence, asof, mode=mode)
    for ranked in result['top'].values():
        for row in ranked:
            account, reason = pm01.eligible(row['code'], row['name'])
            row.update(account_eligible=account, account_reason=reason, market_universe='YES')
    return result
