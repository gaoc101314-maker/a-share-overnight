"""2026-09-30真实数据验收；绝不把缺口转换成通过。"""
import json
from collections import Counter
import auto_pools as ap
import pm01
import pm03

v=json.loads((ap.DATA/'latest_candidate.json').read_text(encoding='utf8'))
assert v['d0']=='2026-09-30'
s=ap.connect(pm01.freeze(v['snapshot']),v)
rows=pm03.merge(s)
paths,errors=pm01.fanout([s['rows'][r['代码']] for r in rows],lambda q:pm03.path_reality(q,s['d0']),budget=120,workers=6)
p=pm03.build(s,paths)
ap.atomic_json(ap.ROOT/'PM03_BLIND_UNIVERSE_V012_2026-09-30.json',p)
(ap.ROOT/'PM03_BLIND_UNIVERSE_V012_2026-09-30.txt').write_text(pm03.export_text(p),encoding='utf-8-sig')
counts=Counter(f for r in p['unknown_detail'] for f in r['fields'])
receipt={'PRODUCT':'PRODUCTION_REALITY_V0.1.2','TEST_DATE':p['d0'],
 'TOP50_OUTPUT':p['top_output'],'SECOND_BOARD_POOL_COUNT':p['second_count'],
 'SECOND_BOARD_POOL_STATUS':p['second_status'],'LEADER_POOL_COUNT':p['leader_count'],
 'LEADER_POOL_NEW':len(v['leader_pool']['new']),'LEADER_POOL_REMOVED':len(v['leader_pool']['removed_ma20']),
 'LEADER_POOL_STATUS':p['leader_status'],'UNIQUE_COUNT':p['unique_count'],
 'UNKNOWN_FIELD_COUNT':p['unknown_count'],'UNKNOWN_STOCK_COUNT':p['unknown_stock_count'],
 'UNKNOWN_BY_FIELD':dict(counts),'PATH_GAP_DETAIL':p['path_gap_detail'],
 'HISTORY_GAP_DETAIL':p['history_gap_detail'],'TOP50_HISTORY_DETAIL':v['top']['missing'],
 'SECOND_BOARD_MISSING_DATES':v['second_board']['missing_dates'],
 'SECOND_BOARD_LIMITED_COVERAGE':v['second_board']['limited_coverage_dates'],
 'LEADER_STARTING_STATE':'EMPTY_SEED_REPLAY; earlier survivors unknown',
 'FIVE_SOURCE_STATUS':p['five_source_status'],'PATH_DATA_STATUS':p['path_status'],
 'BLIND_TEST_READY':p['blind_ready'],'NETWORK_ERRORS':errors,
 'RESULT':'VERIFIED' if p['blind_ready']=='YES' else 'FAIL'}
ap.atomic_json(ap.ROOT/'PRODUCTION_REALITY_V012_acceptance.json',receipt)
(ap.ROOT/'PRODUCTION_REALITY_V012_回执.txt').write_text(json.dumps(receipt,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(receipt,ensure_ascii=True))
