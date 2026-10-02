import json
import unittest
from unittest.mock import patch
import pm01
import pm03


class Reality(unittest.TestCase):
    def setUp(self):
        self.q={'code':'600000','name':'测试','price':9,'day_pct':-10,
                'source_timestamp':'2026-09-30T15:00:00+08:00','quote_source':'FIXTURE',
                'prev_close':10,'day_high':11,'day_low':8,'turnover':2,'amount':1000}

    def response(self,date):
        class R:
            def json(inner):
                return {'data':{'sh600000':{'data':{'date':date,'data':['1400 10 100','1430 9.5 200']}}}}
        return R()

    def test_exact_minutes_and_ratio(self):
        with patch('pm03.get',return_value=self.response('20260930')):
            p=pm03.path_reality(self.q,'2026-09-30')
        self.assertEqual(p['values']['PRICE_1400'],10)
        self.assertEqual(p['values']['PRICE_1430'],9.5)
        self.assertAlmostEqual(p['values']['RETURN_1430'],-5)
        self.assertAlmostEqual(p['values']['CURRENT_TO_HIGH'],9/11-1)
        self.assertEqual(p['trace']['1400']['timestamp'],'2026-09-30T14:00:00+08:00')

    def test_wrong_date_and_network_do_not_invent_minutes(self):
        for response in [self.response('20260929'),ConnectionError('offline')]:
            kwargs={'side_effect':response} if isinstance(response,Exception) else {'return_value':response}
            with patch('pm03.get',**kwargs):
                p=pm03.path_reality(self.q,'2026-09-30')
            self.assertEqual(p['values']['PRICE_1400'],'UNKNOWN')
            self.assertEqual(p['values']['PRICE_1430'],'UNKNOWN')
            self.assertEqual(p['values']['DAY_HIGH'],11)

    def test_future_minute_is_unknown(self):
        self.q['source_timestamp']='2026-09-30T14:10:00+08:00'
        with patch('pm03.get',return_value=self.response('20260930')):
            p=pm03.path_reality(self.q,'2026-09-30')
        self.assertEqual(p['values']['PRICE_1400'],10)
        self.assertEqual(p['values']['PRICE_1430'],'UNKNOWN')

    def test_merge_preserves_three_ranks_and_account(self):
        s=pm01.blank_snapshot()
        s.update(d0='2026-09-30',rows={'600000':self.q})
        for i,n in enumerate(['3','5','10']):
            s['top'][n]=[{**self.q,'rank':i+1}]
        s=pm01.freeze(s)
        r=pm03.merge(s)
        self.assertEqual(len(r),1)
        self.assertEqual([r[0]['SOURCE_'+n+'D_RANK'] for n in ['3','5','10']],[1,2,3])
        self.assertEqual(r[0]['SECOND_BOARD'],'UNKNOWN')
        p=pm03.build(s,{})
        self.assertEqual(p['blind_ready'],'NO')
        self.assertEqual(p['path_status'],'PARTIAL')
        self.assertTrue(pm03.export_text(p).endswith('END_PM03_BLIND_UNIVERSE'))

    def test_confirmed_absence_and_unknown_are_distinct(self):
        s=pm01.freeze(pm01.blank_snapshot())
        s.update(rows={'600000':self.q},top={'3':[{**self.q,'rank':1}],'5':[],'10':[]},
            pool_status={'3':'PASS','5':'PASS','10':'FAIL'},second_status='PASS',leader_status='PARTIAL')
        r=pm03.merge(s)[0]
        self.assertEqual(r['SOURCE_5D_RANK'],'-')
        self.assertEqual(r['SOURCE_10D_RANK'],'UNKNOWN')
        self.assertEqual(r['SECOND_BOARD'],'NO')
        self.assertEqual(r['LEADER_POOL'],'UNKNOWN')
        s['leader']=[self.q]
        self.assertEqual(pm03.merge(s)[0]['LEADER_POOL'],'YES')

    def test_ready_requires_identity_path_and_freeze(self):
        s=pm01.freeze(pm01.blank_snapshot())
        s.update(rows={'600000':self.q},top={n:[{**self.q,'rank':1}] for n in ('3','5','10')},
            pool_status={n:'PASS' for n in ('3','5','10')},second_status='PASS',leader_status='PASS',
            same_snapshot='PASS',input_freeze='PASS')
        with patch('pm03.get',return_value=self.response('20260930')):
            paths={'600000':pm03.path_reality(self.q,'2026-09-30')}
        self.assertEqual(pm03.build(s,paths)['blind_ready'],'YES')
        s['input_freeze']='FAIL'
        self.assertEqual(pm03.build(s,paths)['blind_ready'],'NO')
        s['input_freeze']='PASS';s['rows']['600000']['price']='UNKNOWN'
        self.assertEqual(pm03.build(s,paths)['blind_ready'],'NO')


if __name__=='__main__':
    unittest.main()
