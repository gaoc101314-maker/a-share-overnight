import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import auto_pools as ap


class Maintenance(unittest.TestCase):
    def test_second_day_not_third_day_and_calendar_age(self):
        calendar=ap.trading_dates('2026-09-01','2026-09-30')
        feeds={d:{'source':'FIXTURE','coverage':'ALL_A_OBSERVED','rows':[]} for d in calendar}
        feeds['2026-09-29']['rows']=[{'code':'688001','name':'测试','consecutive':2}]
        feeds['2026-09-30']['rows']=[{'code':'688001','name':'测试','consecutive':3}]
        p=ap.second_board('2026-09-30',feeds,calendar,[])
        r=p['rows'][0]
        self.assertEqual(r['latest_second_board_date'],'2026-09-29')
        self.assertEqual(r['age_trading_days'],1)
        self.assertEqual(len(p['rows']),1)

    def test_second_board_30_day_exit(self):
        calendar=ap.trading_dates('2026-08-03','2026-09-30')
        feeds={d:{'source':'FIXTURE','coverage':'ALL_A_OBSERVED','rows':[]} for d in calendar}
        feeds['2026-08-28']['rows']=[{'code':'600001','name':'过期','consecutive':2}]
        p=ap.second_board('2026-09-30',feeds,calendar,[{'code':'600001'}])
        self.assertEqual(p['rows'],[])
        self.assertEqual(p['removed'],['600001'])

    def test_unknown_full_observation_run_start_not_invented(self):
        calendar=ap.trading_dates('2026-09-01','2026-09-30')
        feeds={d:{'source':'TENCENT_OBSERVED_CLOSE_LIMIT','coverage':'ALL_A_OBSERVED','rows':[{'code':'600001','name':'测试','consecutive':None}]} for d in calendar}
        p=ap.second_board('2026-09-30',feeds,calendar,[])
        self.assertEqual(p['status'],'PARTIAL')
        self.assertTrue(p['unresolved_run_start'])

    def test_leader_inheritance_add_exit_and_equal_ma20(self):
        dates=ap.trading_dates('2026-08-03','2026-09-30')[-20:]
        prices={c:{d:10 for d in dates} for c in ['300001','600001','600002']}
        prices['600001'][dates[-1]]=8
        top={'top':{n:[{'code':'300001','name':'创业板保留'}] for n in ['3','5','10']},'certified':True}
        prior=[{'code':'600001','name':'退出','first_entry_date':'2026-08-03','last_common_certification_date':'2026-08-04'},
               {'code':'600002','name':'保持','first_entry_date':'2026-08-03','last_common_certification_date':'2026-08-04'}]
        p=ap.leaders(dates[-1],top,prices,prior,True)
        self.assertEqual(p['new'],['300001'])
        self.assertEqual(p['removed_ma20'],['600001'])
        self.assertEqual(next(r for r in p['rows'] if r['code']=='600002')['first_entry_date'],'2026-08-03')
        next_top={'top':{n:[] for n in ['3','5','10']},'certified':True}
        p2=ap.leaders(dates[-1],next_top,prices,p['rows'],True)
        self.assertIn('300001',{r['code'] for r in p2['rows']})

    def test_missing_ma20_retains_unknown_not_false_exit(self):
        top={'top':{n:[] for n in ['3','5','10']},'certified':True}
        p=ap.leaders('2026-09-30',top,{},[{'code':'600001','name':'旧池'}],True)
        self.assertEqual(p['status'],'PARTIAL')
        self.assertEqual(p['removed_ma20'],[])
        self.assertEqual(p['rows'][0]['pool_status'],'UNKNOWN_MA20')

    def test_failed_version_preserves_verified(self):
        with tempfile.TemporaryDirectory() as tmp:
            ap.publish({'d0':'2026-09-29','version_id':'a','five_source_status':'PASS'},tmp)
            ap.publish({'d0':'2026-09-30','version_id':'b','five_source_status':'FAIL'},tmp)
            import json
            self.assertEqual(json.loads((Path(tmp)/'latest_verified.json').read_text())['version_id'],'a')
            self.assertEqual(len(list((Path(tmp)/'archive').glob('*.json'))),2)

    def test_exit_and_reentry_preserve_first_ever_date(self):
        dates=ap.trading_dates('2026-08-03','2026-09-30')[-21:]
        bars={d:10 for d in dates};bars['2026-09-29']=8;bars['2026-09-30']=12
        empty={'top':{n:[] for n in ['3','5','10']},'certified':True}
        prior=[{'code':'600001','name':'测试','first_entry_date':'2026-09-01','last_common_certification_date':'2026-09-02'}]
        exited=ap.leaders('2026-09-29',empty,{'600001':bars},prior,True)
        self.assertEqual(exited['removed_ma20'],['600001'])
        common={'top':{n:[{'code':'600001','name':'测试'}] for n in ['3','5','10']},'certified':True}
        reentered=ap.leaders('2026-09-30',common,{'600001':bars},[],True,exited['entry_ledger'])
        self.assertEqual(reentered['rows'][0]['first_entry_date'],'2026-09-01')

    def test_live_snapshot_limit_price_is_not_fixed_threshold(self):
        snapshot={'d0':'2026-09-30','market_count':2,'source_timestamp':'2026-09-30T15:00:00+08:00','rows':{
            '688001':{'code':'688001','name':'科创','price':12,'limit_up_price':12,'source_timestamp':'2026-09-30T15:00:00+08:00'},
            '920001':{'code':'920001','name':'北交所','price':13,'limit_up_price':13,'source_timestamp':'2026-09-30T15:00:00+08:00'}}}
        with patch('auto_pools.atomic_json'):
            p=ap.observed_limit_history(snapshot)
        self.assertEqual(p['coverage'],'ALL_A_OBSERVED')
        self.assertEqual(len(p['rows']),2)


if __name__=='__main__':unittest.main()
