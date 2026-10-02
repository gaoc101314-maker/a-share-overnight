import copy,json,unittest
from datetime import datetime
from unittest.mock import patch
import pm01

class Contract(unittest.TestCase):
 def setUp(self):
  self.s=pm01.blank_snapshot();self.s.update(d0='2026-09-30',errors=[],same_snapshot='PASS',market_count=50,data_source='FIXTURE_ONLY')
  for i in range(50):
   c=f'600{i:03d}'; r=dict(code=c,name='测试',price=10,day_pct=2,source_timestamp='2026-09-30T15:00:00+08:00',account_eligible='YES',rank=i+1,period_pct=5)
   self.s['rows'][c]=r
   for n in pm01.PERIODS:self.s['top'][str(n)].append(r.copy());self.s['pool_status'][str(n)]='PASS'
 def test_missing_manual(self):
  s=pm01.freeze(self.s);self.assertEqual((s['second_status'],s['leader_status'],s['ready']),('WAITING_INPUT','WAITING_INPUT','NO'))
 def test_complete_contract(self):
  s=pm01.freeze(self.s,'600000','600001');self.assertEqual(s['ready'],'YES');self.assertEqual(s['input_freeze'],'PASS');text=pm01.export_text(s);self.assertEqual(text.count('｜'),908);self.assertTrue(text.endswith('END_PM01_REALITY'));self.assertIn('NOT_LIVE_PRODUCTION',text);self.assertEqual(len(s['pool_metadata']),5)
 def test_unknown_invalid_stale(self):
  for value in ('12345','600000 7','999999'):
   s=pm01.freeze(self.s,value,'600001');self.assertEqual(s['ready'],'NO');self.assertEqual(s['second_status'],'FAIL')
  self.s['rows']['600000']['source_timestamp']='2026-09-29T15:00:00+08:00';self.assertEqual(pm01.freeze(self.s,'600000','600001')['ready'],'NO')
 def test_market_failure_blocks_ready(self):
  self.s['pool_status']['10']='FAIL';self.assertEqual(pm01.freeze(self.s,'600000','600001')['ready'],'NO')
 def test_account_separate(self):
  for c in ('300001','688001','920001'):self.assertEqual(pm01.eligible(c,'公司')[0],'NO')
  self.assertEqual(pm01.eligible('301001','公司')[0],'YES');self.assertEqual(pm01.eligible('600000','*ST公司')[0],'NO')
 def test_identity_revision(self):
  a=pm01.freeze(self.s,'600000','600001');b=pm01.freeze(self.s,'600002','600001');self.assertNotEqual(a['bundle_id'],b['bundle_id']);self.assertNotIn('SNAPSHOT_ID',self.s['top']['3'][0])
 def test_calendar(self):
  self.assertFalse(pm01.trading_day('2026-10-02'));self.assertFalse(pm01.trading_day('2026-09-25'));self.assertTrue(pm01.trading_day('2026-09-30'));self.assertEqual(pm01.time_mode(datetime.fromisoformat('2026-09-30T14:30:00+08:00'),'2026-09-30',True)[1],'NO')
 def test_network_failure(self):
  with patch('pm01.get',side_effect=ConnectionError('offline')):
   s=pm01.refresh();self.assertTrue(s['errors']);self.assertEqual(pm01.freeze(s,'600000','600001')['ready'],'NO')

if __name__=='__main__':unittest.main()
