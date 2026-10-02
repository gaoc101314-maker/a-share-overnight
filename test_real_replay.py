"""验证真实输入的合并、来源身份、保留排名与缺口，不用模拟名单冒充现实验收。"""
import json
import unittest
import pm03
import real_replay as rr


class RealReplay(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.second=rr.read_pool(rr.ROOT/rr.SECOND,137)
        cls.leader=rr.read_pool(rr.ROOT/rr.LEADER,136)
        cls.market=json.loads((rr.ROOT/'.auto_pool_cache/latest_market_snapshot.json').read_text(encoding='utf8'))
        cls.version=json.loads((rr.ROOT/'five_source_versions/latest_candidate.json').read_text(encoding='utf8'))

    def test_real_codes_and_multi_source_identity(self):
        s,names=rr.prepare(self.market,self.version['snapshot'],self.second,self.leader)
        rows=pm03.merge(s)
        self.assertEqual(len(names),273)
        self.assertEqual(len(rows),279)
        self.assertEqual(len({r['代码'] for r in rows}),279)
        self.assertEqual(sum(r['SECOND_BOARD']=='YES' for r in rows),137)
        self.assertEqual(sum(r['LEADER_POOL']=='YES' for r in rows),136)
        self.assertTrue(all(r['SECOND_BOARD'] in ('YES','NO') and r['LEADER_POOL'] in ('YES','NO') for r in rows))
        for n in ('3','5','10'):
            actual={r['代码']:r['SOURCE_'+n+'D_RANK'] for r in rows if isinstance(r['SOURCE_'+n+'D_RANK'],int)}
            self.assertEqual(actual,{r['code']:r['rank'] for r in self.version['snapshot']['top'][n]})
        p=rr.make_product(s,{},self.second,self.leader,names,self.version['version_id'])
        self.assertEqual(p['multi_source_count'],92)
        self.assertEqual(p['five_source_status'],'PASS')
        self.assertEqual(p['blind_ready'],'NO')
        self.assertEqual(p['account_status'],'PASS')
        self.assertTrue(rr.export_text(p).endswith('END_PM03_BLIND_UNIVERSE'))

    def test_names_dates_and_codes_not_silently_repaired(self):
        second={**self.second,'rows':[dict(r) for r in self.second['rows']]}
        second['rows'][0]['name']='错误名称'
        with self.assertRaisesRegex(ValueError,'名称不匹配'):
            rr.prepare(self.market,self.version['snapshot'],second,self.leader)
        with self.assertRaisesRegex(ValueError,'D0'):
            rr.prepare({**self.market,'d0':'2026-09-29'},self.version['snapshot'],self.second,self.leader)


if __name__=='__main__':unittest.main()
