"""盘后自动维护和真实回放入口；云端调度调用，不需要老板电脑开机。"""
import argparse
import json
import time
from pathlib import Path
import pm01
import auto_pools as ap

last=[0]
def progress(done,total,elapsed):
    if time.monotonic()-last[0]>20 or done==total:
        print(f'FETCH {done}/{total} {elapsed:.1f}s',flush=True)
        last[0]=time.monotonic()


def run(replay=False,snapshot_file=None,replay_days=12):
    if not replay:
        config=ap.ROOT/'five_source_activation.json'
        try:
            approved=json.loads(config.read_text(encoding='utf8'))
        except (OSError,ValueError):
            approved={}
        if any(approved.get(key) is not True for key in ('historical_replay_approved','leader_seed_complete','manual_pool_comparison_approved')):
            raise RuntimeError('FORWARD_NOT_ACTIVATED: historical replay and formal seed not verified')
        now=pm01.now_cn()
        if pm01.trading_day(now.date().isoformat()) is not True or now.hour<16:
            print('SKIP: not a verified trading day after 16:00',flush=True)
            return []
    snapshot=json.loads(Path(snapshot_file).read_text(encoding='utf8')) if snapshot_file else pm01.refresh(progress)
    if not snapshot.get('d0') or not snapshot.get('base_dates'):
        raise RuntimeError('MARKET_SNAPSHOT_UNAVAILABLE')
    day=snapshot['d0']
    ap.atomic_json(ap.CACHE/'latest_market_snapshot.json',snapshot)
    if not replay and day!=pm01.now_cn().date().isoformat():
        raise RuntimeError('QUOTE_DATE_NOT_TODAY')
    if any(q['source_timestamp'][:10]!=day or q['source_timestamp'][11:16]<'15:00' for q in snapshot['rows'].values()):
        raise RuntimeError('SNAPSHOT_NOT_COMPLETE_CLOSE')
    prices,history_gaps=ap.histories(snapshot)
    days=ap.trading_dates((pm01.datetime.fromisoformat(day)-pm01.timedelta(days=max(60,replay_days*3))).date().isoformat(),day)[-replay_days:] if replay else [day]
    start=(pm01.datetime.fromisoformat(days[0])-pm01.timedelta(days=60)).date().isoformat()
    calendar=ap.trading_dates(start,day)
    cutoff=(pm01.datetime.fromisoformat(days[0])-pm01.timedelta(days=ap.WINDOW_DAYS)).date().isoformat()
    required=[d for d in calendar if d>=cutoff]
    feeds,feed_errors=pm01.fanout(required,lambda d:ap.limit_history(d,prices,day),progress,budget=180,workers=4)
    observed=ap.observed_limit_history(snapshot)
    if observed['coverage']=='ALL_A_OBSERVED':
        feeds[day]=observed
        feed_errors.pop(day,None)
    prior=None
    seed_complete=False
    try:
        seed=json.loads((ap.DATA/'latest_verified.json').read_text(encoding='utf8'))
        if seed['d0']<days[0] and seed['five_source_status']=='PASS':
            prior=seed;seed_complete=True
    except (OSError,ValueError,KeyError):
        pass
    if prior is None:
        try:
            seed=json.loads((ap.ROOT/'leader_pool_seed.json').read_text(encoding='utf8'))
            assert seed['source']=='COMPANY_OFFICIAL_SEED' and seed['asof_date']<days[0]
            assert all(len(r['code'])==6 and r['code'].isdigit() and r['name'] and
                r['first_entry_date']<=r['last_common_certification_date']<=seed['asof_date'] and
                pm01.trading_day(r['first_entry_date']) is True for r in seed['rows'])
            assert len({r['code'] for r in seed['rows']})==len(seed['rows'])
            prior={'d0':seed['asof_date'],'leader_pool':{'rows':seed['rows']},'second_board':{'rows':[]}}
            seed_complete=True
        except (OSError,ValueError,KeyError,AssertionError,TypeError):
            pass
    versions=[]
    for date in days:
        top=ap.top50(date,prices,snapshot['rows'],day)
        second=ap.second_board(date,feeds,calendar,prior['second_board']['rows'] if prior else [])
        leader=ap.leaders(date,top,prices,prior['leader_pool']['rows'] if prior else [],seed_complete,
            prior['leader_pool'].get('entry_ledger',{}) if prior else {})
        version=ap.daily_version(date,top,second,leader,snapshot,prior)
        version['history_data_gap']=history_gaps
        version['limit_up_feed_errors']=feed_errors
        version['limit_up_source_trace']={d:{k:v for k,v in f.items() if k!='rows'} for d,f in feeds.items() if d<=date}
        version['replay_universe_warning']='Historical universe uses end-date securities; historical survivorship not independently verified' if replay else None
        ap.publish(version)
        (ap.DATA/(date+'.txt')).write_text(ap.report(version),encoding='utf8')
        print(ap.report(version),flush=True)
        versions.append(version);prior=version
    return versions


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--replay',action='store_true')
    parser.add_argument('--snapshot')
    parser.add_argument('--replay-days',type=int,default=12)
    args=parser.parse_args()
    if not 1<=args.replay_days<=30:parser.error('replay-days must be 1..30')
    versions=run(args.replay,args.snapshot,args.replay_days)
    if versions:
        print('RESULT='+('VERIFIED' if all(v['five_source_status']=='PASS' for v in versions) else 'FAIL'),flush=True)
