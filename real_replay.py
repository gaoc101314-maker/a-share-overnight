"""公司CSV五源历史考卷：只合并真实输入和已冻结D0行情，不生成候选或交易决策。"""
import copy
import hashlib
import json
import re
import unicodedata
from pathlib import Path
import pandas as pd
import pm01
import pm03

ROOT=Path(__file__).parent
DAY='2026-09-30'
PRODUCT='PM03_BLIND_UNIVERSE_'+DAY
SECOND='二板总池_'+DAY+'.csv'
LEADER='总龙头池_'+DAY+'.csv'


def normalize_name(name):
    return re.sub(r'^(XD|XR|DR)','',re.sub(r'\s+','',unicodedata.normalize('NFKC',name)))


def read_pool(path,expected):
    """按字符串读取，保留000开头代码；重复代码的名称冲突必须显式失败。"""
    raw=Path(path).read_bytes()
    table=None
    for encoding in ('utf-8-sig','gb18030'):
        try:
            table=pd.read_csv(path,dtype=str,encoding=encoding,keep_default_na=False)
            break
        except UnicodeError:
            continue
    if table is None or list(table.columns)!=['股票代码','股票名称']:
        raise ValueError('CSV必须只有股票代码、股票名称两列')
    table=table.apply(lambda col:col.str.strip())
    if not table['股票代码'].str.fullmatch(r'\d{6}').all() or not table['股票名称'].str.len().gt(0).all():
        raise ValueError('CSV存在无效代码或空名称')
    if table.groupby('股票代码')['股票名称'].nunique().gt(1).any():
        raise ValueError('同一代码存在冲突名称')
    unique=table.drop_duplicates('股票代码')
    if len(unique)!=expected:
        raise ValueError('公司池唯一代码数量与137/136输入声明不一致')
    return {'file':Path(path).name,'raw_count':len(table),'unique_count':len(unique),
        'sha256':hashlib.sha256(raw).hexdigest(),'source':'USER_PROVIDED_COMPANY_REAL_POOL_CSV',
        'date':DAY,'screenshot_continuity':'USER_ATTESTED; original screenshots not independently inspected',
        'rows':[{'code':r['股票代码'],'name':r['股票名称']} for _,r in unique.iterrows()]}


def prepare(market,top_snapshot,second,leader):
    if market['d0']!=DAY or top_snapshot['d0']!=DAY:
        raise ValueError('D0不一致，拒绝混用日期')
    s=copy.deepcopy(market)
    # 严格沿用已生成版本的三张50行名单和排名，绝不因账户资格或公司池重排。
    s['top']=copy.deepcopy(top_snapshot['top'])
    s['base_dates']=copy.deepcopy(top_snapshot['base_dates'])
    s['pool_status']=copy.deepcopy(top_snapshot['pool_status'])
    s['history_failures']=copy.deepcopy(top_snapshot.get('history_failures',{}))
    for n in ('3','5','10'):
        rows=s['top'][n]
        if len(rows)!=50 or len({r['code'] for r in rows})!=50 or {r['rank'] for r in rows}!=set(range(1,51)):
            raise ValueError('已有Top50输出不完整：'+n)
    names=[]
    for pool in (second,leader):
        for r in pool['rows']:
            q=s['rows'].get(r['code'])
            if q is None or q['source_timestamp'][:10]!=DAY:
                raise ValueError('缺少D0代码行情：'+r['code'])
            if normalize_name(r['name'])!=normalize_name(q['name']):
                raise ValueError('CSV与行情名称不匹配：'+r['code'])
            names.append({'code':r['code'],'csv_name':r['name'],'quote_name':q['name'],'status':'MATCH'})
    s=pm01.freeze(s,'\n'.join(r['code'] for r in second['rows']),'\n'.join(r['code'] for r in leader['rows']))
    if s['second_status']!='PASS' or s['leader_status']!='PASS':
        raise ValueError('公司池行情挂接不完整')
    s['bundle_id']='REAL_REPLAY-'+hashlib.sha256(json.dumps([s['snapshot_id'],s['top'],second['sha256'],leader['sha256']],sort_keys=True,ensure_ascii=False).encode()).hexdigest()[:24]
    for key,pool,label in [('second',second,'SECOND_BOARD'),('leader',leader,'LEADER_POOL')]:
        input_map={r['code']:r['name'] for r in pool['rows']}
        for r in s[key]:
            r.update(POOL_VERSION_ID=pool['sha256'],POOL_ASOF_DATE=DAY,
                POOL_ASSET={'source':pool['source'],'csv_file':pool['file'],'csv_sha256':pool['sha256'],
                    'csv_name':input_map[r['code']],'code':r['code'],'asof_date':DAY})
    s['trace']['COMPANY_REAL_POOLS']={'second':{k:v for k,v in second.items() if k!='rows'},
        'leader':{k:v for k,v in leader.items() if k!='rows'}}
    return s,names


def make_product(s,paths,second,leader,names,top_version):
    p=pm03.build(s,paths)
    identity=['SOURCE_'+n+'D_RANK' for n in ('3','5','10')]+['SECOND_BOARD','LEADER_POOL']
    same_date=bool(p['rows']) and all(r['SOURCE_TIMESTAMP'][:10]==DAY for r in p['rows'])
    valid=all(r[f]!='UNKNOWN' for r in p['rows'] for f in identity) and same_date
    # 本次验收的是给定五张名单的真实身份。全市场历史认证状态独立保留，不伪造原Top50认证。
    p['five_source_status']='PASS' if valid else 'FAIL'
    p['blind_ready']='YES' if valid and p['path_status']=='PASS' else 'NO'
    p.update(product_id=PRODUCT,test_date=DAY,input_scope='FIXED_EXISTING_TOP50_AND_COMPANY_REAL_CSV_POOLS',
        top_version_id=top_version,second_input=second,leader_input=leader,name_matches=names,
        multi_source_count=sum(sum(isinstance(r['SOURCE_'+n+'D_RANK'],int) for n in ('3','5','10'))+(r['SECOND_BOARD']=='YES')+(r['LEADER_POOL']=='YES')>1 for r in p['rows']),
        account_status='PASS' if all(r['ACCOUNT_ELIGIBLE'] in ('YES','NO') and r['ACCOUNT_INELIGIBLE_REASON']!='UNKNOWN' for r in p['rows']) else 'FAIL',
        identity_status='PASS' if valid else 'FAIL',replay_date_consistency='PASS' if same_date else 'FAIL',
        original_market_capture_status=s['same_snapshot'])
    return p


def export_text(p):
    text=pm03.export_text(p)
    metadata=[f'PRODUCT_ID={PRODUCT}',f'TEST_DATE={DAY}',
        'FIVE_SOURCE_SCOPE=Fixed existing Top50 outputs + user supplied company real CSV pools; no new market recertification',
        f"TOP50_VERSION_ID={p['top_version_id']}",f"MULTI_SOURCE_COUNT={p['multi_source_count']}",
        f"ACCOUNT_DATA_STATUS={p['account_status']}",
        f"REPLAY_DATE_CONSISTENCY={p['replay_date_consistency']}",f"ORIGINAL_MARKET_CAPTURE_STATUS={p['original_market_capture_status']}",
        'SNAPSHOT_SCOPE=Frozen replay batch with per-stock D0 source timestamps; not an atomic exchange snapshot',
        'COMPANY_REAL_INPUT='+json.dumps({'second':{k:v for k,v in p['second_input'].items() if k!='rows'},'leader':{k:v for k,v in p['leader_input'].items() if k!='rows'}},ensure_ascii=False)]
    return text.replace('PM03_BLIND_UNIVERSE\n','PM03_BLIND_UNIVERSE\n'+'\n'.join(metadata)+'\n',1)


def receipt(p):
    return {'PRODUCT':'PM03_FIVE_SOURCE_REAL_REPLAY_V0.1','TEST_DATE':DAY,
        '3D_TOP50':p['top_output']['3'],'5D_TOP50':p['top_output']['5'],'10D_TOP50':p['top_output']['10'],
        'SECOND_BOARD_INPUT':p['second_input']['unique_count'],'LEADER_POOL_INPUT':p['leader_input']['unique_count'],
        'SECOND_BOARD_REAL_POOL':'PASS','LEADER_REAL_POOL':'PASS','NAME_CODE_MATCH':'273/273',
        'FIVE_SOURCE_STATUS':p['five_source_status'],'FIVE_SOURCE_UNIQUE_COUNT':p['unique_count'],
        'MULTI_SOURCE_COUNT':p['multi_source_count'],'MULTI_SOURCE_IDENTITY':p['identity_status'],
        'D0_PATH_DATA':p['path_status'],'1400_DATA':str(sum(r['PRICE_1400']!='UNKNOWN' for r in p['rows']))+'/'+str(p['unique_count']),
        '1430_DATA':str(sum(r['PRICE_1430']!='UNKNOWN' for r in p['rows']))+'/'+str(p['unique_count']),
        'ACCOUNT_DATA_STATUS':p['account_status'],'UNKNOWN_FIELD_COUNT':p['unknown_count'],
        'UNKNOWN_STOCK_COUNT':p['unknown_stock_count'],'UNKNOWN_DETAIL':p['unknown_detail'],
        'BLIND_UNIVERSE_OUTPUT':PRODUCT+'.txt','BLIND_TEST_READY':p['blind_ready'],
        'TOP50_HISTORY_CERTIFICATION':p['pool_status'],'RESULT':'VERIFIED' if p['blind_ready']=='YES' else 'FAIL'}


def generate():
    second=read_pool(ROOT/SECOND,137);leader=read_pool(ROOT/LEADER,136)
    market=json.loads((ROOT/'.auto_pool_cache/latest_market_snapshot.json').read_text(encoding='utf8'))
    version=json.loads((ROOT/'five_source_versions/latest_candidate.json').read_text(encoding='utf8'))
    s,names=prepare(market,version['snapshot'],second,leader)
    paths,errors=pm01.fanout([s['rows'][r['代码']] for r in pm03.merge(s)],lambda q:pm03.path_reality(q,DAY),budget=180,workers=6)
    p=make_product(s,paths,second,leader,names,version['version_id'])
    p['fetch_failures']=errors
    (ROOT/(PRODUCT+'.json')).write_text(json.dumps(p,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf8')
    (ROOT/(PRODUCT+'.txt')).write_text(export_text(p),encoding='utf-8-sig')
    (ROOT/'PM03_FIVE_SOURCE_REAL_REPLAY_receipt.json').write_text(json.dumps(receipt(p),ensure_ascii=False,indent=2),encoding='utf8')
    print(json.dumps(receipt(p),ensure_ascii=True))


def render():
    import streamlit as st
    import streamlit.components.v1 as components
    st.title('PM03真实五源回放')
    st.caption('公司真实CSV池 · 固定考卷 · 不生成C01—C05')
    try:
        p=json.loads((ROOT/(PRODUCT+'.json')).read_text(encoding='utf8'))
        # 每次显示都校验实际输入文件哈希，避免CSV替换后仍显示旧考卷。
        for file,key in [(SECOND,'second_input'),(LEADER,'leader_input')]:
            if hashlib.sha256((ROOT/file).read_bytes()).hexdigest()!=p[key]['sha256']:
                raise ValueError('公司CSV已变化，旧考卷不得冒充本次输入')
    except (OSError,ValueError,KeyError) as e:
        st.error('真实五源回放尚未取得或校验失败：'+str(e))
        return
    with st.container(border=True):
        st.write('TEST_DATE='+DAY)
        st.write('3D Top50=50/50 · 5D Top50=50/50 · 10D Top50=50/50')
        st.write(f"二板真实池={p['second_count']} · 龙头真实池={p['leader_count']}")
        st.write(f"FIVE_SOURCE_UNIQUE_COUNT={p['unique_count']} · MULTI_SOURCE_COUNT={p['multi_source_count']}")
        st.write(f"FIVE_SOURCE_STATUS={p['five_source_status']} · PATH_DATA_STATUS={p['path_status']}")
        st.write(f"ACCOUNT_DATA_STATUS={p['account_status']} · BLIND_TEST_READY={p['blind_ready']}")
        st.caption(f"UNKNOWN_FIELD_COUNT={p['unknown_count']} · UNKNOWN_STOCK_COUNT={p['unknown_stock_count']}")
    st.info('RESEARCH_MODE=YES · 公司名单来自本次上传CSV；截图连续覆盖由提供者声明，未重新检查原截图。三个Top50沿用现有输出，原历史认证缺口另行保留。')
    if p['blind_ready']=='NO':st.warning('身份已取得，仍有真实路径缺口。请连同UNKNOWN明细交给高级助理，不伪造盲测就绪。')
    text=export_text(p)
    encoded=json.dumps(text,ensure_ascii=True).replace('<','\\u003c')
    components.html('''<button id="copy" style="width:100%;padding:15px;background:#172b45;color:white;border:0;border-radius:10px;font-size:16px">复制真实五源盲生成Reality</button><div id="msg" role="status"></div><textarea id="fallback" readonly style="display:none;width:100%;height:120px"></textarea><script>const payload='''+encoded+''';document.getElementById('copy').onclick=async()=>{try{await navigator.clipboard.writeText(payload);document.getElementById('msg').textContent='已复制完整2026-09-30真实五源Reality';}catch(e){const t=document.getElementById('fallback');t.style.display='block';t.value=payload;t.select();document.getElementById('msg').textContent=document.execCommand('copy')?'已复制完整2026-09-30真实五源Reality':'请长按文本全选复制';}};</script>''',height=200)
    st.download_button('下载完整盲生成Reality',text.encode('utf-8-sig'),file_name=PRODUCT+'.txt',mime='text/plain')
    with st.expander('市场认证去重宇宙'):
        columns=['股票','代码','SOURCE_3D_RANK','SOURCE_5D_RANK','SOURCE_10D_RANK','SECOND_BOARD','LEADER_POOL','CURRENT_RETURN','RETURN_1400','RETURN_1430','CURRENT_TO_HIGH','TURNOVER','ACCOUNT_ELIGIBLE']
        st.dataframe(pd.DataFrame(p['rows'])[columns].astype(str),hide_index=True,use_container_width=True)
        chosen=st.selectbox('查看单只完整Reality',range(len(p['rows'])),format_func=lambda i:p['rows'][i]['代码']+' '+p['rows'][i]['股票'])
        st.json(p['rows'][chosen])
    with st.expander('验收回执、输入来源与真实缺口'):
        st.json(receipt(p))
        st.code(text,language=None)


if __name__=='__main__':
    generate()
