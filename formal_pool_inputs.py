"""Original approved company members; source identity is never re-dated."""
import csv
import hashlib
import io
import re
from pathlib import Path

FILES = {
    'second': ('二板总池_2026-09-30.csv', '62954fff51ed399243abb6a6898675f0b7d754c6', 137),
    'leader': ('总龙头池_2026-09-30.csv', 'd2d01c278e38c27d80934ec74c32285d7f43cb9a', 136),
}

def load(root=None):
    result = {}
    root = Path(root or Path(__file__).parent)
    for kind, (name, expected, count) in FILES.items():
        raw = (root / name).read_bytes()
        sha = hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()
        if sha != expected:
            raise ValueError('FORMAL_POOL_SOURCE_CHANGED:'+kind)
        rows = list(csv.DictReader(io.StringIO(raw.decode('utf-8-sig'))))
        codes = [r['股票代码'] for r in rows]
        if len(codes) != count or len(set(codes)) != count or any(not re.fullmatch(r'[0-9]{6}', c) for c in codes):
            raise ValueError('FORMAL_POOL_IDENTITY_INVALID:'+kind)
        result[kind] = '\n'.join(codes)
    result['source_version'] = 'COMPANY-REAL-SEED-2026-09-30'
    result['source_date'] = '2026-09-30'
    result['scope'] = 'APPROVED_MEMBER_INHERITANCE; CURRENT_QUOTES_REQUIRED; NOT_NEW_POOL_MAINTENANCE'
    return result
