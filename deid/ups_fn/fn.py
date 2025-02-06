import base64
from pathlib import Path
from ups_fn.spark import Spark
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W
from ups_tool import util as fn_util
from functools import reduce
import re
# from Cryptodome import SHA256, SHAKE128, SHAKE256
# from Cryptodome.Hash import SHA256, SHAKE128, SHAKE256


logger = fn_util.get_logger()

c_idx = lambda c, x: f'_x_{c}_{x}'
c_map = lambda c: f'_m_{c}'

###########################################
# yml 내 expr 등에서 사용하는 함수
# pyspark.sql.functions + a
lit = F.lit
sha2 = lambda x, bit: F.sha2(x, bit)
sha256_concat = lambda sep, *x: sha2(F.concat_ws(sep, *x), 256)
sha256 = sha256_concat
concat = lambda *x: F.concat_ws('', *x)
concat_ws = F.concat_ws
ceil = lambda x, y: F.ceil(x / F.pow(10, (0-y))) * F.pow(10, (0-y))
floor = lambda x, y: F.floor(x / F.pow(10, (0-y))) * F.pow(10, (0-y))
round45 = F.round
rand_round = lambda x, y, z='long': F.format_string(f'%.{y-1}e', x.cast('float')).cast('float').cast(z)
replace_string = F.replace
substring = F.substring
lower = F.lower
upper = F.upper
to_date = F.to_date
datediff = F.datediff
dateadd = F.dateadd
year = F.year
month = F.month
# rand_range = lambda x, y: (F.rand() * (y - x + 2) + x - 1).cast('int')
rand_range = lambda x, y, z='long': (F.rand() * (y - x + 1)).cast(z) + x
regexp_extract = F.regexp_extract
regexp_replace = F.regexp_replace

###########################################
def _convert_expr(c, idx, params, expr, dfname=None):
    def _convert_key(m):
        # logger.info(m.groups()[0])
        x = int(m.groups()[0])
        x = x if x >= 0 else params['idxs'][params['idxs'].index(idx) + x]
        cname = c if x == 0 else c_idx(c, x)
        return cname if dfname is None else f'{dfname}["{cname}"]'

    def _convert_cols(m):
        # logger.info(m.groups()[0])
        x = int(m.groups()[0])
        return params['cols'][x] if dfname is None else f'{dfname}["{params["cols"][x]}"]'

    def _convert_fns(m):
        x = m.groups()[0]
        return f'F.{x}'

    expr_str = None
    if expr is not None:
        expr_str = str(expr)
        expr_str = re.sub(r'\{(-?\d+)\}', _convert_key, expr_str)
        expr_str = re.sub(r'\{\$(\d+)\}', _convert_cols, expr_str)
        expr_str = re.sub(r'\{#([^\}]+)\}', _convert_fns, expr_str)
    logger.info(expr_str)
    return expr_str

###########################################
def add_null_columns(df, cs):
    for c in cs:
        df = df.withColumn(c, F.lit(None).cast('string'))
    return df

###########################################
def filtering(df, params):
    expr = _convert_expr('', -1, params, params['filter'], 'df')
    logger.info(expr)
    df = df.filter(expr)
    return df

###########################################
def add_row_number(df, c, sequential=False):
    logger.info(c)
    df = df.withColumn(c, F.monotonically_increasing_id())
    if sequential:
        columns = [x for x in df.columns if x != c]
        logger.info(columns)
        logger.info(f'tmp_{c}')
        df = df.withColumn(f'tmp_{c}', F.row_number().over(W.orderBy(df[c].desc())))
        df = df.select(*[df[x] for x in columns], df[f'tmp_{c}'].alias(c))
    return df

###########################################
def groupby_count(df, c_base, idx, params):
    logger.info(f'c_base: {c_base}')
    logger.info(f'idx: {idx}')
    logger.info(f'params: {params}')
    ix_cfg = params[idx]
    by_column = eval(_convert_expr(c_base, idx, params, ix_cfg['by']))
    dx = df.groupby(*by_column).count()
    dx = dx.select(*by_column, dx['count'].alias(c_idx(c_base, idx)))
    df = df.join(dx, on=by_column, how='left')
    return df

# ###########################################
# def sha256(df, c_base, idx, params):
#     ix_cfg = params[idx]
#     logger.info(ix_cfg)

#     ###########################################
#     @F.udf()
#     def _udf(c):
#         if c is None: return None
#         msg = f'{ix_cfg["salt"]}{c}' if ix_cfg['salt_first'] else f'{c}{ix_cfg["salt"]}'
#         h = SHA256.new()
#         h.update(msg.encode(ix_cfg['enc_in'] if 'enc_in' in ix_cfg else 'utf-8'))
#         v = h.digest()
#         return v.hex() if ix_cfg['enc_out'] == 'hex' else base64.b64encode(v).decode('utf-8')

#     ###########################################
#     expr_str = _convert_expr(c_base, idx, params, ix_cfg['expr'], 'df')
#     logger.info(expr_str)
#     df = df.withColumn(c_idx(c_base, idx), _udf(eval(expr_str)))
#     return df

###########################################
def hash(df, c_base, idx, params):
    ix_cfg = params[idx]
    logger.info(ix_cfg)

    ###########################################
    @F.udf()
    def _udf(c):
        if c is None: return None

        def _not_defined_set_default(o, k, v):
            if (k not in o) or (o[k] is None):
                o[k] = v
            return o[k]

        minmax = {
            'SHAKE256': range(1, 64+1),
            'SHAKE128': range(1, 32+1),
            'BLAKE2b': range(1, 64+1),
            'BLAKE2s': range(1, 32+1),
            'TupleHash256': range(8, 64+1),
            'TupleHash128': range(8, 64+1),
            'SHA3_512': [64],
            'SHA3_384': [48],
            'SHA3_256': [32],
            'SHA3_224': [28],
            'SHA512': [28, 32, 64],
            'SHA384': [48],
            'SHA256': [32],
            'SHA224': [28],
        }

        _not_defined_set_default(ix_cfg, 'salt_first', False)
        _not_defined_set_default(ix_cfg, 'algorithm', 'SHA256')
        _not_defined_set_default(ix_cfg, 'encode', 'hex')
        _not_defined_set_default(ix_cfg, 'bytes', max(minmax[ix_cfg['algorithm']]))
        if ix_cfg['bytes'] not in minmax[ix_cfg['algorithm']]:
            ix_cfg['bytes'] = max(minmax[ix_cfg['algorithm']])

        msg = f'{ix_cfg["salt"]}{c}' if ix_cfg['salt_first'] else f'{c}{ix_cfg["salt"]}'
        
        args = {}
        if ix_cfg['algorithm'] == 'SHA512': args['truncate'] = ix_cfg['bytes'] * 8
        if ix_cfg['algorithm'].startswith('TupleHash'): args['digest_bytes'] = ix_cfg['bytes']
        if ix_cfg['algorithm'].startswith('BLAKE'): args['digest_bytes'] = ix_cfg['bytes']

        hash_module = __import__(f'Cryptodome.Hash.{ix_cfg["algorithm"]}', fromlist=['Cryptodome.Hash'])
        h = hash_module.new(**args)
        h.update(msg.encode('utf-8'))
        v = h.read(ix_cfg['bytes']) if ix_cfg['algorithm'].startswith('SHAKE') else h.digest()
        return v.hex() if ix_cfg['encode'].lower() == 'hex' else base64.b64encode(v).decode('utf-8')

    ###########################################
    expr_str = _convert_expr(c_base, idx, params, ix_cfg['origin'], 'df')
    logger.info(expr_str)
    df = df.withColumn(c_idx(c_base, idx), _udf(eval(expr_str)))
    return df

###########################################
def mapping(df, c_base, idx, params, dm):
    logger.info(f'{c_base}: {idx}: {params}')
    ix_cfg = params[idx]
    map_cfg = ix_cfg['map']

    match_cols = _convert_expr(c_base, idx, params, ix_cfg['match'])
    logger.info(match_cols)
    match_cols = eval(match_cols)

    add_column_name = c_idx(c_base, idx)
    if type(map_cfg['value']) == list:
        dm = dm.select(
            *[dm[x].alias(c_map(match_cols[ii])) for ii, x in enumerate(map_cfg['key'])],
            F.array(*[dm[x] for x in map_cfg['value']]).alias(add_column_name)
        )
    else:   # str
        dm = dm.select(
            *[dm[x].alias(c_map(match_cols[ii])) for ii, x in enumerate(map_cfg['key'])],
            dm[map_cfg['value']].alias(add_column_name)
        )

    columns = df.columns
    join_condition = reduce(lambda x, y: x & y, [df[k].eqNullSafe(dm[c_map(k)]) for k in match_cols])
    df = df.join(dm, join_condition, 'left')
    # df = df.select(*[x for x in df.columns if x not in [c_map(k) for k in match_cols]])
    df = df.select(*columns, add_column_name)
    return df

###########################################
person_counter = mapping

###########################################
def person_noise(df, c_base, idx, params, dm):
    df = mapping(df, c_base, idx, params, dm)
    add_column_name = c_idx(c_base, idx)
    noise_name = c_idx('noise', idx)
    df = df.select(*[c for c in df.columns if c != add_column_name], df[add_column_name].alias(noise_name))

    org = _convert_expr(c_base, idx, params, params[idx]['origin'], 'df')
    df = df.withColumn(add_column_name, eval(org) + df[noise_name])
    return df

###########################################
def person_noise_date(df, c_base, idx, params, dm):
    df = mapping(df, c_base, idx, params, dm)
    add_column_name = c_idx(c_base, idx)
    noise_name = c_idx('noise', idx)
    # print(f'{add_column_name} => {noise_name}')
    df = df.select(*[c for c in df.columns if c != add_column_name], df[add_column_name].alias(noise_name))
    
    org = _convert_expr(c_base, idx, params, params[idx]['origin'], 'df')
    df = df.withColumn(add_column_name, eval(org) + F.make_interval(days=df[noise_name][0], mins=df[noise_name][1]))
    # df = df.withColumn(add_column_name, org + F.expr(f'interval {df[noise_name][0]} day {df[noise_name[1]]} minute'))
    return df

###########################################
def noise(df, c_base, idx, params):
    ix_cfg = params[idx]
    org = _convert_expr(c_base, idx, params, params[idx]['origin'], 'df')
    df = df.withColumn(c_idx(c_base, idx), eval(org) + rand_range(*ix_cfg['range']))
    return df

###########################################
def noise_date(df, c_base, idx, params):
    ix_cfg = params[idx]
    org = _convert_expr(c_base, idx, params, params[idx]['origin'], 'df')
    df = df.withColumn(c_idx(c_base, idx),
                       eval(org) + F.make_interval(days=rand_range(*ix_cfg['range_day']), mins=rand_range(*ix_cfg['range_minute'])))
    return df

###########################################
def replace_regexp(df, c_base, idx, params):
    ix_cfg = params[idx]

    c_match = _convert_expr(c_base, idx, params, ix_cfg['match'])
    for ex in ix_cfg['expr']:
        c_str = _convert_expr(c_base, idx, params, ex['c'], 'df')
        v_str = _convert_expr(c_base, idx, params, ex['v'], 'df')
        logger.info(f'{c_str}: {v_str}')
        df = df.withColumn(c_idx(c_base, idx), F.regexp_replace(df[c_match], c_str, v_str))
        c_match = c_idx(c_base, idx)
    return df

###########################################
def replace(df, c_base, idx, params):
    ix_cfg = params[idx]
    ix_cfg['type'] = 'string' if 'type' not in ix_cfg else ix_cfg['type']

    ###########################################
    # 반올림: 사사오입
    # _round45 = lambda x, y: round(x+10**(-len(str(x))-1), y)
    # @F.udf(ix_cfg['type'])
    # def round45(x, y=F.lit(0)):
    #     return None if x is None else int(_round45(x, y)) if y <= 0 else _round45(x, y)

    ###########################################
    # 랜덤라운딩: return float. y: 남길 앞 자리 개수
    # ex) rand_round45(1234, 2)  =>  1200
    # @F.udf(ix_cfg['type'])
    # def rand_round45(x, y):
    #     return None if x is None else int(_round45(x, y-len(str(abs(x)))))

    ###########################################
    else_str = _convert_expr(c_base, idx, params, ix_cfg['else'], 'df')
    ow = F.lit(None).cast(ix_cfg['type']) if else_str is None else eval(else_str)

    if ('expr' not in ix_cfg) or (ix_cfg['expr'] is None):
        df = df.withColumn(c_idx(c_base, idx), ow)
        return df

    w = F
    for ex in ix_cfg['expr']:
        c_str = _convert_expr(c_base, idx, params, ex['c'], 'df')
        v_str = _convert_expr(c_base, idx, params, ex['v'], 'df')
        logger.info(f'{c_str}: {v_str}')
        if 'r' in ex:
            w = w.when(eval(f'{c_str}.rlike("{ex["r"]}")'), F.lit(None).cast(ix_cfg['type']) if v_str is None else eval(v_str))
        else:
            w = w.when(eval(c_str), F.lit(None).cast(ix_cfg['type']) if v_str is None else eval(v_str))
    w = w.otherwise(ow)
    df = df.withColumn(c_idx(c_base, idx), w)
    return df

###########################################
def _get_high_low(df, c, v, opt):
    l_max = None if 'left' not in opt else df.filter(df[c] <= v).select(F.max(c)).collect()[0][0]
    h_min = None if 'right' not in opt else df.filter(df[c] >= v).select(F.min(c)).collect()[0][0]
    return [l_max, v, h_min]

###########################################
# IQR (InterQuartile Range)
#   4분범위 중 25%~75% (중앙값에서 아래위 25%)
def get_iqr(df, c, opt={}):
    if df.select(c).dtypes[0][1] == 'string':
        raise Exception(f'1.5 iqr: not support string column: {c}')

    exact = opt['exact'] if 'exact' in opt else  0.5
    exact = 0.5 if (exact < 0) or (exact > 1) else exact
    q1, q3 = df.approxQuantile(c, [0.25, 0.75], exact)      # 0: exact & expensive ~ 1
    iqr = q3 - q1

    v_low, v_high = [None,None,None], [None,None,None]
    if 'lower' in opt:
        bound_lower = q1 - (1.5 * iqr)
        v_low = _get_high_low(df, c, bound_lower, opt['lower'])
    if 'upper' in opt:
        bound_upper = q3 + (1.5 * iqr)
        v_high = _get_high_low(df, c, bound_upper, opt['upper'])
    return v_low + v_high

###########################################
# 시그마 규칙
#   다른말: 68-95-99.7규칙, 경험적인 규칙
#   평균에서 양쪽 3표준편차의 범위에 거의 모든 값(99.7%)이 들어간다
#   Z-score = avg / stddev(standard deviation)
#       +-1: 68.2%
#       +-2: 95.4%
#       +-3: 99.7%
#       bound = avg + (Z-score * stddev)
def get_3sigma(df, c, avg, stddev, zscore=3, opt={}):
    # 0 들어오면 False 처리하므로 반드시 None 비교
    if stddev is None:
        return [None, None]

    f_avg = float(avg)
    v_low, v_high = [None,None,None], [None,None,None]
    if 'lower' in opt:
        bound_lower = f_avg - (stddev * zscore)
        v_low = _get_high_low(df, c, bound_lower, opt['lower'])
    if 'upper' in opt:
        bound_upper = f_avg + (stddev * zscore)
        v_high = _get_high_low(df, c, bound_upper, opt['upper'])
    return v_low + v_high

###########################################
lf_not_null = lambda df, c: df.filter(df[c].isNotNull()).count()
lf_min = lambda df, c: df.select(F.min(c)).collect()[0][0]
lf_max = lambda df, c: df.select(F.max(c)).collect()[0][0]
lf_min_max = lambda df, c: df.select(F.array(F.min(c), F.max(c))).collect()[0][0]
lf_percentile = lambda df, c, p, acc=10000: df.select(F.percentile_approx(c, p if list==type(p) else [p], acc)).collect()[0][0]
lf_avg = lambda df, c: df.select(F.avg(c)).collect()[0][0]
lf_median = lambda df, c: lf_percentile(df, c, 0.5)
lf_stddev = lambda df, c: df.select(F.stddev(c)).collect()[0][0]
lf_skewness = lambda df, c: df.select(F.skewness(c)).collect()[0][0]
lf_kurtosis = lambda df, c: df.select(F.kurtosis(c)).collect()[0][0]
lf_3sigma = lambda df, c, opt: get_3sigma(df, c, lf_avg(df, c), lf_stddev(df, c), 3, opt)
lf_15iqr = lambda df, c, opt: get_iqr(df, c, opt)

###########################################
def local_delete(df, c_base, idx, params):
    logger.info(f'c_base: {c_base}')
    logger.info(f'idx: {idx}')
    logger.info(f'params: {params}')
    ix_cfg = params[idx]

    if ('under' not in ix_cfg) and ('regexp' not in ix_cfg):
        raise Exception(f'local_delete: must have "under" or "regexp"')

    if 'regexp' in ix_cfg:
        df = local_delete_regexp(df, c_base, idx, params)
    if 'under' in ix_cfg:
        df = local_delete_groupby_count(df, c_base, idx, params)
    return df

###########################################
def local_delete_groupby_count(df, c_base, idx, params):
    logger.info(f'c_base: {c_base}')
    logger.info(f'idx: {idx}')
    logger.info(f'params: {params}')
    ix_cfg = params[idx]

    origin = _convert_expr(c_base, idx, params, ix_cfg.get('origin', c_base))

    dx = df.groupby(origin).count()
    df = df.join(dx, on=[origin], how='left')
    data_type = dict(df.dtypes)[c_base]
    df = df.withColumn(c_idx(c_base, idx),
                       F.when(F.col('count') < ix_cfg['under'], F.lit(None).cast(data_type))
                       .otherwise(F.col(origin)))
    return df

###########################################
def local_delete_regexp(df, c_base, idx, params):
    logger.info(f'c_base: {c_base}')
    logger.info(f'idx: {idx}')
    logger.info(f'params: {params}')
    ix_cfg = params[idx]

    origin = _convert_expr(c_base, idx, params, ix_cfg.get('origin', c_base))

    data_type = dict(df.dtypes)[c_base]
    df = df.withColumn(c_idx(c_base, idx),
                       F.when(F.regexp(origin, F.lit(ix_cfg['regexp'])), F.lit(None).cast(data_type))
                       .otherwise(F.col(origin)))
    return df

###########################################
# 상하단코딩
def top_bottom_coding(df, c_base, idx, params):
    logger.info(f'c_base: {c_base}')
    logger.info(f'idx: {idx}')
    logger.info(f'params: {params}')
    ix_cfg = params[idx]

    top_value = ix_cfg.get('top', None)
    bottom_value = ix_cfg.get('bottom', None)
    fns = ['3sigma', '1.5iqr']
    if (top_value in fns) or (bottom_value in fns):
        opt = {'exact': 0.5}
        if top_value in fns: opt['upper'] = 'left'
        if bottom_value in fns: opt['lower'] = 'right'
        if '3sigma' in [top_value, bottom_value]:
            v_sigma = get_3sigma(df, c_base, lf_avg(df, c_base), lf_stddev(df, c_base), zscore=3, opt=opt)
        if '1.5iqr' in [top_value, bottom_value]:
            v_iqr = get_iqr(df, c_base, opt)
        if top_value == '3sigma': top_value = [v_sigma[4], v_sigma[4]]
        if top_value == '1.5iqr': top_value = [v_iqr[4], v_iqr[4]]
        if bottom_value == '3sigma': bottom_value = [v_sigma[3], v_sigma[3]]
        if bottom_value == '1.5iqr': bottom_value = [v_iqr[3], v_iqr[3]]

    if top_value is None:
        v_max = lf_max(df, c_base)
        top_value = [v_max, v_max]
    if bottom_value is None:
        v_min = lf_min(df, c_base)
        bottom_value = [v_min, v_min]
    df = df.withColumn(c_idx(c_base, idx),
                       F.when(df[c_base] > top_value[0], top_value[1])
                       .when(df[c_base] < bottom_value[0], bottom_value[1])
                       .otherwise(df[c_base]))
    return df
