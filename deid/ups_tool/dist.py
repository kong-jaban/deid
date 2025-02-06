import findspark
findspark.init()
from pyspark.sql import SparkSession

import ups_fn.run
import ups_tool.dist2xls
from ups_tool import util as fn_util
import argparse
from pathlib import Path
import os
# import dask
# import dask.dataframe as dd
# from loky import get_reusable_executor
import pandas as pd
import yaml

EXCLUE_COLUMNS = ['_rn', '_jk', 'seq', '일련번호', 'id', '_corrupt_record']
PARSE_INFO_SHEET = 'parseinfo'
SHEMA_SHEET = '02.컬럼정보'
COL_NO = 'C'
COL_NAME = 'D'
COL_DESC = 'E'
CATALOG = 'catalog'

# _to_list = lambda x, sep: x.split(sep) if x else []
# _make_regex = lambda x: '.*'.join([re.escape(c) for c in x.split('*')])

########################################################################
def main(args):
    print('\n'.join([f'{k}: {v}' for k, v in vars(args).items()]))

    configs, yml_path = make_yml(args)

    run_args = argparse.Namespace(
        catalog = yml_path,
        jobs=args.jobs,
        output_sum=args.output_sum,
        storage=args.storage,
    )
    ups_fn.run.main(run_args)

    # xls_path = Path(configs['storage'], f'분포.{fn_util.get_file_postfix()}.xlsx')
    # if 'dist_old' in args.jobs:
    #     dist2xls_args = argparse.Namespace(
    #         catalog = configs['catalog'],
    #         encoding = configs['dist_encoding'],
    #         dist_old = str(Path(configs['storage'], 'step_dist_old')),
    #         # dist_new = str(Path(configs['storage'], 'step_dist_new')),
    #         schema_file = args.info,
    #         sheet = SHEMA_SHEET,
    #         header = 1,
    #         number = COL_NO,
    #         name = COL_NAME,
    #         desc = COL_DESC,
    #         out_xls = xls_path,
    #         max_rows = 1000,
    #         shorten = False,
    #     )
    #     xls_path = ups_tool.dist2xls.main(dist2xls_args)



########################################################################
def make_yml(args):
    parse_info = get_parse_info(args)

    filepath = Path(args.file)
    configs = {}
    configs['customer'] = '고객사명'
    configs['catalog'] = CATALOG
    configs['storage'] = args.storage if args.storage else str(filepath.parent)
    configs['dist_encoding'] = parse_info['dist_encoding']
    configs['salt'] = '솔트'
    configs['data_with_jk'] = False

    configs['step_org'] = {}
    configs['step_org']['in'] = {}
    configs['step_org']['in']['directory'] = str(filepath.parent)
    configs['step_org']['in']['filename'] = filepath.name
    configs['step_org']['in']['encoding'] = parse_info['encoding']
    configs['step_org']['in']['header'] = parse_info['has_header']
    configs['step_org']['in']['sep'] = parse_info['sep']
    configs['step_org']['in']['multiLine'] = True
    configs['step_org']['in']['nanValue'] = parse_info['nanValue']
    if parse_info.get('nullValue'):
        configs['step_org']['in']['nullValue'] = parse_info['nullValue']
    configs['step_org']['in']['schema'] = get_schema(args)

    configs['step_dist_old'] = {}
    configs['step_dist_old']['include'] = get_dist_columns(args)

    configs['step_sum_old'] = {}
    configs['step_sum_old']['opt'] = {'lower': 'right', 'upper': 'left'}
    configs['step_sum_old']['include'] = get_sum_columns(args)

    opath = Path(filepath.parent, 'c.customer.yml')
    with open(opath, 'w', encoding='utf-8') as f:
        yaml.dump(configs, f, allow_unicode=True)
    print(f'catalog yml  ==>  {opath}')
    return configs, opath

########################################################################
def get_sum_columns(args):
    df = pd.read_excel(args.info, sheet_name=SHEMA_SHEET, header=0)
    return df[df['집계'].isin(['o','O'])]['컬럼명'].to_list()

########################################################################
def get_dist_columns(args):
    df = pd.read_excel(args.info, sheet_name=SHEMA_SHEET, header=0)
    return df[~df['분포'].isin(['x','X'])]['컬럼명'].to_list()

########################################################################
def get_parse_info(args):
    parse_info = {}
    df = pd.read_excel(args.info, sheet_name='parseinfo', header=None)
    for ii, row in df.iterrows():
        parse_info[row[0]] = row[1]
    return parse_info

########################################################################
def get_schema(args):
    schema = []
    df = pd.read_excel(args.info, sheet_name=SHEMA_SHEET, header=0)
    for ii, row in df.iterrows():
        schema.append({'name': row['컬럼명'], 'data_type': row['데이터타입'], 'nullable': True})
    return schema

# ########################################################################
# def dist_spark(configs, args):
#     spark = SparkSession.builder.appName('ups').getOrCreate()

#     ddl = [f'{c} {v}' for c, v in configs['columns'].items()]

#     df = spark.read.csv(str(args.file),
#                     encoding=configs['encoding'],
#                     sep=configs['sep'],
#                     header=configs['has_header'],
#                     mode='PERMISSIVE',
#                     schema=','.join(ddl),
#                     ignoreLeadingWhiteSpace=True,
#                     ignoreTrailingWhiteSpace=True,
#                     nanValue=configs['nanValue'],
#                     nullValue=configs['nullValue']
#                 )
#     for c in configs['dist_columns']:
#         opath = Path(args.out_dir, f'{c}.csv')
#         dx = df.groupby(c).count().toPandas()
#         dx.to_csv(opath, header=True, index=False, encoding='cp949')
#         print(f'{c} => {opath}')

# ########################################################################
# def dist_dask(args):
#     dask.config.set({
#         'dask.temporary-directory': args.tmp_dir,
#         'dask.dataframe.shuffle-compression': None,
#     })
#     dask.config.set(scheduler=get_reusable_executor())

#     df = dd.read_csv(args.file,
#                     encoding=args.encoding,
#                     sep=args.sep,
#                     header=0 if args.header else None,
#                     dtype=str,
#                     skipinitialspace=True,
#                     na_values= [],
#                    )
#     p_nu = re.compile('|'.join([_make_regex(c) for c in _to_list(args.numeric_columns, ',')]), re.IGNORECASE)
#     for c in [c for c in df.columns if p_nu.search(c)]:
#         df[c] = dd.to_numeric(df[c], errors='coerce')
#     # print(df.head())

#     p_in = re.compile('|'.join([_make_regex(c) for c in _to_list(args.include_columns, ',')]), re.IGNORECASE)
#     p_ex = re.compile('|'.join([_make_regex(c) for c in _to_list(args.exclude_columns, ',')]), re.IGNORECASE)
#     include_columns = [c for c in df.columns if (not p_ex.search(c)) and (p_in.search(c))]

#     for c in list(set(include_columns) - set(EXCLUE_COLUMNS)):
#         opath = Path(args.out_dir, f'{c}.csv')
#         # dx = df.groupby(by=c, dropna=False)[c].count()
#         dx = df.groupby(by=c, dropna=False)[c].agg({c:['count']})
#         dx = dx.sort_values('count' if args.sort_count else c, na_position='first')
#         dx.to_csv(opath, encoding='euc-kr', single_file=True)
#         print(f'{c} => {opath}')

########################################################################
# python ups_tool/distribution.py --file=~/data/financedata.csv \
#   --out_dir=~/data/test --tmp_dir=~/data \
#   --include_columns=seq,*name*,bithdate,age,gender,address,zipcode,phonenum
#   --numeric_columns=seq,age
#   --exclude_columns=gender,address \
if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                     description='데이터 파일로부터 컬럼별로 분포/집계 뽑기')
    # todo #1
    #   설정파일로 파라미터 받기
    #   group으로 상호배제
    # todo
    #   dist2xls 호출 여부에 따라 호출 하기/안하기
    parser.add_argument('--info', required=True, action=fn_util.PathAction, help='csv2info 로 생성한 작업 액셀 파일 경로')
    parser.add_argument('--file', required=True, action=fn_util.PathAction, help='데이터 파일 경로')
    # parser.add_argument('--out_dir', required=True, action=fn_util.PathAction, help='분포 파일 저장 디렉토리')
    # parser.add_argument('--tmp_dir', default=fn_util.get_path('~/data/tmp'), help='임시 디렉토리 (기본값: %(default)s)')
    # parser.add_argument('--sort_count', default=False, action=argparse.BooleanOptionalAction,
    #                     help='count 기준 정렬 여부 (기본값: %(default)s)')
    parser.add_argument('--jobs', default='org,dist_old,sum_old', help='수행 단계 지정 (기본값: %(default)s)\norg: 처음 1회 필수\ndist_old: 분포\nsum_old: 연속형')
    parser.add_argument('--output_sum', default=None, help=f'저장할 xlsx 파일명')
    parser.add_argument('--storage', default='', help=f'저장할 디렉토리')
    # parser.add_argument('--encoding', default='utf-8', help='데이터 파일 인코딩 (기본값: %(default)s)')
    # parser.add_argument('--sep', default=',', help='컬럼 구분자 (기본값: %(default)s)')
    # parser.add_argument('--header', default=True, action=argparse.BooleanOptionalAction, help='헤더 유무 (기본값: --header)')
    # parser.add_argument('--data_type_default', default='string', help='기본 데이터 타입 (기본값: %(default)s)')
    # parser.add_argument('--data_types', action='append', help='컬럼 데이터 타입. 컬럼명:데이터타입 형태')
    # parser.add_argument('--numeric_columns', help='숫자형 컬럼\n"," 넣어서 여러 컬럼 지정\n"*" 가능\n대소문자 구분 하지 않음')
    # parser.add_argument('--include_columns', help='분포/집계 뽑을 컬럼\n지정하지 않으면 모든 컬럼\n","로 구분하여 여러 컬럼 지정\n"*" 가능')
    # parser.add_argument('--exclude_columns', help=f'분포/집계 뽑지 않을 컬럼\n","로 구분하여 여러 컬럼 지정\n"*" 가능\n대소문자 구분 하지 않음\n지정하지 않아도 {", ".join(EXCLUE_COLUMNS)} 컬럼은 제외함')
    args = parser.parse_args()
    main(args)
