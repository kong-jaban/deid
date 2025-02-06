import os
import sys
from time import sleep
from typing import Any
# if __name__ == '__main__':
#     path=os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
# else:
#     path=os.path.dirname(os.getcwd())
# sys.path.append(path)
# print(path)

import findspark
findspark.init()
from pyspark.sql import SparkSession

from ups_tool.xls import XLS
from ups_tool import util as fn_util
from pathlib import Path
import argparse
from datetime import datetime
from chardet.universaldetector import UniversalDetector
import csv
import pandas as pd
# import dask
# from loky import get_reusable_executor
# import dask.dataframe as dd

XLS_NAME = f'작업.{datetime.now().strftime("%y%m%d%H%M%S")}.xlsx'
CORRUPTED_NAME = f'corrupted.csv'
CORRUPTED_RECORD = '_corrupt_record'
configs = {}
PARSE_INFO_SHEET = 'parseinfo'
SHEMA_SHEET = '02.컬럼정보'
CATALOG = 'catalog'

########################################################################
def main(args):
    print('\t', end='')
    print('\n\t'.join([f'{k}: {v}' for k, v in vars(args).items()]))

    configs = {k:v for k, v in vars(args).items()}
    path = Path(configs['file'])
    configs['out_dir'] = args.storage if args.storage else path.parent
    configs['first_file'] = list(path.parent.glob(path.name))[0]
    
    if configs['encoding'] == '':
        configs['encoding'] = detect_encoding(configs)
    if configs['sep'] == '':
        configs['sep'] = detect_sep(configs)
    if configs['header'].lower() in ['y', 'yes', 't', 'true']:
        configs['has_header'] = True
    elif configs['header'].lower() in ['n', 'no', 'f', 'false']:
        configs['has_header'] = False
    else:
        configs['has_header'] = detect_has_header(configs)
    configs['columns'] = detect_column_info(configs)
    print('\t-----------------------\n\t', end='')
    print('\n\t'.join([f'{k}: {fn_util.to_printable(v)}' for k, v in configs.items()]))

    # if configs['check_corrupt']:
    #     check_corrupted_dask(configs)

    configs['columns'] = convert_spark_type(configs)
    print('\t-----------------------\n\t', end='')
    print('\n\t'.join([f'{k}: {fn_util.to_printable(v)}' for k, v in configs.items()]))
    if configs['check_corrupt']:
        check_corrupted_spark(configs)

    _to_xlsx(configs)
    print(f'numeric: {",".join([c for c, v in configs["columns"].items() if v != "string"])}')

########################################################################
# spark
def check_corrupted_spark(configs):
    spark = SparkSession.builder.appName('ups').getOrCreate()

    ddl = [f'`{c}` {v}' for c, v in configs['columns'].items()]
    ddl.append(f'{CORRUPTED_RECORD} string')

    total_corrupted_count = 0

    ipath = Path(configs['file'])
    for f in ipath.parent.glob(ipath.name):
        df = spark.read.csv(str(f),
                        encoding=configs['encoding'],
                        sep=configs['sep'],
                        header=configs['has_header'],
                        mode='PERMISSIVE',
                        schema=','.join(ddl),
                        ignoreLeadingWhiteSpace=True,
                        ignoreTrailingWhiteSpace=True,
                        nanValue=configs['nanValue'],
                        nullValue=configs['nullValue']
                    )
        opath = str(Path(configs['out_dir'], 'check_corrupt', f'{f.stem}.parquet'))
        # <-- 변경 금지. 반드시 이렇게 해야 제대로 파악함
        df.write.mode('overwrite').parquet(opath)
        df = spark.read.parquet(opath)
        dx = df.filter(df[CORRUPTED_RECORD].isNotNull())
        corrupted_count = dx.count()
        total_corrupted_count += corrupted_count
        # -->
        print('-' * 60)
        print(f'{f} => {"no corrupted" if corrupted_count == 0 else f"corrupted: {corrupted_count}"}')
        if corrupted_count > 0:
            print(configs['columns'])
            for row in dx.select(CORRUPTED_RECORD).limit(10).collect():
                print(f'[{row[0]}]')

    if total_corrupted_count == 0:
        df = spark.read.parquet(str(Path(configs['out_dir'], 'check_corrupt', '*.parquet')))
        df.write.mode('overwrite').parquet(str(Path(configs['out_dir'], 'org_rn', f'{CATALOG}.parquet')))

# ########################################################################
# # dask/pandas
# #   column, value 갯수 안 맞아도 on_bad_lines 에러가 발생하지 않는다
# #   왜 이럴까? 이에 관한 옵션도 없다
# def check_corrupted_dask(configs):
#     corrupted_count = 0
#     corrupted_file = Path(configs['out_dir'], CORRUPTED_NAME)

#     dask.config.set({
#         'dask.temporary-directory': args.tmp_dir,
#         'dask.dataframe.shuffle-compression': None,
#     })
#     dask.config.set(scheduler=get_reusable_executor())

#     def _write_corrupted_line(line):
#         with open(corrupted_file, mode='a+') as f:
#             corrupted_count += 1
#             f.write(line)
#             f.write('\n')
#             f.flush()

#     df = dd.read_csv(configs['file'],
#                      encoding=configs['encoding'],
#                      sep=configs['sep'],
#                      header=0 if configs['has_header'] else None,
#                      dtype=configs['columns'],
#                      skipinitialspace=True,
#                     #  low_memory=False,
#                      on_bad_lines=_write_corrupted_line,
#                      engine='python',
#                     )
#     df.to_csv(Path(configs['out_dir'], f'{CORRUPTED_NAME}.csv'), index=False, header=True)

#     print(f'corrupted: {corrupted_count}')
#     if corrupted_count > 0:
#         print(f'\t==> {corrupted_file if corrupted_count > 0 else "no corrupted"}')

########################################################################
def detect_column_info(configs):
    df = pd.read_csv(configs['first_file'],
                     encoding=configs['encoding'],
                     sep=configs['sep'],
                     header=0 if configs['has_header'] else None,
                     nrows=1000000,
                     low_memory=False,
                    )
    # print(df.dtypes)
    return df.dtypes.apply(lambda x: x.name).to_dict()

########################################################################
def detect_has_header(configs):
    bytes = 4096
    sniffer = csv.Sniffer()
    data = open(configs['first_file'], 'r', encoding=configs['encoding']).read(bytes)
    return sniffer.has_header(data)

########################################################################
def detect_sep(configs):
    bytes = 4096
    sniffer = csv.Sniffer()
    data = open(configs['first_file'], 'r', encoding=configs['encoding']).read(bytes)
    delimiter = sniffer.sniff(data).delimiter
    return delimiter

########################################################################
def detect_encoding(configs):
    detector = UniversalDetector()
    detector.reset()
    for line in open(configs['first_file'], 'rb'):
        detector.feed(line)
        if detector.done: break
    detector.close()
    # print(detector.result)
    return detector.result['encoding']

########################################################################
def convert_spark_type(configs):
    cs = {}
    for c, v in configs['columns'].items():
        cname = c if configs['has_header'] else f'_c{c}'
        dtype = v.replace('int64', 'long').replace('float64', 'float').replace('object', 'string')
        cs[cname] = dtype
    return cs

########################################################################
def _to_xlsx(configs):
    headers = [
        {'name': '테이블번호',  'width': 8.1,   'align': 'center',  'valign': 'top'},
        {'name': '테이블명',    'width': 9.5,   'align': 'center',  'valign': 'top'},
        {'name': '컬럼번호',    'width': 6.6,   'align': 'center',  'valign': 'top'},
        {'name': '컬럼명',      'width': 24.5,  'align': 'left',    'valign': 'top'},
        {'name': '컬럼설명',    'width': 34.0,  'align': 'left',    'valign': 'top'},
        {'name': '처리기법',    'width': 9.6,   'align': 'left',    'valign': 'top'},
        {'name': '처리상세',    'width': 29.8,  'align': 'left',    'valign': 'top'},
        {'name': '분포',        'width': 3.6,   'align': 'center',  'valign': 'top'},
        {'name': '집계',        'width': 3.6,   'align': 'center',  'valign': 'top'},
        {'name': '데이터타입',  'width': 8.4,   'align': 'left',    'valign': 'top'},
    ]

    opath = Path(configs['out_dir'], configs['output_info_xlsx'])
    xls = XLS(font_size=9)
    xls.set_sheet_name(SHEMA_SHEET)
    xls_header(xls, headers)
    xls_body(xls, configs['columns'])
    xls_style(xls, headers, configs['columns'])

    xls.create_sheet(PARSE_INFO_SHEET)
    xls.set_cell(col_no=1, row_no=1, value='dist_encoding')
    xls.set_cell(col_no=2, row_no=1, value='euc-kr')
    for ii, x in enumerate(['encoding', 'sep', 'has_header', 'nanValue', 'nullValue']):
        xls.set_cell(col_no=1, row_no=2+ii, value=x)
        xls.set_cell(col_no=2, row_no=2+ii, value=fn_util.to_printable(configs[x]))

    xls.save(opath)
    print(f'==> {opath}')

###########################################
def xls_body(xls, columns):
    for ii, (c, t) in enumerate(columns.items()):
        xls.set_cell(col_no=3, row_no=2+ii, value=1+ii)
        xls.set_cell(col_no=4, row_no=2+ii, value=c)
        xls.set_cell(col_no=xls.col_no('J'), row_no=2+ii, value=t)

###########################################
def xls_header(xls, headers):
    for ii, c in enumerate(headers):
        wc = xls.set_cell(col_no=1+ii, row_no=1, value=c['name'], bg_color='fff8e1')
        wc = xls.set_cell(col_no=1+ii, row_no=1, value=c['name'], bg_color='fff8e1')

########################################################################
def xls_style(xls, headers, columns):
    end_col_no, end_row_no = len(headers), 1 + len(columns)
    xls.border(start_col_no=1, start_row_no=1, end_col_no=end_col_no, end_row_no=end_row_no,
               border_style='thin', border_color='000000')
    for ii, c in enumerate(headers):
        xls.col_width(1+ii, c['width'])
        xls.alignment(start_col_no=1+ii, start_row_no=1, end_col_no=1+ii, end_row_no=1,
                      align='center', valign='center')
        xls.alignment(start_col_no=1+ii, start_row_no=2, end_col_no=1+ii, end_row_no=end_row_no,
                      align=c['align'], valign=c['valign'])

# ########################################################################
# def _integer_or_long(df, c):
#     # integer: -2147483648 to 2147483647
#     # long: -9223372036854775808 to 9223372036854775807
#     dx = df.select(F.max(F.length(F.regexp_replace(df[c], '-', ''))))
#     return 'integer' if dx.collect()[0][0] < len('2147483647') else 'long'

# ########################################################################
# def _decimal_length(df, c):
#     dx = df.select(F.split(df[c], '.').alias('v'))
#     dx = dx.select(F.max(F.length(dx['v'].getItem(0))), F.max(F.length(dx['v'].getItem(1)))).collect()[0]
#     return dx[0], dx[1]

# ########################################################################
# def _is_all(df, c, rx):
#     dx = df.select(c).filter(df[c].isNotNull())
#     not_null_count = dx.count()
#     cnt = dx.filter(dx[c].rlike(rx)).count()
#     return cnt == not_null_count

########################################################################
# python ups_tool/csv2info.py --file=~/data/_test/financedata.utf8.csv --check-corrupt \
#   --encoding=utf-8 --sep=, --header --out_dir=~/data --tmp_dir=~/data/tmp
#   --encoding=utf-8 --sep=, --no-header --out_dir=~/data --tmp_dir=~/data/tmp
# python ups_tool/csv2info.py --file=~/data/_test/financedata.utf8.corrupted.csv --check-corrupt

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                     description='컬럼정보 from CSV file')
    parser.add_argument('--file', required=True, action=fn_util.PathAction,
                        help='데이터 파일 경로')
    parser.add_argument('--encoding', default='', help='데이터 파일 인코딩')
    parser.add_argument('--sep', default='', help=f'컬럼 구분자')
    parser.add_argument('--header', default='', help='헤더 유무 (true/false)')
    # parser.add_argument('--out_dir', default=fn_util.get_path('~/data'), action=fn_util.PathAction,
    #                     help='오류 파일 저장 디렉토리')
    parser.add_argument('--tmp_dir', default=fn_util.get_path('~/data/tmp'), action=fn_util.PathAction,
                        help='임시 디렉토리 (기본값: %(default)s)')
    parser.add_argument('--nanValue', default='NaN', help='non-number value (기본값: %(default)s)')
    parser.add_argument('--nullValue', default='', help='null value (기본값: %(default)s)')
    parser.add_argument('--check-corrupt', default=True, action=argparse.BooleanOptionalAction,
                        help='csv parsing test (기본값: --check-corrupt)')
    parser.add_argument('--storage', default='', help='저장할 디렉토리')
    parser.add_argument('--output_info_xlsx', default=XLS_NAME, help='저장할 xlsx 파일명')
    args = parser.parse_args()
    main(args)
