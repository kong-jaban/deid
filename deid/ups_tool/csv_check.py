import findspark
findspark.init()

from ups_tool import util as fn_util
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
import pandas as pd
from pathlib import Path
import argparse

CORRUPTED = '_corrupt_record'

########################################################################
def main(args):
    print('\t', end='')
    print('\n\t'.join([f'{k}: {v}' for k, v in vars(args).items()]))

    _path_replace = lambda x: x.replace('~', str(Path.home()))

    spark = _get_spark(args)
    print('#' * 40)

    ipath = Path(_path_replace(args.file))
    for f in ipath.parent.glob(ipath.name):
        schema = _make_schema(f, args)
        # print('\n'.join(schema.fieldNames()))
        # continue
        opath = Path(_path_replace(args.out_dir), f.name)
        df = spark.read.csv(str(f),
                        encoding=args.encoding,
                        sep=args.sep,
                        header=args.header,
                        mode='PERMISSIVE',
                        schema=schema,
                        ignoreLeadingWhiteSpace=True,
                        ignoreTrailingWhiteSpace=True,
                    )
        # <-- 변경 금지. 반드시 이렇게 해야 제대로 파악함
        df.write.mode('overwrite').parquet(f'{opath}.parquet')
        df = spark.read.parquet(f'{opath}.parquet')
        corrupted_count = df.filter(df[CORRUPTED].isNotNull()).count()
        # -->
        print(f'{f} => {"ok" if corrupted_count == 0 else f"fail: {corrupted_count}"}')
        if corrupted_count > 0:
            # print('\t{}'.format('\n\t'.join(schema.fieldNames()[:-1])))
            print(f'column: {schema.fieldNames()[:-1]}')
            for row in df.filter(df[CORRUPTED].isNotNull()).select(CORRUPTED).limit(10).collect():
                print(f'[{row[0]}]')

########################################################################
def _make_schema(path, args):
    schema = []
    df = pd.read_csv(path, encoding=args.encoding, header=0 if args.header else None, nrows=1)
    columns = df.columns if args.header else [f'_c{x}' for x in range(df.shape[1])]
    for c in columns:
        schema.append(StructField(c, StringType(), True))

    schema.append(StructField(CORRUPTED, StringType(), True))
    return StructType(schema)

########################################################################
def _get_spark(args):
    configs = {
        'spark.driver.bindAddress': '127.0.0.1',
        'spark.driver.memory': '32g',
        'spark.driver.maxResultSize': '16g',
        'spark.local.dir': args.tmp_dir,
        'spark.sql.autoBroadcastJoinThreshold': -1,
        'spark.sql.caseSensitive': 'false',
        'spark.sql.debug.maxToStringFields': 2000,
        'spark.sql.columnNameOfCorruptRecord': CORRUPTED,
    }
    conf = SparkConf().setAll([(k,v) for k, v in configs.items()])
    builder = SparkSession.builder.appName('ups').config(conf=conf)
    return builder.getOrCreate()

########################################################################
# python ups_tool/csv_check.py --file=~/data/_test/financedata.utf8.corrupted.csv \
#   --encoding=utf-8 --sep=, --header --out_dir=~/data/corrupted --tmp_dir=~/data/tmp
#   --encoding=utf-8 --sep=, --no-header --out_dir=~/data/corrupted --tmp_dir=~/data/tmp
if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                     description='CSV file checker')
    parser.add_argument('--info', required=True, action=fn_util.PathAction,
                        help='csv2info 로 생성한 작업 액셀 파일 경로')
    parser.add_argument('--file', required=True, action=fn_util.PathAction,
                        help='데이터 파일 경로')
    parser.add_argument('--encoding', default='utf-8', help='데이터 파일 인코딩 (기본값: %(default)s)')
    parser.add_argument('--sep', default=',', help='컬럼 구분자 (기본값: %(default)s)')
    parser.add_argument('--header', default=True, action=argparse.BooleanOptionalAction, help='헤더 유무 (기본값: --header)')
    # parser.add_argument('--multiline', default=True, action=argparse.BooleanOptionalAction, help='데이터에 CR, LF가 포함된 경우 (기본값: --multiline)')
    parser.add_argument('--out_dir', default=fn_util.get_path('~/data/corrupted'), action=fn_util.PathAction,
                        help='오류 파일 저장 디렉토리')
    parser.add_argument('--tmp_dir', default=fn_util.get_path('~/data/tmp'), action=fn_util.PathAction,
                        help='임시 디렉토리 (기본값: %(default)s)')
    args = parser.parse_args()
    main(args)
