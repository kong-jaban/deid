from ups_fn.spark import Spark

from ups_tool import util as fn_util
import argparse
from pathlib import Path
from datetime import datetime
from pyspark.sql import functions as F
import math

########################################################################
def add_name(df):
    family_name = [*'김이박최정강조윤장임한오서신권황안송전홍']
    name = [*'영일이삼사오륙칠팔구']
    @F.udf()
    def _udf(x):
        idx = x % 1000
        return f'{family_name[math.floor(idx/100)]}{name[math.floor(idx%100/10)]}{name[math.floor(idx%10)]}'
    df = df.withColumn('name', _udf(df['sid']))
    return df

########################################################################
def add_birthday(df, startday='19450815'):
    days = (datetime.today() - datetime.strptime(startday, '%Y%m%d')).days + 1
    df = df.withColumn('birthday', F.date_add(F.to_date(F.lit(startday), 'yyyyMMdd'), F.pmod(F.col('sid'), days)))
    return df

########################################################################
def add_date(df):
    df = df.withColumn('date1', df['birthday'] + F.make_interval(years=F.lit(10)))
    df = df.withColumn('date2', df['date1'] + F.make_interval(days=F.lit(10)))
    return df

########################################################################
def add_gender(df, genders=['F','M']):
    cols = df.columns
    df = df.withColumn('genders', F.array(*[F.lit(x) for x in genders]))
    df = df.withColumn('gender', df['genders'][F.pmod('sid', 2)])
    df = df.select(*cols, 'gender')
    return df

########################################################################
def add_income(df):
    df = df.withColumn('income', df['sid']*1000)
    return df

########################################################################
def add_disease(df):
    return df

########################################################################
def main(args):
    print('\n'.join([f'{k}: {v}' for k, v in vars(args).items()]))

    spark = Spark()

    # sid
    df = spark.createDataFrame([(x,) for x in range(args.rows)], schema='sid int')

    df = df.transform(add_name)
    df = df.transform(add_birthday, startday='19450815')
    df = df.transform(add_date)
    df = df.transform(add_gender)
    df = df.transform(add_income)
    df = df.transform(add_disease)
    
    opath = Path(args.file)
    opath = Path(opath.parent, f'{opath.stem}_{args.rows}.csv')
    spark.write_csv(df, path=opath, header=True)
    fn_util.csv_folder_to_file(opath)
    # spark.write_parquet(df, opath)
    print(f'\t==>  {opath}')

########################################################################
if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                     description='테스트 데이터 생성')

    parser.add_argument('--file', required=True, action=fn_util.PathAction, help='데이터 파일 경로')
    parser.add_argument('--rows', default=100000, type=int, help='생성할 행 수 (기본값: %(default)s)')
    args = parser.parse_args()
    main(args)
    
# python ups_tool/create_test_data.py --file=~/data/test/org/test_data.csv --rows=10000
