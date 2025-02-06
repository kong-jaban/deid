from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, IntegerType, LongType, DecimalType, FloatType, DateType, TimestampType, DoubleType
import re
from pathlib import Path
from ups_fn import fn
from ups_tool import util as fn_util
logger = fn_util.get_logger()

###########################################
backtick = lambda x: '`' + x + '`'

###########################################
def make_schema(cfg):
    if not cfg:
        return None

    def _data_type(c):
        x = c['data_type'].lower()
        if x in ['str', 'string']: return StringType()
        if x in ['int', 'integer']: return IntegerType()
        if x in ['long']: return LongType()
        if x in ['date']: return DateType()
        if x in ['timestamp']: return TimestampType()
        if x in ['float']: return FloatType()
        if x in ['double']: return DoubleType()
        if x.startswith('dec'):
            return DecimalType(c['precision'], c['scale'])

    schema = []
    for c in cfg:
        # logger.info(f'\tmake_schema: {c}')
        schema.append(StructField(c['name'], _data_type(c), c['nullable']))

    return StructType(schema) if schema else None

###########################################
def create_dataframe(spark, data=[], columns=[], schema=None):
    return spark.createDataFrame(data, schema=schema, columns=columns)

###########################################
def load_data(spark, storage, filename):
    df = None

    if re.search('[\*\?]', str(filename)):
        f_list = Path(storage['directory']).glob(filename)
    else:
        f_list = [Path(storage['directory'], filename)]

    if 'schema' in storage:
        storage['schema'] = make_schema(storage['schema'])
    for path in f_list:
        logger.info(f'\t{path.suffix}: {path}')
        if path.suffix == '.parquet':
            dx = spark.read_parquet(path)
        else:   # storage['filetype'] == 'csv':
            dx = spark.read_csv(path=path, **storage)
                # sep = storage['sep'] if 'sep' in storage else ',',
                # encoding = storage['encoding'] if 'encoding' in storage else 'utf-8',
                # header = storage['header'] if 'header' in storage else True,
                # mode = storage['mode'] if 'mode' in storage else 'FAILFAST',
                # inferSchema = storage['inferSchema'] if 'inferSchema' in storage else False,
                # schema = schema,
                # multiLine = storage['multiLine'] if 'multiLine' not in storage else False,
            # )
        df = dx if df is None else df.union(dx.select(*df.columns))

    if df:
        if 'schema' not in storage:
            default_type = storage['type_cast_default'] if 'type_cast_default' in storage else 'string'
            for c in df.columns:
                dtype = storage['type_cast'][c] if ('type_cast' in storage) and (c in storage['type_cast']) else default_type
                if dtype in ['int', 'integer', 'long', 'float', 'double']:
                    df = df.withColumn(c, F.replace(c, F.lit(','), F.lit('')).cast(dtype))
                elif dtype != 'string':
                    df = df.withColumn(c, df[c].cast(dtype))

        if ('filter' in storage) and (storage['filter']):
            df = fn.filtering(df, storage)
            # df = df.filter(fn._convert_expr(None, None, None, storage['filter']))
    return df

###########################################
def save_data(spark, df, storage, filename):
    out_path = Path(storage['directory'], filename)

    if out_path.suffix == '.parquet':
        df.write.mode('overwrite').parquet(str(out_path))
    else:   # csv
        sortorder = []
        if 'orderby' in storage:
            for c in storage['orderby']:
                k, v = list(c.items())[0]
                sortorder.append(df[k].desc() if v == 'desc' else df[k].asc())

        spark.write_csv(
            df=df,
            path=out_path,
            sort=sortorder,
            one_file=('make_1_file' in storage) and (storage['make_1_file']),
            **storage,
        )
    return out_path

###########################################
def remove_duplicates(spark, df, prefix, params):
    print('\t', 'remove_duplicates')
    cnt_before = df.count()
    df = df.dropDuplicates()
    cnt_after = df.count()
    print(f'\t\tremove duplicates: {cnt_before} => {cnt_after}')
    return df

###########################################
def select(spark, df, prefix, params):
    # select 컬럼과 filter 컬럼이 다를 수 있으므로 filter를 항상 먼저 수행
    if 'filter' in params:
        print(f'\t\t{params["filter"]}')
        if list == type(params['filter']):
            for flt in params['filter']:
                df = df.filter(flt)
        else:
            df = df.filter(params['filter'])

    if 'groupby_count' in params:
        print(f'\t\t{params["groupby_count"]}')
        df = df.groupBy(*[c for c in params['groupby_count']]).count()

    if 'column' in params:
        kv = [list(c.items())[0] for c in params['column']] if list == type(params['column']) else params['column'].items()
        df = df.selectExpr(*[f'{backtick(v)} as {backtick(k)}' for k, v in kv])
        print(df.columns)

    return df
