import findspark
findspark.init()

from pyspark.sql import SparkSession, DataFrame
import copy
from ups_tool import util as fn_util
logger = fn_util.get_logger()


class Spark:
    def __init__(self) -> None:
        self._spark = SparkSession.builder.appName('upsdata.info').getOrCreate()
        for conf in self._spark.sparkContext.getConf().getAll():
            logger.info(f'{conf[0]}: {conf[1]}')

    def createDataFrame(self, data, schema=None, columns=[]) -> DataFrame:
        if schema:
            return self._spark.createDataFrame(data, schema=schema)
        else:
            return self._spark.createDataFrame(data).toDF(*columns)

    def read_parquet(self, path) -> DataFrame:
        return self._spark.read.parquet(str(path))

    def write_parquet(self, df, path) -> None:
        df.write.mode('overwrite').parquet(str(path))

    def get_csv_options(self, opts) -> dict:
        params = copy.deepcopy(opts)
        for x in ['directory', 'filename', 'filetype', 'filter', 'key', 'make_1_file', 'multiline',
                  'step', 'storage', 'type_cast', 'type_cast_default', 'value', 'compression']:
            if x in params: params.pop(x)
        return params

    def get_csv_options_write(self, opts) -> dict:
        params = self.get_csv_options(opts)
        if 'mode' in params:
            params.pop('mode')
        return params

    def read_csv(self, path, **kwargs) -> DataFrame:
        # change default
        kwargs['multiLine'] = kwargs.get('multiLine', kwargs.get('multiline', False))
        # if kwargs['multiLine']==None:
        #     kwargs['multiLine'] = kwargs.get('multiline', True)
        kwargs['header'] = kwargs.get('header', True)
        kwargs['escape'] = kwargs.get('escape', '"')
        kwargs['mode'] = kwargs.get('mode', 'PERMISSIVE' if kwargs.get('schema', None) else 'FAILFAST')
        # ignore...WhiteSpace: default: read=false, write=true
        kwargs['ignoreLeadingWhiteSpace'] = kwargs.get('ignoreLeadingWhiteSpace', True)
        kwargs['ignoreTrailingWhiteSpace'] = kwargs.get('ignoreTrailingWhiteSpace', True)
        logger.info(kwargs)
        if 'compression' in kwargs:
            return self._spark.read.option('compression', kwargs['compression']).csv(path = str(path), **self.get_csv_options(kwargs))
        return self._spark.read.csv(path = str(path), **self.get_csv_options(kwargs))

    def write_csv(self, df, path, sort=[], one_file=True, **kwargs) -> None:
        # change default
        kwargs['header'] = kwargs.get('header', True)
        kwargs['escape'] = kwargs.get('escape', '"')
        if one_file:
            df = df.coalesce(1)
        if len(sort) > 0:
            df = df.sortWithinPartitions(*sort)
        logger.info(kwargs)
        logger.info(self.get_csv_options_write(kwargs))
        df.write.mode('overwrite').csv(path=str(path), **self.get_csv_options_write(kwargs))
