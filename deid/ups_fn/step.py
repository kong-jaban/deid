from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent) if __name__ == '__main__' else str(Path().parent))

from pyspark.sql import functions as F
from ups_fn import fn_local
from ups_fn import fn
from ups_tool.elapsed import Elapsed
from ups_tool import util as fn_util
import re
import json
logger = fn_util.get_logger()

step_ins = {
    'step_pre': 'org_rn',
    'step_deid': 'step_pre',
    'step_post_jk': 'step_deid',
    'step_post_old': 'step_pre',
    'step_post_new': 'step_deid',
    'step_sum_org': 'org_rn',
    'step_sum_pre': 'step_pre',
    'step_sum_old': 'step_pre',
    'step_sum_new': 'step_post',
    'step_dist_org': 'org_rn',
    'step_dist_pre': 'step_pre',
    'step_dist_old': 'step_pre',
    'step_dist_new': 'step_post',
    'step_sample_old': 'step_pre',
    'step_sample_new': 'step_post',
}

class DEID:
    def __init__(self, spark, ds_cfg, args=None) -> None:
        self.spark = spark
        self.ds_cfg = ds_cfg
        self.ds_cfg['storage'] = fn_util.get_path(self.ds_cfg['storage'])
        self.ds_cfg['step_org']['in']['directory'] = fn_util.get_path(self.ds_cfg['step_org']['in']['directory'])
        if 'dist_encoding' not in self.ds_cfg: self.ds_cfg['dist_encoding'] = 'euc-kr'
        self.catalog = ds_cfg['catalog']
        self.c_filename = lambda c: f'{self.catalog}.{c}.parquet'
        self._rn = self.ds_cfg['_rn'] if ('_rn' in self.ds_cfg) and (self.ds_cfg['_rn']) else '_rn'
        self._jk = self.ds_cfg['_jk'] if ('_jk' in self.ds_cfg) and (self.ds_cfg['_jk']) else '_jk'
        self._data_file_prefix = f'data.{self.catalog}'
        self._jk_file_prefix = f'jk.{self.catalog}'
        self.use_pre = self._use_or_not_step_pre()
        self.args = args

    ########################################################################
    _get_step_out_cfg = lambda self, step: {'directory': str(Path(self.ds_cfg['storage'], step))}
    _get_filename = lambda self, cfg=None: cfg['filename'] if cfg and ('filename' in cfg) else f'{self.catalog}.parquet'
    _sorted_c_idx = lambda self, cs: sorted([int(x) for x in list(cs) if str(x).isdigit()])
    _last_c_idx = lambda self, cs: max([int(x) for x in list(cs) if str(x).isdigit()])
    _csv_object = lambda self, step: {
        'directory': str(Path(self.ds_cfg['storage'], step)),
        'encoding': self.ds_cfg['dist_encoding'],
        'make_1_file': True,
    }

    ########################################################################
    def _use_or_not_step_pre(self):
        in_use = False
        if self.ds_cfg.get('step_pre'):
            in_use = self.ds_cfg['step_pre'].get('column')
            in_use = in_use or self.ds_cfg['step_pre'].get('remove_duplicated')
        if not in_use:
            logger.warn(f'no pre config ==> "pre" is "{step_ins["step_pre"]}"')
            for k, v in step_ins.items():
                if v == 'step_pre':
                    step_ins[k] = step_ins['step_pre']
        return in_use

    ########################################################################
    def step_org(self):
        step = 'step_org'
        elapsed = Elapsed(step)
        st_cfg = self.ds_cfg[step]
        in_cfg = st_cfg['in']
        # in_cfg['directory'] = str(Path(self.ds_cfg['storage'], 'org'))
        df = fn_local.load_data(self.spark, in_cfg, in_cfg['filename'])
        df = df.transform(fn.add_row_number, self._rn)

        ot_cfg = self._get_step_out_cfg(step_ins['step_pre'])
        ot_path = fn_local.save_data(self.spark, df, ot_cfg, self._get_filename())
        elapsed.print_elapsed(f'==> {ot_path}')
        return ot_path

    ########################################################################
    def load_data_step_pre_in(self):
        step = 'step_pre'
        st_cfg = self.ds_cfg[step]
        in_cfg = self._get_step_out_cfg(step_ins[step])
        df = fn_local.load_data(self.spark, in_cfg, self._get_filename())
        if st_cfg and st_cfg.get('filter_in'):
            df = fn.filtering(df, {'filter': st_cfg.get('filter_in')})
        return df

    ########################################################################
    def step_pre(self):
        step = 'step_pre'
        elapsed = Elapsed(step)

        if not self.use_pre:
            logger.warn('no pre config')
            elapsed.print_elapsed()
            return

        df = self.load_data_step_pre_in()
        st_cfg = self.ds_cfg[step]
        # in_cfg = self._get_step_out_cfg(step_ins[step])
        # df = fn_local.load_data(self.spark, in_cfg, self._get_filename())
        # if st_cfg.get('filter_in'):
        #     df = fn.filtering(df, {'filter': st_cfg.get('filter_in')})

        ot_cfg = self._get_step_out_cfg(step)
        if st_cfg.get('column'):
            self.step_deid(step=step, df=df)

            for c_base, cfg in st_cfg['column'].items():
                logger.info(f'{c_base}: {list(cfg.keys())}: {fn.c_idx(c_base, self._last_c_idx(cfg.keys()))}')
                dm = fn_local.load_data(self.spark, ot_cfg, self.c_filename(c_base))
                dm = dm.select(dm[self._rn], dm[fn.c_idx(c_base, self._last_c_idx(cfg.keys()))].alias(c_base))
                df = df.drop(c_base)
                df = df.join(dm, on=[self._rn], how='left')

        if st_cfg.get('remove_duplicated'):
            dx = df.groupBy(st_cfg['remove_duplicated']).count()
            dx = dx.withColumnRenamed('count', '__count')
            dd = dx.groupBy('__count').count()
            dd = dd.withColumn('row_count', dd['__count'] * dd['count'])
            dd = dd.select(dd['__count'].alias('중복수'), 'count', 'row_count')
            dd = dd.coalesce(1).sort('중복수')
            # dd.write.mode('overwrite').csv(str(Path(st_cfg['out']['directory'], 'duplicated_jk_count.csv')), header=True)
            dd.toPandas().to_csv(
                Path(ot_cfg['directory'], f'duplicated_jk_count.{self.catalog}.csv'), index=False)

            df = df.join(dx, on=st_cfg['remove_duplicated'], how='left')
            df = df.filter('__count = 1')
            df = df.drop('__count')

        if st_cfg.get('filter_out'):
            df = fn.filtering(df, {'filter': st_cfg.get('filter_out')})
        ot_path = fn_local.save_data(self.spark, df, ot_cfg, self._get_filename())

        elapsed.print_elapsed(f'==> {ot_path}')
        return ot_path

    ########################################################################
    def get_person_mapping_table(self, idx_cfg, df=None):
        logger.info(idx_cfg)
        map_cfg = idx_cfg['map']

        def _create_mapping_table(df):
            df = self.load_data_step_pre_in() if df is None else df

            dm = df.select(*map_cfg['key']).distinct()

            if idx_cfg['fn'] == 'person_counter':
                dm = dm.transform(fn.add_row_number, map_cfg['value'], sequential=True)
            
            if idx_cfg['fn'] == 'person_noise':
                dm = dm.withColumn(map_cfg['value'], fn.rand_range(*idx_cfg['range']))
            
            if idx_cfg['fn'] == 'person_noise_date':
                dm = dm.withColumn(map_cfg['value'], F.array(
                    fn.rand_range(*idx_cfg['range_day']) if ('range_day' in idx_cfg) and idx_cfg['range_day'] else F.lit(0),
                    fn.rand_range(*idx_cfg['range_minute']) if ('range_minute' in idx_cfg) and idx_cfg['range_minute']  else F.lit(0),
                    ))
            fn_local.save_data(self.spark, dm, map_cfg, map_cfg['filename'])

        map_file = Path(map_cfg['directory'], map_cfg['filename'])
        if not map_file.exists():
            _create_mapping_table(df)

        return fn_local.load_data(self.spark, map_cfg, map_cfg['filename'])
    
    ########################################################################
    def step_deid(self, step='step_deid', df=None):
        elapsed = Elapsed(step)

        _str_to_list = lambda x: [x] if str == type(x) else x

        st_cfg = self.ds_cfg[step]
        in_cfg = self._get_step_out_cfg(step_ins[step])
        ot_cfg = self._get_step_out_cfg(step)
        if step != 'step_pre':
            df = fn_local.load_data(self.spark, in_cfg, self._get_filename())
        # if (step == 'step_pre') and (st_cfg.get('filter_in')):
        #     df = fn.filtering(df, {'filter': st_cfg.get('filter_in')})

        for ii, (c_base, cfg) in enumerate(st_cfg['column'].items()):
            logger.info(cfg)
            elapsed_c = Elapsed(f'{step}: {ii+1}. {c_base}')
            idxs = {k:int(k) for k in cfg.keys() if k != 'cols'}
            for k_old, k_new in idxs.items():
                if (type(k_old) != type(k_new)) or (k_old != k_new):
                    cfg[k_new] = cfg.pop(k_old)

            cfg['cols'] = cfg['cols'] if cfg.get('cols') else []
            dx = df.select(self._rn, *list(set(([c_base] if c_base in df.columns else [])+cfg['cols'])))

            # logger.info(list(cfg.keys()))
            cfg['idxs'] = self._sorted_c_idx(cfg.keys())
            for idx in cfg['idxs']:
                idx_cfg = cfg[idx]
                logger.info(f'{idx:>3} {idx_cfg}')
                if idx_cfg['fn'] in ['person_counter', 'person_noise', 'person_noise_date']:
                    idx_cfg['match'] = _str_to_list(idx_cfg['match'])
                    idx_cfg['map'] = {}
                    map_cfg = idx_cfg['map']
                    map_cfg['filename'] = f'{idx_cfg["fn"]}.{".".join(idx_cfg["match"])}.parquet'
                    map_cfg['key'] = idx_cfg['match']
                    map_cfg['value'] = '_value'
                    map_cfg['directory'] = idx_cfg['directory']

                # if idx_cfg['fn'] != 'mapping':
                if idx_cfg['fn'] not in ['mapping', 'person_counter', 'person_noise', 'person_noise_date']:
                    fx = getattr(fn, cfg[idx]['fn'])
                    dx = dx.transform(fx, c_base, idx, cfg)
                else:
                    idx_cfg['match'] = _str_to_list(idx_cfg['match'])
                    map_cfg = idx_cfg['map']
                    map_cfg['key'] = _str_to_list(map_cfg['key'])
                    if len(map_cfg['key']) != len(idx_cfg['match']):
                        logger.error(f'column count mismatch: {map_cfg["key"]}, {idx_cfg["match"]}')
                        return

                    map_cfg['directory'] = fn_util.get_path(str(map_cfg['directory']))
                    if idx_cfg['fn'] != 'mapping':
                        dm = self.get_person_mapping_table(idx_cfg, df=df if step=='step_pre' else None)
                    else:
                        dm = fn_local.load_data(self.spark, map_cfg, map_cfg['filename'])
                        dm = dm.select(map_cfg['value'], *map_cfg['key'])
                    
                    # dm.show()
                    if dm == None:
                        logger.error('mapping table is not exists')
                        return
                    
                    fx = getattr(fn, idx_cfg['fn'])
                    dx = dx.transform(fx, c_base, idx, cfg, dm)
            ot_path = fn_local.save_data(self.spark, dx, ot_cfg, self.c_filename(c_base))
            elapsed_c.print_elapsed(f'{c_base} ==> {ot_path}')

        elapsed.print_elapsed()
        return

    ########################################################################
    def step_post(self, step='step_post'):
        elapsed = Elapsed(step)

        st_cfg = self.ds_cfg[step]
        in_cfg = {
            'old': self._get_step_out_cfg(step_ins[f'{step}_old']),
            'new': self._get_step_out_cfg(step_ins[f'{step}_new']),
        }
        ot_cfg = self._get_step_out_cfg(step)

        if not st_cfg.get('include'):
            st_cfg_org = self.ds_cfg['step_org']
            st_cfg_org['in']['directory'] = str(Path(self.ds_cfg['storage'], 'org'))
            df_org = fn_local.load_data(self.spark, st_cfg_org['in'], st_cfg_org['in']['filename'])
            st_cfg['include'] = df_org.columns

        if st_cfg.get('exclude'):
            # 순서 바뀌는 문제 있슴
            # st_cfg['include'] = list(set(st_cfg['include']) - set(st_cfg['exclude']))
            for x in st_cfg['exclude']:
                st_cfg['include'].remove(x)

        if self.ds_cfg.get('data_with_jk'):
            st_cfg['include'].insert(0, self._jk)

        df_new = None
        new_cols = self.ds_cfg[step_ins[f'{step}_new']]['column']
        for c in [x for x in st_cfg['include'] if x in new_cols]:
            # logger.info(f'{new_cols[c].keys()}: {self._last_c_idx(new_cols[c].keys())}')
            c_last_idx = fn.c_idx(c, self._last_c_idx(new_cols[c].keys()))
            logger.info(f'{c}: {c_last_idx}')
            df_c = fn_local.load_data(self.spark, in_cfg['new'], self.c_filename(c))
            df_c = df_c.select(df_c[self._rn], df_c[c_last_idx].alias(c))
            df_new = df_c if df_new is None else df_new.join(df_c, on=[self._rn], how='left')

        if df_new and (self._jk not in df_new.columns):
            if self._jk in st_cfg.get('include'):
                st_cfg['include'].remove(self._jk)
                logger.info(f'no {self._jk}')

        df_old = fn_local.load_data(self.spark, in_cfg['old'], self._get_filename())
        df_old = df_old.select(self._rn, *[x for x in st_cfg['include'] if (not df_new) or (x not in df_new.columns)])

        df = df_old.join(df_new, on=[self._rn], how='left') if df_new else df_old
        df = df.select(self._rn, *st_cfg['include'])

        if st_cfg.get('filter_out'):
            df = fn.filtering(df, {'filter': st_cfg.get('filter_out')})
        ot_path = fn_local.save_data(self.spark, df, ot_cfg, self._get_filename())

        elapsed.print_elapsed(f'==> {ot_path}')
        return ot_path

    ########################################################################
    def step_post_1_file(self):
        step = 'step_post_1_file'
        elapsed = Elapsed(step)

        st_cfg = self.ds_cfg['step_post']
        in_cfg = self._get_step_out_cfg('step_post')
        ot_cfg = self._get_step_out_cfg(step)
        ot_cfg['make_1_file'] = True

        df = fn_local.load_data(self.spark, in_cfg, self._get_filename())

        if self.ds_cfg.get('data_with_jk'):
            df = df.drop(self._rn)

        if self.args and self.args.output_post_data:
            data_file = self.args.output_post_data
        else:
            # data_file = f'{self._data_file_prefix}.{fn_util.get_file_postfix()}.csv'
            data_file = f'{self._data_file_prefix}.csv'
            
        ot_path = fn_local.save_data(self.spark, df, ot_cfg, data_file)
        fn_util.csv_folder_to_file(ot_path)

        elapsed.print_elapsed(f'==> {ot_path}')
        return ot_path

    ########################################################################
    def step_post_jk(self):
        step = 'step_post_jk'
        elapsed = Elapsed(step)

        st_cfg = self.ds_cfg[step_ins[step]]
        in_cfg = self._get_step_out_cfg(step_ins[step])

        df = fn_local.load_data(self.spark, in_cfg, self.c_filename(self._jk))

        c_last_idx = fn.c_idx(self._jk, self._last_c_idx(st_cfg['column'][self._jk].keys()))
        df = df.select(df[self._rn], df[c_last_idx].alias(self._jk))

        ot_cfg = self._get_step_out_cfg('step_post_1_file')
        ot_cfg['make_1_file'] = True
        if self.args and self.args.output_post_jk:
            data_file = self.args.output_post_jk
        else:
            # data_file = f'{self._jk_file_prefix}.{fn_util.get_file_postfix()}.csv'
            data_file = f'{self._jk_file_prefix}.csv'
        ot_path = fn_local.save_data(self.spark, df, ot_cfg, data_file)
        fn_util.csv_folder_to_file(ot_path)

        elapsed.print_elapsed(f'==> {ot_path}')
        return ot_path

    ########################################################################
    step_dist_org = lambda self: self.step_dist('step_dist_org')
    step_dist_pre = lambda self: self.step_dist('step_dist_pre')
    step_dist_old = lambda self: self.step_dist('step_dist_old')
    step_dist_new = lambda self: self.step_dist('step_dist_new')
    ########################################################################
    def step_dist(self, step='step_dist_org'):
        elapsed = Elapsed(step)

        st_cfg = self.ds_cfg[step]
        in_cfg = self._get_step_out_cfg(step_ins[step])
        df = fn_local.load_data(self.spark, in_cfg, self._get_filename())

        if not st_cfg.get('include'):
            st_cfg['include'] = df.columns
        if not st_cfg.get('exclude'):
            st_cfg['exclude'] = []

        for c in st_cfg['include']:
            if c not in df.columns:
                # logger.error(f'column does not exists: {c}')
                raise Exception(f'column does not exists: {c}')

        for c in list(set(st_cfg['include']) - set(st_cfg['exclude']) - set([self._rn, self._jk])):
            ot_cfg = self._csv_object(step)
            dx = df.groupBy(c).count().sort(c)
            ot_path = fn_local.save_data(self.spark, dx, ot_cfg, f'{self.catalog}.{c}.csv')
            fn_util.csv_folder_to_file(ot_path)
            logger.info(f'{step}: {c} ==> {ot_path}')

        elapsed.print_elapsed()
        return ot_path

    ########################################################################
    step_sum_org = lambda self: self.step_sum('step_sum_org')
    step_sum_pre = lambda self: self.step_sum('step_sum_pre')
    step_sum_old = lambda self: self.step_sum('step_sum_old')
    step_sum_new = lambda self: self.step_sum('step_sum_new')
    ########################################################################
    def step_sum(self, step='step_sum_org'):
        elapsed = Elapsed(step)

        if not self.ds_cfg.get(step).get('include'):
            elapsed.print_elapsed('no step_sum_old.included columns')
            return

        st_cfg = self.ds_cfg[step]
        in_cfg = self._get_step_out_cfg(step_ins[step])
        df = fn_local.load_data(self.spark, in_cfg, self._get_filename())

        if st_cfg.get('exclude'):
            st_cfg['include'] = list(set(st_cfg['include']) - set(st_cfg['exclude']))

        for c in st_cfg['include']:
            if c not in df.columns:
                raise Exception(f'column does not exists: {c}')

        # logger.info(st_cfg['include'])
        # logger.info([x for x in df.columns if x in st_cfg['include']])

        ot_cfg = self._csv_object(step)
        if self.args and self.args.output_sum:
            sum_file = self.args.output_sum
        else:
            # sum_file = f'{self.catalog}.{fn_util.get_file_postfix()}.csv'
            sum_file = f'{self.catalog}.csv'
        ot_path = Path(ot_cfg['directory'], sum_file)
        ot_path.parent.mkdir(parents=True, exist_ok=True)
        def _sum_write(v):
            with open(ot_path, 'a', encoding=ot_cfg['encoding']) as f:
                logger.info(v)
                f.write(','.join([str(x) for x in v]))
                f.write('\n')
                f.flush()

        # accuracy = self.ds_cfg['accuracy'] if 'accuracy' in self.ds_cfg else 10000
        if not st_cfg.get('accuracy'):
            st_cfg['accuracy'] = 10000
        if not st_cfg.get('opt'):
            st_cfg['opt'] = {'lower': 'right', 'upper': 'left', 'exact': 0.5}
        if 'exact' not in st_cfg['opt']:
            st_cfg['opt']['exact'] = 0.5

        ps = [0.01, 0.25, 0.5, 0.75, 0.99]
        columns = ['column','not_null','min','max']
        columns.extend([f'{int(x*100)}%' for x in ps])
        columns.extend(['low(3sigma)_left','low(3sigma)','low(3sigma)_right'])
        columns.extend(['up(3sigma)_left','up(3sigma)','up(3sigma)_right'])
        columns.extend(['low(1.5iqr)_left','low(1.5iqr)','low(1.5iqr)_right'])
        columns.extend(['up(1.5iqr)_left','up(1.5iqr)','up(1.5iqr)_right'])
        _sum_write(columns)
        # dx = None
        for c in [x for x in df.columns if x in st_cfg['include']]:
            print(f'{c}: {dict(df.dtypes)[c]}')
            # long=bigint
            if not re.search('(int|long|float|double|decimal)', dict(df.dtypes)[c], re.IGNORECASE):
                continue

            v = [c]
            v.extend([fn.lf_not_null(df, c)])
            logger.info(v)
            if v[1] == 0:
                v.extend([None] * len(columns[2:]))
            else:
                v.extend(fn.lf_min_max(df, c))
                v.extend(fn.lf_percentile(df, c, ps, st_cfg['accuracy']))
                v.extend(fn.lf_3sigma(df, c, st_cfg['opt']))
                v.extend(fn.lf_15iqr(df, c, st_cfg['opt']))
            _sum_write(v)
            # df_tmp = fn_local.create_dataframe(self.spark, data=[v], schema=','.join([f'`{x}` string' for x in columns]))
            # dx = dx.union(df_tmp) if dx else df_tmp

        # if not dx:
        #     logger.warn('no valid included columns')
        #     ot_path = None
        # else:
        #     ot_cfg = self._csv_object(step)
        #     # ot_path = fn_local.save_data(self.spark, dx, ot_cfg, f'{self.catalog}.{fn_util.get_file_postfix()}.csv')
        #     # fn_util.csv_folder_to_file(ot_path)
        #     ot_path = Path(ot_cfg['directory'], f'{self.catalog}.{fn_util.get_file_postfix()}.csv')
        #     dx.toPandas().to_csv(ot_path, index=False, float_format='%.4f', encoding=ot_cfg['encoding'])
            # logger.info(f'{step} ==> {ot_path}')

        elapsed.print_elapsed(f'==> {ot_path}')
        return ot_path

    ########################################################################
    def step_sample(self):
        step='step_sample'
        elapsed = Elapsed(step)

        st_cfg = self.ds_cfg[step]
        in_cfg = {
            'old': self._get_step_out_cfg(step_ins[f'{step}_old']),
            'new': self._get_step_out_cfg(step_ins[f'{step}_new']),
        }

        #############################
        df_old = fn_local.load_data(self.spark, in_cfg['old'], self._get_filename())
        if st_cfg.get('default_filter'):
            for x in st_cfg.get('default_filter'):
                df_old = df_old.filter(x)
        dx_old = None
        for x in st_cfg.get('filters', [{'count': 5}]):
            if x.get('filter'):
                dx = df_old.filter(x['filter']).limit(x['count'])
            else:
                dx = df_old.limit(x['count'])
            dx_old = dx_old.union(dx) if dx_old else dx
        dx_old = dx_old.coalesce(1).sort(self._rn)
        pd_old = dx_old.toPandas().transpose()
        pd_old.reset_index(inplace=True)
        pd_old.columns=['name']+[f'_c{x}' for x in range(pd_old.shape[1]-1)]
        # print(pd_old)

        ot_cfg = self._csv_object(step)
        logger.info(ot_cfg)

        #############################
        dx_new = None
        df_new = fn_local.load_data(self.spark, in_cfg['new'], self._get_filename())
        if not df_new:
            dx = pd_old
        else:
            dx_new = dx_old.select(self._rn).join(df_new, on=[self._rn], how='left')
            dx_new = dx_new.drop(self._jk)
            dx_new = dx_new.coalesce(1).sort(self._rn)
            pd_new = dx_new.toPandas().transpose()
            pd_new.reset_index(inplace=True)
            pd_new.columns=['name']+[f'_c{x}' for x in range(pd_new.shape[1]-1)]
            # print(pd_new)
            dx = pd_old.join(pd_new.set_index('name'), on='name', how='outer', lsuffix='_전', rsuffix='_후')

        dx = dx.sort_index()
        # print(dx)
        if self.args and self.args.output_sample:
            sample_file = self.args.output_sample
        else:
            # sample_file = f'{self.catalog}.{fn_util.get_file_postfix()}.csv'
            sample_file = f'{self.catalog}.csv'
        ot_path = Path(ot_cfg['directory'], sample_file)
        ot_path.parent.mkdir(parents=True, exist_ok=True)
        dx.to_csv(ot_path, header=True, index=False, encoding=ot_cfg['encoding'])

        elapsed.print_elapsed(f'==> {ot_path}')
        return ot_path

    ########################################################################
    def step_count(self):
        step = 'step_count'
        elapsed = Elapsed(step)
        jsonData = {}

        def _jsonAppend(jsonData, path, file_size, column_count, row_count):
            jsonData[path.parent.name] = {}
            jsonData[path.parent.name]['filename'] = path.name
            jsonData[path.parent.name]['filesize'] = file_size
            jsonData[path.parent.name]['column_count'] = column_count
            jsonData[path.parent.name]['row_count'] = row_count

        def _write(f, path, file_size, column_count, row_count):
            logger.info(path)
            logger.info(f'file_size: {file_size:,} bytes')
            logger.info(f'column count: {column_count:,}')
            logger.info(f'row count: {row_count:,}')

            f.write(f'\n\nfile: {path.parent.name}/{path.name}')
            f.write(f'\n\tfile size   : {file_size:,} bytes')
            f.write(f'\n\tcolumn count: {column_count:,}')
            f.write(f'\n\trow count   : {row_count:,}')
            f.flush()
            
            if self.args and self.args.output_count_json:
                _jsonAppend(jsonData, path, file_size, column_count, row_count)
            
        ot_cfg = self._csv_object(step)
        if self.args and self.args.output_count:
            count_file = self.args.output_count
        else:
            # count_file = f'count.{self.catalog}.{fn_util.get_file_postfix()}.txt'
            count_file = f'count.{self.catalog}.txt'
        ot_path = Path(ot_cfg['directory'], count_file)
        ot_path.parent.mkdir(parents=True, exist_ok=True)
        with open(ot_path, 'w+', encoding=ot_cfg['encoding']) as f:
            # csv는 org
            in_cfg = self.ds_cfg['step_org']['in']
            in_file = Path(in_cfg['directory'], in_cfg['filename'])
            # file_size = fn_util.get_filesize(in_file)
            file_size = fn_util.get_filesize(in_file.parent, in_file.name)

            # step_org에서 필터링을 하므로 필터링 시간을 줄이기 위해서
            # column/row count는 org_rn
            in_cfg = self._get_step_out_cfg('org_rn')
            df = fn_local.load_data(self.spark, in_cfg, self._get_filename())
            _write(f, in_file, file_size, len(df.columns)-1, df.count())

            if self.ds_cfg.get('step_post'):
                in_cfg = self._get_step_out_cfg('step_post_1_file')
                in_cfg['multiLine'] = True
                if self.args and self.args.output_post_data:
                    data_file = self.args.output_post_data
                else:
                    data_file = f'{self._data_file_prefix}.csv'
                path = Path(max(Path(in_cfg['directory']).glob(data_file)))
                df = fn_local.load_data(self.spark, in_cfg, path.name)
                _write(f, path, fn_util.get_filesize(path), len(df.columns), df.count())

        if jsonData:
            if self.args and self.args.output_count:
                json_file = f'{self.args.output_count}.json'
            else:
                # json_file = f'count.{self.catalog}.{fn_util.get_file_postfix()}.txt.json'
                json_file = f'count.{self.catalog}.txt.json'
            with open(Path(ot_cfg['directory'], json_file), 'w+', encoding=ot_cfg['encoding']) as f:
                json.dump(jsonData, f)
        elapsed.print_elapsed(f'==> {ot_path}')
        return ot_path
