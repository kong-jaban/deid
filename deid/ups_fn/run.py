import os
import sys
from time import sleep
if __name__ == '__main__':
    path=os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
else:
    path=os.path.dirname(os.getcwd())
sys.path.append(path)
# print(path)

from ups_fn.spark import Spark
from ups_fn.step import DEID
from ups_tool.elapsed import Elapsed
from ups_tool import util as fn_util

import yaml
from pathlib import Path
from datetime import datetime
import argparse

###########################################
# 가명처리
#   input
#   output

###########################################
def main(args):
    print('\n'.join([f'{k}: {v}' for k, v in vars(args).items()]))

    ymls = []
    if 'add_configs' in args and args.add_configs:
        ymls = [fn_util.get_path(x) for x in args.add_configs]
    ymls.append(args.catalog)
    ds_cfg = load_config(ymls)

    logger = fn_util.get_logger(str(ds_cfg['catalog']))
    elapsed = Elapsed(ds_cfg['catalog'])

    spark = Spark()

    jobs = args.jobs.split(',')
    if ('post_jk' in jobs) and ds_cfg.get('data_with_jk'):
        logger.warning(f'post_jk: 데이터전문기관 결합용에서 불필요')
        jobs.remove('post_jk')

    deid = DEID(spark, ds_cfg, args)
    for job in [x.strip() for x in jobs if x and x.strip()]:
        ot_path = getattr(deid, f'step_{job}')()

    elapsed.print_elapsed()
    return

###########################################
def load_config(ymls=[]):
    def _join(loader, node):
        seq = loader.construct_sequence(node)
        return ''.join([str(x) for x in seq])

    yml_str = ''
    for x in ymls:
        with open(x, 'r', encoding='utf8') as f:
            yml_str += f.read() + '\n'

    yaml.add_constructor('!join', _join)
    return yaml.load(yml_str, Loader=yaml.FullLoader)

###########################################
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', required=True, action=fn_util.PathAction, help='테이블 설정 yml 경로')
    parser.add_argument('--jobs', required=True, help='작업 목록. 데이터 yml에 정의한 범위 내에서 지정 (ex)org,pre,deid,post')
    parser.add_argument('--add_configs', required=False, action='append', help='테이블 yml 경로')
    parser.add_argument('--output_sum', default=None, help=f'저장할 sum csv 파일명')
    # parser.add_argument('--output_info_xlsx', default='', help=f'저장할 info xlsx 파일명')
    parser.add_argument('--output_sample', default='', help=f'저장할 sample 파일명')
    parser.add_argument('--output_count', default='', help=f'저장할 count 파일명')
    parser.add_argument('--output_count_json', default='N', help='json 형식 count 파일 생성 여부')
    parser.add_argument('--output_post_data', default='', help=f'저장할 data 파일명')
    parser.add_argument('--output_post_jk', default='', help=f'저장할 jk 파일명')
    args = parser.parse_args()
    main(args)
