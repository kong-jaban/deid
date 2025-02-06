import os
import sys
import yaml
# python3 -m pip install pycryptodomex
from Cryptodome.Hash import SHA256
from pathlib import Path
import logging
from datetime import datetime
import shutil
import re
from configparser import ConfigParser
from itertools import chain
import argparse
from typing import List

###########################################
get_fn_name = lambda depth=0: sys._getframe(depth+1).f_code.co_name
get_py_name = lambda depth=0: sys._getframe(depth+1).f_code.co_filename
get_file_postfix = lambda : datetime.now().strftime('%Y%m%d%H%M%S')
get_path = lambda x: Path(x.replace('~', str(Path.home())))
to_printable = lambda x: x.replace('\t', '\\t').replace('\r', '\\r').replace('\n', '\\n') if type(x)==str else x

###########################################
def get_spark_env(item: str, default_value: str | int) -> str | int:
    parser = ConfigParser()
    with open(Path(os.getenv('SPARK_HOME'), 'conf', 'spark-env.sh')) as f:
        conf = chain(('[top]',), f)
        parser.read_file(conf)
    return parser['top'].get(item, default_value)

###########################################
def get_logger(logName: str=None, logLevel: int=logging.INFO) -> logging.Logger:
    # print(get_py_name(1))
    streamHandler = logging.StreamHandler()
    fileHandler = logging.FileHandler(
        Path(get_path(get_spark_env('SPARK_LOG_DIR', '~/data/logs')), f'{get_file_postfix()}.log'))

    logging.basicConfig(
        level=logLevel,
        datefmt='%Y-%m-%d %H:%M:%S',
        # format='%(asctime)s:%(name)s:%(levelname)s:%(message)s',
        format='%(asctime)s:%(levelname)s:%(module)s:%(funcName)s:%(lineno)d:%(message)s',
        handlers=[streamHandler, fileHandler],
    )
    logger = logging.getLogger(get_py_name(1) if logName is None else logName)
    return logger

###########################################
def load_config(ymls:List[str]) -> any:
    def _join(loader, node):
        seq = loader.construct_sequence(node)
        return ''.join([str(x) for x in seq])

    yml_str = ''
    for x in ymls:
        with open(f'{x}.yml') as f:
            yml_str += f.read() + '\n'

    yaml.add_constructor('!join', _join)
    return yaml.load(yml_str, Loader=yaml.FullLoader)

###########################################
def sha256(msg: str, salt: str=None, salt_first: bool=True) -> str:
    if msg is None: return None

    h = SHA256.new()
    if (salt is not None) and (salt_first):
        h.update(salt.encode('utf-8'))
    h.update(msg.encode('utf-8'))
    if (salt is not None) and (not salt_first):
        h.update(salt.encode('utf-8'))
    return h.digest()

###########################################
def get_filesize(path: Path | str, pattern: str='*') -> int:
    f = Path(path)
    if f.is_file():
        return f.stat().st_size
    if f.is_dir():
        total_size = 0
        for f_part in f.glob(pattern):
            total_size += f_part.stat().st_size
        return total_size
    return -1

###########################################
def csv_folder_to_file(path: Path | str) -> None:
    path = Path(path)
    fs = list(path.glob('*.csv'))
    if len(fs) == 0:
        raise Exception('No csv files')
    if len(fs) > 1:
        raise Exception('Multiple csv files')

    _tmp = Path(path.parent, f'{path.name}.xxx')
    shutil.move(fs[0], _tmp)
    shutil.rmtree(path)
    shutil.move(_tmp, path)

###########################################
# for command line action
class PathAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, str(get_path(values)))
