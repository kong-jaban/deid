#!/usr/bin/env python

import os
from pathlib import Path
import shutil
import argparse

NAME = 'ups'
VERSION = '0.1'

###########################################
def build_wheel(args):
    print('build_wheel:', args)
    source = Path('source')
    target = Path('target')
    pkgs = ['ups_fn', 'ups_tool']
    for x in [source, target]:
        if x.exists():
            shutil.rmtree(x)
    source.mkdir()
    for pkg in pkgs:
        shutil.copytree(f'../{pkg}', f'./{source}/{pkg}')
        #
        for x in source.glob('**/__pycache__'):
            shutil.rmtree(x)
        # 빈 디렉토리 삭제
        # list로 만들어서 삭제해야 FileNotFoundError 안 뜸
        for x in list(source.glob('**')):
            if len(list(x.glob('*'))) == 0:
                shutil.rmtree(x)
        #
        for x in Path(source, pkg).glob('**'):
            with open(Path(x, '__init__.py'), 'w') as f:
                f.write('')
                
    # 아래 설치해야 오류 안 남
    #   sudo apt install python3.11-venv
    #   python -m pip install --upgrade build
    make_pyproject()
    make_setup_cfg(args)
    os.system('python -m build .')

    os.system(f'python -m pip uninstall {NAME} -y')
    os.system(f'python -m pip install --no-index --find-links={os.path.dirname(__file__)}/dist {NAME}')

# os.path.dirname(__file__)
###########################################
def make_setup_cfg(args):
    with open('setup.cfg', mode='w') as f:
        f.write('[metadata]\n')
        f.write(f'name = {args.name}\n')
        # f.write('# version = attr: ups_fn.version\n')
        f.write(f'version = {args.version}\n')
        f.write('author = timecandy\n')
        f.write('author_email = timecandy@upsdata.info\n')
        f.write('description = upsdata data core\n')
        f.write('\n')
        f.write('[options]\n')
        f.write('zip_safe = False\n')
        f.write('packages = find:\n')
        f.write('\n')
        f.write('[options.packages.find]\n')
        f.write('where=source\n')

###########################################
def make_pyproject():
    with open('pyproject.toml', mode='w') as f:
        f.write('[build-system]\n')
        f.write('requires = ["setuptools", "wheel"]\n')
        f.write('build-backend = "setuptools.build_meta"\n')

###########################################
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', default=NAME, help='module name (기본값: %(default)s)')
    parser.add_argument('--version', default=VERSION, help='module version (기본값: %(default)s)')
    args = parser.parse_args()
    build_wheel(args)
