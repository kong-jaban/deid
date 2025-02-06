#!/usr/bin/env python

import os
import argparse
import build_module as bm

# 버전 궁합 & root/user 구분을 위해 이미지 레이어
# base01 -> 02 -> 03 -> 04 -> deid
# 
# docker save -o ups_deid:231204.01.tar ups_deid:231204.01
# docker load -i ups_deid:231204.01.targ
# docker run -it --rm -p 4040:4040 -p 18080:8080 -v c:/data:/home/ups/data:rw ups_deid:231204.01 $SPARK_HOME/sbin/start-history-server.sh
# http://localhost:18080

vs = {
    # 'linux' : 'registry.access.redhat.com/ubi9/ubi:9.3-1476',
    'linux' : 'registry.access.redhat.com/ubi9/ubi-minimal:9.3-1475',
    'python': '3.11',
    'java'  : '17',
    'hadoop': '3.3.6',
    'spark' : '3.5.0',
    # 'pip'   : '231217',
    'ups'   : '231217',
    'ups.wheel': '0.1',
    'base01': '240104.01',
    'base02': '240104.01',
    'base03': '240104.01',
    'base04': '240104.01',
    'deid'  : '240213.01',
}

get_tags = lambda x: f'ups_{x}:{vs[x]}'

dockers = {
    'linux': {
        'tag': vs['linux'],
    },
    'base01': {
        'tag': get_tags('base01'),
        'from': 'linux',
        'build-arg': [
            f'PYTHON_VERSION={vs["python"]}',
        ],
        'ex': [
            f'{get_tags("base01")} cat /etc/os-release',
            f'{get_tags("base01")} locale',
            f'{get_tags("base01")} python --version',
            f'{get_tags("base01")} python -m pip --version',
        ],
    },
    'base02': {
        'tag': get_tags('base02'),
        'from': 'base01',
        'build-arg': [
            f'JAVA_VERSION={vs["java"]}',
        ],
        'ex': [f'{get_tags("base02")} java -version'],
    },
    'base03': {
        'tag': get_tags('base03'),
        'from': 'base02',
        'build-arg': [
            f'SPARK_VERSION={vs["spark"]}',
            f'HADOOP_VERSION={vs["hadoop"]}',
            f'HADOOP_MAJOR_VERSION={vs["hadoop"][0]}',
        ],
        'ex': [f'-v c:\\data:/home/ups/data:rw {get_tags("base03")} pyspark --version'],
    },
    'base04': {
        'tag': get_tags('base04'),
        'from': 'base03',
        'build-arg': [
            'UID=$(id -u)',
            'GID=$(id -g)',
            f'PYTHON_VERSION={vs["python"]}',
            f'SPARK_VERSION={vs["spark"]}',
            f'HADOOP_VERSION={vs["hadoop"]}',
        ],
        'ex': [
            f'-v c:\\data:/home/ups/data:rw {get_tags("base04")} pyspark --version',
            f'{get_tags("base04")} whoami',
        ],
    },
    'deid': {
        'tag': get_tags('deid'),
        'from': 'base04',
        'build-arg': [
            f'PYTHON_VERSION={vs["python"]}',
            f'SPARK_VERSION={vs["spark"]}',
            'MODULE_NAME=ups',
        ],
        'ex': [
            f'{get_tags("deid")} python --version',
            f'{get_tags("deid")} python -m pip --version',
            f'{get_tags("deid")} java -version',
            f'-p 4040:4040 -v c:\\data:/home/ups/data:rw {get_tags("deid")} pyspark --version',
        ],
    },
}

###########################################
def build(args):
    name = args.what
    if name == 'deid':
        modname = 'ups'
        parser = argparse.ArgumentParser()
        parser.add_argument('--name')
        parser.add_argument('--version')
        bm.build_wheel(parser.parse_args([f'--name={modname}', f'--version={vs["{}.wheel".format(modname)]}']))
    v = dockers[name]
    cmd = ['docker build']
    cmd.append(f'-f dockerfile.{name}')
    cmd.append(f'--build-arg FROM_TAG={dockers[v["from"]]["tag"]}')
    for barg in v['build-arg']:
        cmd.append(f'--build-arg {barg}')
    cmd.append(f'--tag {dockers[name]["tag"]}')
    cmd.append('.')
    print(' '.join(cmd))
    os.system(' '.join(cmd))
    if args.save_image and (name == 'deid'):
        # save image file
        oname = f'{dockers[name]["tag"].replace(":", "_")}.tar'
        os.system(f'docker save -o ~/data/{oname} {dockers[name]["tag"]}')
        print(f'saved at ~/data/{oname}')
        print(f'docker load -i {oname}')

    print('\n')
    for x in v['ex']:
        print(f'docker run -it --rm {x}')

###########################################
if __name__ == "__main__":
    whats = '/'.join([x for x in dockers.keys() if x != 'linux'])
    parser = argparse.ArgumentParser()
    parser.add_argument('--what', default='deid', help=f'빌드할 이미지. {whats} (기본값: %(default)s)')
    parser.add_argument('--save_image', default=False, action=argparse.BooleanOptionalAction,
                        help='이미지 내보내기 여부 (--what=ups 에서만 유효) (기본값: %(default)s)')
    args = parser.parse_args()
    build(args)