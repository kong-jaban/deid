from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent.parent) if __name__ == '__main__' else str(Path().parent))

import pandas as pd
from tqdm import tqdm
from konlpy.tag import Okt

###########################################
def _code_5(df):
    def _apply(row):
        return f'{row[0]:0<5}'

    tqdm.pandas()
    df['code'] = df.loc[:, ['코드']].progress_apply(_apply, axis=1)
    return df

###########################################
def _morph(df):
    def _apply(row):
        okt = Okt()
        return '#'.join(okt.morphs(row[0]))

    tqdm.pandas()
    df['morph_ko'] = df.loc[:, ['국문 항목명']].progress_apply(_apply, axis=1)
    return df

###########################################
def main():
    df = pd.read_csv(
        '제7차 한국표준직업분류.csv',
        header = 0,
        sep = ',',
        dtype = object,
        encoding = 'utf8',
    )

    df = _code_5(df)
    df = _morph(df)

    cols = {
        'code': 'code',
        '국문 항목명': 'name_ko',
        '영문 항목명': 'name_en',
        'morph_ko': 'morph_ko',
    }
    df = df.loc[:, list(cols.keys())].rename(columns=cols, errors='raise')

    df.to_csv(
        '한국표준직업분류.csv',
        index = False,
        header = True,
        encoding = 'utf8',
    )

###########################################
if __name__ == "__main__":
    main()
