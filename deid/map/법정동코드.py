from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent.parent) if __name__ == '__main__' else str(Path().parent))

import pandas as pd
from tqdm import tqdm

###########################################
def _sido(df):
    def _apply(row):
        return row[0].split(' ')[0]

    tqdm.pandas()
    df['sido'] = df.loc[:, ['법정동명']].progress_apply(_apply, axis=1)
    return df

###########################################
def _sigungu(df):
    def _apply(row):
        s = row[0].split(' ')
        if len(s) == 1 or s[0].startswith('세종'):
            return None
        if (len(s) == 4) and (not s[3].endswith('리')):
            return f'{s[1]} {s[2]}'
        return s[1]

    tqdm.pandas()
    df['sigungu'] = df.loc[:, ['법정동명']].progress_apply(_apply, axis=1)
    return df

###########################################
def _dong(df):
    def _apply(row):
        s = row[0].split(' ')
        if s[0].startswith('세종'):
            return s[1] if len(s) > 1 else None
        if (len(s) == 4) and (not s[3].endswith('리')):
            return s[3]
        return s[2] if len(s) > 2 else None

    tqdm.pandas()
    df['dong'] = df.loc[:, ['법정동명']].progress_apply(_apply, axis=1)
    return df

###########################################
def _sido_short(df):
    sd1 = ['세종', '서울', '부산', '인천', '광주', '대전', '대구', '울산', '강원', '경기', '제주',]
    sd2 = {'경상남':'경남', '경상북':'경북', '전라남':'전남', '전라북':'전북', '충청남':'충남', '충청북':'충북',}

    def _apply(row):
        if row[0] is None: return None

        for sd in sd1:
            if row[0].startswith(sd): return sd
        for k, v in sd2.items():
            if row[0].startswith(k): return v

    tqdm.pandas()
    df['sido_s'] = df.loc[:, ['sido']].progress_apply(_apply, axis=1)
    return df

###########################################
def _sigungu_short(df):
    def _apply(row):
        return row[0][:-1] if row[0] else None

    tqdm.pandas()
    df['sigungu_s'] = df.loc[:, ['sigungu']].progress_apply(_apply, axis=1)
    return df

###########################################
def main():
    df = pd.read_csv(
        '법정동코드 전체자료.txt',
        header = 0,
        sep = '\t',
        dtype = object,
        encoding = 'cp949',
    )

    df = _sido(df)
    df = _sigungu(df)
    df = _dong(df)
    df = _sido_short(df)
    df = _sigungu_short(df)

    df.to_csv(
        '법정동코드.csv',
        index = False,
        header = True,
        encoding = 'utf8',
    )

###########################################
if __name__ == "__main__":
    main()
