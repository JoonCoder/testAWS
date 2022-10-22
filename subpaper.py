import asyncio
import websockets
import json
import pyupbit as ub
from pandas import DataFrame
import math

KRWLst = []
codeLst = []
dataLst = []
cnt = 0
sCnt = 0
cCnt = 0
colLst = ['a', 'b', 'c']
account = 1000000
minTrade = 5000

# 메인함수 분리
def dataPlace(np_rank, rank_df, account_df, df):
    global sCnt
    for i in range(np_rank.shape[0]):
        name = np_rank[i]
        pos_rank = KRWLst.index(name)
        rank_df.at[pos_rank, 'name'] = name

        if type(rank_df.at[pos_rank, colLst[0]]) != int:
            rank_df.at[pos_rank, colLst[0]] = int(i)

        else:
            if type(rank_df.at[pos_rank, colLst[1]]) != int:
                rank_df.at[pos_rank, colLst[1]] = rank_df.at[pos_rank, colLst[0]]
                rank_df.at[pos_rank, colLst[0]] = int(i)

            elif type(rank_df.at[pos_rank, colLst[1]]) == int:
                rank_df.at[pos_rank, colLst[2]] = rank_df.at[pos_rank, colLst[1]]
                rank_df.at[pos_rank, colLst[1]] = rank_df.at[pos_rank, colLst[0]]
                rank_df.at[pos_rank, colLst[0]] = int(i)

                if account_df.at[1, 'name'] == 'NaN':
                    if rank_df.at[pos_rank, colLst[0]] - rank_df.at[pos_rank, colLst[2]] < -1 \
                            and rank_df.at[pos_rank, colLst[0]] < 3:
                        buy_trader(account_df, name, df)  # 매수
                        print("매수시점 : ", rank_df.at[pos_rank, colLst[0]], "/", rank_df.at[pos_rank, colLst[1]], "/", rank_df.at[pos_rank, colLst[2]])
                    else:
                        sCnt = sCnt + 1
                        if sCnt == 1:
                            print("탐색중")
                        elif sCnt == 2000:
                            print("탐색중.")
                        elif sCnt == 10000:
                            print("탐색중..")
                        elif sCnt == 20000:
                            print("탐색중...")
                            sCnt = 0
                else:
                    if 0.99 > account_df.at[1, 'current_price'] / account_df.at[1, 'buyPrice'] < 1.01:
                        sell_trader(account_df, df)
                    else:
                        control_account(account_df, df)

# 빈 데이터프레임 작성1(코인데이터 수집)
def newDF():
    raw_data = {'name': ['KRW-BTC'],
                'price': 'NaN',
                'flow': [0],
                }
    df = DataFrame(raw_data)
    return df

# 빈 데이터프레임 작성2(등락률 랭크)
def rankDF():
    rank_row = {'name': ['KRW-BTC'],
                colLst[0]: 'NaN',
                colLst[1]: 'NaN',
                colLst[2]: 'NaN',
                }
    rank_df = DataFrame(rank_row)
    return rank_df

# 빈 데이터프레임 작성3(거래 시뮬레이션)
def accountDF():
    global account
    account_row = {'name': ['KRW'],
                'buyPrice': '0',
                'Asset_size': account,
                'current_price': '0',
                }
    account_df = DataFrame(account_row)
    account_df.at[1, 'name'] = 'NaN'
    account_df.at[2, 'name'] = '평가손익'
    return account_df

# 매수 시뮬레이터
def buy_trader(account_df, name, df):
    global account
    closeRow = df.loc[df['name'] == name]
    closePrice = closeRow['price']
    df_valueLv01 = closePrice.to_numpy()
    NumChange = float(df_valueLv01)
    account_df.at[1, 'name'] = name
    account_df.at[1, 'buyPrice'] = NumChange
    account_df.at[1, 'current_price'] = NumChange
    calNum01 = round(account_df.at[0, 'Asset_size'] / account_df.at[1, 'buyPrice'], 3)
    print("calNum01 : ", calNum01)
    if calNum01 > 0:
        calNum = math.trunc(calNum01)
    else:
        calNum = round(calNum01, 3)
    account_df.at[1, 'Asset_size'] = calNum
    account_df.at[0, 'Asset_size'] = account_df.at[0, 'Asset_size'] - (NumChange * calNum)
    totalA = account_df.at[0, 'Asset_size'] + (account_df.at[1, 'current_price'] * account_df.at[1, 'Asset_size'])
    account_df.at[2, 'Asset_size'] = totalA
    account_df.at[2, 'buyPrice'] = account
    account_df.at[2, 'current_price'] = round((totalA / account - 1) * 100, 2)
    print("매수 시퀀스 진입!!")
    print(account_df)

# 보유종목 가격 추적
def control_account(account_df, df):
    global cCnt
    name = account_df.at[1, 'name']
    closeRow = df.loc[df['name'] == name]
    closePrice = closeRow['price']
    df_valueLv01 = closePrice.to_numpy()
    NumChange = float(df_valueLv01)
    account_df.at[1, 'current_price'] = NumChange
    totalA = account_df.at[0, 'Asset_size'] + (account_df.at[1, 'current_price'] * account_df.at[1, 'Asset_size'])
    account_df.at[2, 'Asset_size'] = totalA
    account_df.at[2, 'buyPrice'] = account
    account_df.at[2, 'current_price'] = round((totalA / 1000000 - 1) * 100, 2)
    cCnt = cCnt + 1
    if cCnt == 1000:
        print('보유 종목 관리중...!!')
        print(account_df)
        print("=-=-=-=-=-=-=-=-=-구분선=-=-=-=-=-=-=-=-=-=-=-=")
    elif cCnt == 10000:
        print("매수가 : ", account_df.at[1, 'buyPrice'], "현재가 : ", account_df.at[1, 'current_price'], "수익률 : ", round(account_df.at[1, 'current_price'] / account_df.at[1, 'buyPrice']), 2)
        print("현재 계좌상황 : ", account_df.at[2, 'Asset_size'], " / ", account_df.at[2, 'current_price'])
    elif cCnt == 100000:
        print("매수가 : ", account_df.at[1, 'buyPrice'], "현재가 : ", account_df.at[1, 'current_price'], "수익률 : ", round(account_df.at[1, 'current_price'] / account_df.at[1, 'buyPrice']), 2)
        print("현재 계좌상황 : ", account_df.at[2, 'Asset_size'], " / ", account_df.at[2, 'current_price'])
    elif cCnt >= 200000:
        print("=-=-=-=-=-===time over===-=-=-=-=-=-=")
        sell_trader(account_df, df)
    elif cCnt == 0:
        print('보유 종목 관리중...!!')
        print("현재 계좌상황 : ", account_df.at[2, 'Asset_size'], " / ", account_df.at[2, 'current_price'])

# 매도 시뮬레이터
def sell_trader(account_df, df):
    global account
    name = account_df.at[1, 'name']
    closeRow = df.loc[df['name'] == name]
    closePrice = closeRow['price']
    df_valueLv01 = closePrice.to_numpy()
    NumChange = float(df_valueLv01)
    Stock = account_df.at[1, 'Asset_size']
    pay = NumChange * Stock * 0.0025
    account_df.at[0, 'Asset_size'] = NumChange * Stock + account_df.at[0, 'Asset_size'] - pay
    account_df.at[1, 'Asset_size'] = 0
    account_df.at[1, 'buyPrice'] = 0
    account_df.at[1, 'current_price'] = 0
    account_df.at[1, 'name'] = 'NaN'
    totalA = account_df.at[0, 'Asset_size'] + (account_df.at[1, 'current_price'] * account_df.at[1, 'Asset_size'])
    account_df.at[2, 'Asset_size'] = totalA
    account = totalA
    account_df.at[2, 'current_price'] = round((account / 1000000 - 1) * 100, 2)
    print("매도 시퀀스 진입...!!")
    print("현재 계좌상황 : ", account_df.at[2, 'Asset_size'], " / ", account_df.at[2, 'current_price'])

# 데이터 정형화
def dataSet(data):
    name = data.get('cd')
    price = round(data.get('tp'), 2)
    flow = round(data.get('scr') * 100, 2)
    dataset = (name, price, float(flow))
    return dataset

# 데이터의 데이터프레임화
def dataFrame(df, rank_df, dataset, account_df):
    global cnt
    global posCnt
    global colLst
    position = KRWLst.index(dataset[0])
    df.loc[position] = dataset
    new_df = df.sort_values('flow', ascending=False)
    np_df = new_df.to_numpy()
    np_rank = np_df[:, 0]
    if round(((np_rank.shape[0]) / (len(KRWLst)) * 100)) >= 50:
        cnt = 0
        dataPlace(np_rank, rank_df, account_df, df)
    else:
        cnt = cnt + 1
        if cnt == 30:
            print("now loading...", round(((np_rank.shape[0]) / (len(KRWLst)) * 100)), "%")
            cnt = 0
# 거래가능 종목 추출
def KRW():
    tickers = ub.get_tickers()
    for i in tickers:
        if "KRW-" in i:
            KRWLst.append(i)
        else:
            pass
    for j in KRWLst:
        codeLst.append({'ticket': "test"},)
        codeLst.append({'type': 'ticker', 'codes': [j], 'isOnlyRealtime': True})
        codeLst.append({'format': 'SIMPLE'},)

# 웹소켓 연결을 통한 데이터 송신
async def upbit_ws_client(df, rank_df, account_df):
    uri = 'wss://api.upbit.com/websocket/v1'
    async with websockets.connect(uri,  ping_interval=60) as websocket:
        subscribe_fmt = codeLst
        subscribe_data = json.dumps(subscribe_fmt)
        await websocket.send(subscribe_data)

        while True:
            data = await websocket.recv()
            data = json.loads(data)
            dataset = dataSet(data)
            dataFrame(df, rank_df, dataset, account_df)

# 구동부
async def main():
    df = newDF()
    rank_df = rankDF()
    account_df = accountDF()
    KRW()
    await upbit_ws_client(df, rank_df, account_df)

# 시작 트리거
asyncio.run(main())