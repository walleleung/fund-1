#!/usr/bin/env python
#coding=utf-8
import os,sys,httplib,json,time,datetime,sqlite3
from optparse import OptionParser
import multiprocessing
from multiprocessing import Pool, Lock

class bcolors:
    RED = '\033[0;37;41m'
    GREEN = '\033[0;37;42m'
    YELLOW = '\033[0;37;43m'
    BLUE = '\033[0;37;44m'
    PURPLE = '\033[0;37;45m'
    CYAN = '\033[0;37;46m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def unix_timestamp(timestr, format):
    return datetime.datetime.strptime(timestr, format).strftime("%s")

def formatYmd(unixtime):
    return datetime.date.fromtimestamp(float(unixtime)).strftime('%Y-%m-%d')

def formatYmdHm(unixtime):
    return datetime.datetime.fromtimestamp(float(unixtime)).strftime('%Y-%m-%d %H:%M')

def formatPercent(f):
    return str(round(f, 2)) + '%'

def readConf():
    file_name = 'list'
    fundcode_list = []
    file = open(file_name)
    tmplist = file.readlines()
    for tmpline in tmplist:
        fundcode_list.append(tmpline.strip())
    return fundcode_list

def getConn():
    # 连接数据库
    conn = sqlite3.connect('db')
    conn.row_factory = sqlite3.Row
    return conn

def createTable():
    conn = getConn()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE if not exists stocks
                   (code char(6) PRIMARY KEY     NOT NULL,
                   name           TEXT    NOT NULL,
                   hold            INT     NOT NULL
                   );''')
    cursor.execute('''CREATE TABLE if not exists stock_daily
                   (
                   code char(6) not null,
                   date int not null,
                   dwjz real not null,
                   ljjz real not null,
                   rzzl real not null,
                   unique (code, date)
                   );''')
    cursor.execute('''CREATE TABLE if not exists stock_valuation
                   (
                   code char(6) not null,
                   date int not null,
                   dwjz real not null,
                   rzzl real not null,
                   unique (code, date)
                   );''')

def getDailyDataFromDB(fund_code):
    conn = getConn()
    cursor = conn.cursor()
    cursor.execute('select * from stock_daily where code=? order by date', (fund_code,))
    return cursor.fetchall()

def getDailyData(fund_code):
    curtime = str(time.time()) + '666';
    httpClient = None
    try:
        httpClient = httplib.HTTPConnection('caifu.baidu.com', 80, timeout=10)
        httpClient.request('GET', '/trade/fund/jzlist?fund_code=' + fund_code + '&cur_page=1&page_size=1000&_=' + curtime)

        response = httpClient.getresponse()
        jijin_json = json.loads(response.read())
        data = jijin_json['data']['list']
        ret = []
        for d in data:
            d_date = unix_timestamp(d['date'], '%Y-%m-%d')
            row = {
                'code' : fund_code,
                'date' : d_date,
                'dwjz' : d['dwjz'],
                'ljjz' : d['ljjz'],
                'rzzl' : d['rzzl']
            }
            ret.append(row)
        return ret
    except Exception, e:
        print fund_code + e
    finally:
        if httpClient:
            httpClient.close()

def getNameFromDB(fund_code):
    conn = getConn()
    cursor = conn.cursor()
    cursor.execute('select name from stocks where code=?', (fund_code,))
    row = cursor.fetchone()
    if row:
        return row['name']

def getName(fund_code):
    try:
        httpClient = httplib.HTTPConnection('so.hexun.com', 80, timeout=10)
        httpClient.request('GET', '/ajax.do?type=fund&key=' + fund_code)
        response = httpClient.getresponse()
        fund_str=response.read().decode(encoding='GBK')
        index_begin=fund_str.index('[')
        index_end=fund_str.rindex(']')
        json_data=json.loads(fund_str[index_begin+1:index_end])
        if json_data['name']:
            return json_data['name']
        else:
            return getName(fund_code)
    except Exception, e:
        print fund_code + e
    finally:
        if httpClient:
            httpClient.close

def getValuation(fund_code):
    try:
        httpClient = httplib.HTTPConnection('fundexh5.eastmoney.com', 80, timeout=10)
        httpClient.request('GET', '/fundwapapi/FundBase.ashx?callback=jsonp1&FCODE=' + fund_code)
        response = httpClient.getresponse()
        fund_str=response.read().decode(encoding='utf-8')
        index_begin=fund_str.index('(')
        index_end=fund_str.rindex(')')
        json_data=json.loads(fund_str[index_begin+1:index_end])
        v = json_data['Datas']['Valuation']
        v = json.loads(v)
        v_date = unix_timestamp(v['gztime'], '%Y-%m-%d %H:%M')
        row = {
            'code' : fund_code,
            'date' : v_date,
            'dwjz' : v['gsz'],
            'rzzl' : v['gszzl'],
        }
        return row
    except Exception, e:
        print fund_code + e
    finally:
        if httpClient:
            httpClient.close

def initFundListFromDB(mod, lock):
    print multiprocessing.current_process().name + ":initFundListFromDB:start"
    nameList = []
    dailyList = []
    valuationList = []
    for i in fundcode_list:
        if int(i) % THREADS_NUM != mod:
            continue

        lock.acquire()

        # 获取stock名称
        name = getNameFromDB(i)
        arr = {
            'code' : i,
            'name' : name
        }
        if name:
            nameList.append(arr)

        # 获取stock历史收益
        tmpdata = getDailyDataFromDB(i)
        if tmpdata:
            dailyList.append(tmpdata)

        lock.release()

        # 获取stock最新估值
        tmpdata = getValuation(i)
        if tmpdata:
            valuationList.append(tmpdata)

    ret = {
        'name' : nameList,
        'daily' : dailyList,
        'valuation' : valuationList
    }
    print multiprocessing.current_process().name + ":initFundListFromDB:end"
    return ret

def initFundList(mod):
    nameList = []
    dailyList = []
    valuationList = []
    for i in fundcode_list:
        if int(i) % THREADS_NUM != mod:
            continue

        # 获取stock名称
        name = getName(i)
        arr = {
            'code' : i,
            'name' : name
        }
        if name:
            nameList.append(arr)

        # 获取stock历史收益
        tmpdata = getDailyData(i)
        if tmpdata:
            dailyList.append(tmpdata)

        # 获取stock最新估值
        tmpdata = getValuation(i)
        if tmpdata:
            valuationList.append(tmpdata)

    ret = {
        'name' : nameList,
        'daily' : dailyList,
        'valuation' : valuationList
    }
    return ret

def gpdx():
    conn = getConn()
    cursor = conn.cursor()
    # 按照累计两天高抛低吸策略给出建议
    gpdx_list = []
    for i in fundcode_list:
        zzl = 100
        # 查询最新两天实际净值
        cursor.execute('select date,rzzl from stock_daily where code=? order by date desc limit 2', (i,))
        list_daily = cursor.fetchall()
        # 查询最新估值
        cursor.execute('select date,rzzl from stock_valuation where code=? order by date desc limit 1', (i,))
        row_valuation = cursor.fetchone()
        if not row_valuation:
            continue
        # 判断最新净值是否已出
        if list_daily[1]['date'] == row_valuation['date']:
            row_1 = list_daily[2]
            row_2 = list_daily[1]
        else:
            row_1 = list_daily[1]
            row_2 = row_valuation
        # 只要有一天涨，则不考虑
        if row_1['rzzl'] > 0 or row_2['rzzl'] > 0:
            continue
        # 计算及存储中间结果
        zzl = zzl * (100+row_1['rzzl'])/100 * (100+row_2['rzzl'])/100
        if zzl > 98:
            continue
        zzl = formatPercent(zzl - 100)
        date_1 = formatYmd(row_1['date'])
        zzl_1 = formatPercent(row_1['rzzl'])
        date_2 = formatYmd(row_2['date'])
        zzl_2 = formatPercent(row_2['rzzl'])

        row = {
            'code' : i,
            'name' : getNameFromDB(i),
            'date_1' : date_1,
            'zzl_1' : zzl_1,
            'date_2' : date_2,
            'zzl_2' : zzl_2,
            'zzl' : zzl,
        }
        gpdx_list.append(row)

    gpdx_list = sorted(gpdx_list, key = lambda x:x['zzl'])
    for r in gpdx_list:
        print bcolors.GREEN + r['code'] + "\t" + r['name'].ljust(20) + "\t" + r['zzl'] + "\t" + r['date_1'] + "," + r['zzl_1'] + "\t" + r['date_2'] + "," + r['zzl_2'] + bcolors.ENDC

# 模拟历史收益
def lssy(i, test_days):
    conn = getConn()
    cursor = conn.cursor()
    cursor.execute('''select date,rzzl from stock_daily
                   where code=? and date in
                   (select date from stock_daily where code=? order by date desc limit ?)
                   order by date''', (i,i,test_days,))
    list = cursor.fetchall()
    buy_days = 0
    profit = 100
    hold_profit = 100
    buy_log = ""
    for row in list:
        rzzl = row['rzzl']
        hold_profit = hold_profit * (100+rzzl)/100
        if rzzl > 0:
            if buy_days >= 2:
                buy_days = 0
                profit = profit * (100+rzzl)/100 * 0.995
                buy_log = buy_log + i + " sell:" + formatYmd(row['date']) + " " + formatPercent(row['rzzl']) + " " + formatPercent(profit-100) + "\n"
            elif buy_days == 1:
                buy_days = buy_days + 1
                profit = profit * (100+rzzl)/100
                buy_log = buy_log + i + " hold:" + formatYmd(row['date']) + " " + formatPercent(row['rzzl']) + "\n"
            else:
                buy_days = 0
        else:
            if buy_days <= -1:
                buy_days = 1
                profit = profit * 0.9988
                buy_log = buy_log + i + " buy:" + formatYmd(row['date']) + " " + formatPercent(row['rzzl']) + "\n"
            elif buy_days == 0:
                buy_days = buy_days - 1
            else:
                profit = profit * (100+rzzl)/100
                buy_log = buy_log + i + " hold:" + formatYmd(row['date']) + " " + formatPercent(row['rzzl']) + "\n"
    ret = {
        'code' : i,
        'name' : getNameFromDB(i),
        'profit' : profit - 100,
        'hold_profit' : hold_profit - 100,
        'buy_log' : buy_log
    }
    return ret

def all_lssy(test_days):
    conn = getConn()
    cursor = conn.cursor()
    lssy_list = []
    for i in fundcode_list:
        ret = lssy(i, test_days)
        lssy_list.append(ret)
    lssy_list = sorted(lssy_list, key = lambda x:x['profit'])
    for r in lssy_list:
        if r['profit'] > 0:
            print (bcolors.RED + r['code'] + "\t" + r['name'].ljust(20) + "\t" 
                   + formatPercent(r['profit']) + " vs " + formatPercent(r['hold_profit']) + bcolors.ENDC)
        else:
            print (bcolors.GREEN + r['code'] + "\t" + r['name'].ljust(20) + "\t" 
                   + formatPercent(r['profit']) + " vs " + formatPercent(r['hold_profit']) + bcolors.ENDC)


def insertToDB(arr):
    conn = getConn()
    cursor = conn.cursor()
    for n in arr['name']:
        cursor.execute('replace into stocks values(?,?,?)', (n['code'], n['name'], 0))
    for ds in arr['daily']:
        for d in ds:
            cursor.execute('replace into stock_daily(code,date,dwjz,ljjz,rzzl) values(?,?,?,?,?)',
                           (d['code'], d['date'], d['dwjz'], d['ljjz'], d['rzzl']))
    for v in arr['valuation']:
        v['date'] = unix_timestamp(formatYmd(v['date']), '%Y-%m-%d')
        cursor.execute('replace into stock_valuation(code,date,dwjz,rzzl) values(?,?,?,?)',
                       (v['code'], v['date'], v['dwjz'], v['rzzl']))
    conn.commit()

def listFromDB():
    lock = Lock()
    p = Pool(THREADS_NUM)
    for i in range(THREADS_NUM):
        p.apply_async(initFundListFromDB, args = (i,lock))
    p.close()
    p.join()

def list():
    p = Pool(THREADS_NUM)
    for i in range(THREADS_NUM):
        p.apply_async(initFundList, args = (i,), callback = insertToDB)
    p.close()
    p.join()

    conn = getConn()
    cursor = conn.cursor()
    cursor.execute('select code,name from stocks order by code')
    return cursor.fetchall()

def main():
    global fundcode_list
    global THREADS_NUM
    THREADS_NUM = 20
    # 初始化fundcode列表
    fundcode_list = readConf()
    # 创建所需数据表（如果不存在的话）
    createTable()

    # 选项菜单
    p = OptionParser(description='show me the money',
                              prog='./fund.py',
                              version='fund 0.1',
                              usage='%prog 163113 30')
    p.add_option('-d', '--daily', action ='store_true', help='get daily jz/zzl of the fund')
    p.add_option('-l', '--list', action ='store_true', help='show the fund list')
    p.add_option('-n', '--name', action ='store_true', help='get name for the fund')
    p.add_option('-s', '--suggest', action ='store_true', help='which fund you can buy today')
    p.add_option('-v', '--valuation', action ='store_true', help='get current valuation of the fund')
    p.add_option('-c', '--checkdiff', action ='store_true', help='check if valuation is ok')
    options, arguments = p.parse_args()
    if len(arguments) == 1:
        if options.name:
            print getNameFromDB(arguments[0])
        elif options.daily:
            l = getDailyDataFromDB(arguments[0])
            for r in l:
                print getNameFromDB(arguments[0]) + "\t" + formatYmd(r['date']) + "\t" + formatPercent(r['rzzl'])
        elif options.valuation:
            r = getValuation(arguments[0])
            print getNameFromDB(arguments[0]) + "\t" + formatYmdHm(r['date']) + "\t" + formatPercent(float(r['rzzl']))
        else:
            p.print_help()
    elif len(arguments) == 2:
        if arguments[0] == 'all':
            all_lssy(arguments[1])
        else:
            r = lssy(arguments[0], arguments[1])
            print r['buy_log']
            print (bcolors.RED + r['code'] + "\t" + r['name'].ljust(20) + "\t" 
                   + formatPercent(r['profit']) + " vs " + formatPercent(r['hold_profit']) + bcolors.ENDC)
    else:
        if options.list:
            l = list()
            for row in l:
                print row['code'] + ":" + row['name']
        elif options.suggest:
            listFromDB()
            gpdx()
        elif options.checkdiff:
            checkdiff()
        else:
            p.print_help()

if __name__ == "__main__":
    main()
