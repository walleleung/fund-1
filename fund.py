#!/usr/bin/env python
#coding=utf-8
import os,sys,httplib,json,time,datetime,sqlite3
from optparse import OptionParser
from multiprocessing import Pool

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
    return datetime.date.fromtimestamp(unixtime).strftime('%Y-%m-%d')

def formatYmdHm(unixtime):
    return datetime.datetime.fromtimestamp(unixtime).strftime('%Y-%m-%d %H:%M')

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
                   gsz real not null,
                   gszzl real not null,
                   unique (code, date)
                   );''')
    conn.commit()

def getDailyData(fund_code):
    # 优先从DB获取数据
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    while yesterday.isoweekday() >= 6:
        yesterday = yesterday - datetime.timedelta(days=1)
    cursor = conn.cursor()
    cursor.execute('select count(*) from stock_daily where code=? and date=?', (fund_code,yesterday.strftime("%s")))
    row = cursor.fetchone()
    if row[0] > 0:
        cursor.execute('select date,rzzl from stock_daily where code=? order by date', (fund_code,))
        return cursor.fetchall()

    curtime = str(time.time()) + '666';
    httpClient = None
    try:
        httpClient = httplib.HTTPConnection('caifu.baidu.com', 80, timeout=10)
        httpClient.request('GET', '/trade/fund/jzlist?fund_code=' + fund_code + '&cur_page=1&page_size=1000&_=' + curtime)

        response = httpClient.getresponse()
        jijin_json = json.loads(response.read())
        data = jijin_json['data']['list']
        for d in data:
            d_date = unix_timestamp(d['date'], '%Y-%m-%d')
            cursor.execute('replace into stock_daily(code,date,dwjz,ljjz,rzzl) values(?,?,?,?,?)',
                           (fund_code, d_date, d['dwjz'], d['ljjz'], d['rzzl']))
        conn.commit()
        cursor.execute('select date,rzzl from stock_daily where code=? order by date', (fund_code,))
        return cursor.fetchall()
    except Exception, e:
        print e
    finally:
        if httpClient:
            httpClient.close()

def getName(fund_code):
    cursor = conn.cursor()
    cursor.execute('select name from stocks where code=?', (fund_code,))
    row = cursor.fetchone()
    if row:
        return row['name']
    try:
        httpClient = httplib.HTTPConnection('so.hexun.com', 80, timeout=10)
        httpClient.request('GET', '/ajax.do?type=fund&key=' + fund_code)
        response = httpClient.getresponse()
        fund_str=response.read().decode(encoding='GBK')
        index_begin=fund_str.index('[')
        index_end=fund_str.rindex(']')
        json_data=json.loads(fund_str[index_begin+1:index_end])
        print fund_code + ":" + json_data['name']
        return json_data['name']
    except Exception, e:
        print e
    finally:
        if httpClient:
            httpClient.close

def getValuation(fund_code):
    cursor = conn.cursor()
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
        cursor.execute('replace into stock_valuation(code,date,gsz,gszzl) values(?,?,?,?)',
                       (fund_code, v_date, v['gsz'], v['gszzl']))
        conn.commit()
        cursor.execute('select code,date,gszzl from stock_valuation where code=? and date=?', (fund_code, v_date))
        return cursor.fetchone()
    except Exception, e:
        print e
    finally:
        if httpClient:
            httpClient.close

def initFundList(mod):
    cursor = conn.cursor()
    # 获取stock名称
    for i in fundcode_list:
        if int(i) % mod != 0:
            continue
        cursor.execute('select count(*) from stocks where code=?', (i,))
        row = cursor.fetchone()
        if row[0] > 0:
            continue
        name = getName(i)
        cursor.execute('insert into stocks values(?,?,?)', (i, name, 0))
    conn.commit()
    # 获取stock历史收益
    for i in fundcode_list:
        if int(i) % mod != 0:
            continue
        getDailyData(i)
    conn.commit()
    # 获取stock最新估值
    for i in fundcode_list:
        if int(i) % mod != 0:
            continue
        getValuation(i)
    conn.commit()

def gpdx():
    cursor = conn.cursor()
    # 按照累计两天高抛低吸策略给出建议
    gpdx_list = []
    for i in fundcode_list:
        zzl = 1
        cursor.execute('select date,rzzl from stock_daily where code=? order by date desc limit 1', (i,))
        row = cursor.fetchone()
        if row['rzzl'] > 0:
            continue
        zzl = zzl * (100+row['rzzl'])/100
        date_1 = formatYmd(row['date'])
        zzl_1 = formatPercent(row['rzzl'])
        cursor.execute('select date,gszzl from stock_valuation where code=? order by date desc limit 1', (i,))
        row = cursor.fetchone()
        if row['gszzl'] > 0:
            continue
        zzl = zzl * (100+row['gszzl'])/100
        date_2 = formatYmd(row['date'])
        if date_1 == date_2:
            continue
        if zzl > 98:
            continue
        zzl = formatPercent(100-zzl)
        zzl_2 = formatPercent(row['gszzl'])
        row = {
            'code' : i,
            'name' : getName(i),
            'date_1' : date_1,
            'zzl_1' : zzl_1,
            'date_2' : date_2,
            'zzl_2' : zzl_2,
            'zzl' : zzl,
        }
        gpdx_list.append(row)

    gpdx_list = sorted(gpdx_list, key = lambda x:x['zzl'])
    for r in gpdx_list:
        print bcolors.GREEN + r['code'] + "\t" + r['name'].ljust(20) + "\t" + "-" + r['zzl'] + "\t" + r['date_1'] + "," + r['zzl_1'] + "\t" + r['date_2'] + "," + r['zzl_2'] + bcolors.ENDC
    print "get gpdx end"

# 模拟历史收益
def lssy(i, test_days):
    cursor = conn.cursor()
    cursor.execute('''select date,rzzl from stock_daily
                   where code=? and date in
                   (select date from stock_daily where code=? order by date desc limit ?)
                   order by date''', (i,i,test_days,))
    list = cursor.fetchall()
    buy_days = 0
    profit = 100
    hold_profit = 100
    for row in list:
        rzzl = row['rzzl']
        hold_profit = hold_profit * (100+rzzl)/100
        if rzzl > 0:
            if buy_days >= 2:
                buy_days = 0
                profit = profit * (100+rzzl)/100 * 0.995
                print i + " sell:" + formatYmd(row['date']) + " " + formatPercent(row['rzzl']) + " " + formatPercent(profit-100)
            elif buy_days == 1:
                buy_days = buy_days + 1
                profit = profit * (100+rzzl)/100
                print i + " hold:" + formatYmd(row['date']) + " " + formatPercent(row['rzzl'])
            else:
                buy_days = 0
        else:
            if buy_days <= -1:
                buy_days = 1
                profit = profit * 0.9988
                print i + " buy:" + formatYmd(row['date']) + " " + formatPercent(row['rzzl'])
            elif buy_days == 0:
                buy_days = buy_days - 1
            else:
                profit = profit * (100+rzzl)/100
                print i + " hold:" + formatYmd(row['date']) + " " + formatPercent(row['rzzl'])
    ret = {
        'profit' : profit - 100,
        'hold_profit' : hold_profit - 100
    }
    return ret

def all_lssy(test_days):
    cursor = conn.cursor()
    lssy_list = []
    for i in fundcode_list:
        ret = lssy(i, test_days)
        if ret['profit'] > 10:
            row = {
                'code' : i,
                'name' : getName(i),
                'profit' : ret['profit'],
                'hold_profit' : ret['hold_profit']
            }
            lssy_list.append(row)
    lssy_list = sorted(lssy_list, key = lambda x:x['profit'])
    for r in lssy_list:
        print (bcolors.RED + r['code'] + "\t" + r['name'].ljust(20) + "\t" 
               + formatPercent(r['profit']) + " vs " + formatPercent(r['hold_profit']) + bcolors.ENDC)

def list():
    p = Pool(10)
    for i in range(10):
        p.apply_async(initFundList, args=(i,))
    p.close()
    p.join()
    cursor = conn.cursor()
    cursor.execute('select code,name from stocks order by code')
    return cursor.fetchall()

def main():
    global fundcode_list, conn
    # 初始化fundcode列表
    fundcode_list = readConf()
    # 创建所需数据表（如果不存在的话）
    conn = getConn()
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
    options, arguments = p.parse_args()
    if len(arguments) == 1:
        if options.name:
            print getName(arguments[0])
        elif options.daily:
            l = getDailyData(arguments[0])
            for r in l:
                print getName(arguments[0]) + "\t" + formatYmd(r['date']) + "\t" + formatPercent(r['rzzl'])
        elif options.valuation:
            r = getValuation(arguments[0])
            print getName(arguments[0]) + "\t" + formatYmdHm(r['date']) + "\t" + formatPercent(r['gszzl'])
        else:
            p.print_help()
    elif len(arguments) == 2:
        if arguments[0] == 'all':
            all_lssy(arguments[1])
        else:
            lssy(arguments[0], arguments[1])
    else:
        if options.list:
            l = list()
            for row in l:
                print row['code'] + ":" + row['name']
        elif options.suggest:
            gpdx()
        else:
            p.print_help()

if __name__ == "__main__":
    main()
