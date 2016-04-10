#!/usr/bin/env python
#coding=utf-8
import sys
import httplib
import json
import time
import datetime
import sqlite3
from optparse import OptionParser

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

def createTable():
    global cursor
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

def getDailyData(fund_code, days):
    curtime = str(time.time()) + '353';
    httpClient = None

    try:
        httpClient = httplib.HTTPConnection('caifu.baidu.com', 80, timeout=10)
        httpClient.request('GET', '/trade/fund/jzlist?fund_code=' + fund_code + '&cur_page=1&page_size=' + str(days) + '&_=' + curtime)

        response = httpClient.getresponse()
        jijin_json = json.loads(response.read())
        data = jijin_json['data']['list']
        data = sorted(data, key = lambda x:x['date'])
        return data
    except Exception, e:
        print e
    finally:
        if httpClient:
            httpClient.close()

def getName(fund_code):
    try:
        httpClient = httplib.HTTPConnection('so.hexun.com', 80, timeout=10)
        httpClient.request('GET', '/ajax.do?type=fund&key=' + fund_code)
        response = httpClient.getresponse()
        fund_str=response.read().decode(encoding='GBK')
        index_begin=fund_str.index('[')
        index_end=fund_str.rindex(']')
        json_data=json.loads(fund_str[index_begin+1:index_end])
        return json_data['name']
    except Exception, e:
        print e
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
        return json_data['Datas']['Valuation']
    except Exception, e:
        print e
    finally:
        if httpClient:
            httpClient.close

def initFundList(file_name):
    global cursor
    fund_list = []
    file = open(file_name)
    tmplist = file.readlines()
    for line in tmplist:
        fund_list.append(line.strip())
    # 获取stock名称
    for i in fund_list:
        cursor.execute('select count(*) from stocks where code=?', (i,))
        row = cursor.fetchone()
        if row[0] > 0:
            continue
        name = getName(i)
        cursor.execute('insert into stocks values(?,?,?)', (i, name, 0))
        print i + ":" + name

def getFundList():
    global cursor
    # （重新）从DB获取数据，方便扩展
    fund_list = []
    cursor.execute('select code from stocks')
    for row in cursor:
        fund_list.append(row['code'])
    return fund_list

def daily():
    # 获取stock每日数据
    for i in fund_list:
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        while yesterday.isoweekday() >= 6:
            yesterday = yesterday - datetime.timedelta(days=1)
            cursor.execute('select count(*) from stock_daily where code=? and date=?', (i,yesterday.strftime("%s")))
            row = cursor.fetchone()
            if row[0] == 0:
                days = 1000
                daily_data = getDailyData(i, days)
                for d in daily_data:
                    d_date = datetime.datetime.strptime(d['date'], '%Y-%m-%d').strftime("%s")
                    cursor.execute('replace into stock_daily(code,date,dwjz,ljjz,rzzl) values(?,?,?,?,?)',
                                   (i, d_date, d['dwjz'], d['ljjz'], d['rzzl']))
                conn.commit()

def valuation():
    # 获取stock估值
    if datetime.datetime.now().hour < 16:
        for i in fund_list:
            v= getValuation(i)
            v = json.loads(v)
            v_date = datetime.datetime.strptime(v['gztime'], '%Y-%m-%d %H:%M').strftime("%s")
            cursor.execute('replace into stock_valuation(code,date,gsz,gszzl) values(?,?,?,?)',
                           (i, v_date, v['gsz'], v['gszzl']))
            conn.commit()
            print "get valuation end"

def gpdx():
    # 按照累计两天高抛低吸策略给出建议
    gpdx_list = []
    for i in fund_list:
        zzl = 1
        cursor.execute('select date,rzzl from stock_daily where code=? order by date desc limit 1', (i,))
        row = cursor.fetchone()
        if row['rzzl'] > 0:
            continue
        zzl = zzl * (1+row['rzzl']/100)
        date_1 = datetime.date.fromtimestamp(row['date']).strftime('%Y-%m-%d')
        zzl_1 = str(round(row['rzzl'], 2)) + "%"
        cursor.execute('select date,gszzl from stock_valuation where code=? order by date desc limit 1', (i,))
        row = cursor.fetchone()
        if row['gszzl'] > 0:
            continue
        zzl = zzl * (1+row['gszzl']/100)
        date_2 = datetime.date.fromtimestamp(row['date']).strftime('%Y-%m-%d')
        if date_1 == date_2:
            continue
        if zzl > 0.98:
            continue
        zzl = round(1-zzl, 4)
        zzl_2 = str(round(row['gszzl'], 2)) + "%"
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
        print bcolors.GREEN + r['code'] + "\t" + r['name'].ljust(20) + "\t" + "-" + str(r['zzl']*100) + "%" + "\t" + r['date_1'] + "," + r['zzl_1'] + "\t" + r['date_2'] + "," + r['zzl_2'] + bcolors.ENDC
    print "get gpdx end"

# 模拟历史收益
def lssy(i, test_days):
    global cursor
    cursor.execute('''select date,rzzl from stock_daily
                   where code=? and date in
                   (select date from stock_daily where code=? order by date desc limit ?)
                   order by date''', (i,i,test_days,))
    list = cursor.fetchall()
    buy_days = 0
    profit = 1
    for row in list:
        rzzl = row['rzzl']
        if rzzl > 0:
            if buy_days >= 2:
                buy_days = 0
                profit = profit * (1+rzzl/100) * 0.995
                print i + " sell:" + datetime.date.fromtimestamp(row['date']).strftime('%Y-%m-%d') + " " + str(row['rzzl']) + " " + str(round((profit-1)*100, 2)) + "%"
            elif buy_days == 1:
                buy_days = buy_days + 1
                profit = profit * (1+rzzl/100)
                print i + " hold:" + datetime.date.fromtimestamp(row['date']).strftime('%Y-%m-%d') + " " + str(row['rzzl'])
            else:
                buy_days = 0
        else:
            if buy_days <= -1:
                buy_days = 1
                profit = profit * 0.9988
                print i + " buy:" + datetime.date.fromtimestamp(row['date']).strftime('%Y-%m-%d') + " " + str(row['rzzl'])
            elif buy_days == 0:
                buy_days = buy_days - 1
            else:
                profit = profit * (1+rzzl/100)
                print i + " hold:" + datetime.date.fromtimestamp(row['date']).strftime('%Y-%m-%d') + " " + str(row['rzzl'])

def test():
    lssy_list = []
    test_days = 30
    for i in fund_list:
        cursor.execute('''select date,rzzl from stock_daily
                       where code=? and date in
                       (select date from stock_daily where code=? order by date desc limit ?)
                       order by date''', (i,i,test_days,))
        list = cursor.fetchall()
        buy_days = 0
        profit = 1
        hold_profit = 1
        for row in list:
            rzzl = row['rzzl']
            hold_profit = hold_profit * (1+rzzl/100)
            if rzzl > 0:
                if buy_days >= 2:
                    buy_days = 0
                    profit = profit * (1+rzzl/100) * 0.995
                elif buy_days == 1:
                    buy_days = buy_days + 1
                    profit = profit * (1+rzzl/100)
                else:
                    buy_days = 0
            else:
                if buy_days <= -1:
                    buy_days = 1
                    profit = profit * 0.9988
                elif buy_days == 0:
                    buy_days = buy_days - 1
                else:
                    profit = profit * (1+rzzl/100)
        if profit > 1.1:
            row = {
                'code' : i,
                'name' : getName(i),
                'profit' : round(profit-1, 4),
                'hold_profit' : round(hold_profit-1, 4)
            }
            lssy_list.append(row)

    lssy_list = sorted(lssy_list, key = lambda x:x['profit'])
    for r in lssy_list:
        lssy(r['code'], test_days)
        print (bcolors.RED + r['code'] + "\t" + r['name'].ljust(20) + "\t" 
               + str(r['profit']*100) + "% vs " + str(r['hold_profit']*100) + "%" + bcolors.ENDC)
        print "get lssy end"

def main():
    global cursor, fund_list
    # 连接数据库
    conn = sqlite3.connect('db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    fund_list = getFundList()
    # 选项菜单
    p = OptionParser(description='show me the money',
                              prog='./fund.py',
                              version='fund 0.1',
                              usage='%prog 163113 30')
    p.add_option('-c', '--createtable', action ='store_true', help='create tables if not exists')
    p.add_option('-d', '--dailydata', action ='store_true', help='get daily data of the fund')
    p.add_option('-i', '--initconfig', action ='store_true', help='init config from config file')
    p.add_option('-n', '--getname', action ='store_true', help='get name for the fund')
    p.add_option('-s', '--suggest', action ='store_true', help='which fund you can buy today')
    p.add_option('-v', '--valuation', action ='store_true', help='get current valuation of the fund')
    options, arguments = p.parse_args()
    if len(arguments) == 1:
        if options.getname:
            print getName(arguments[0])
        elif options.dailydata:
            print getDailyData(arguments[0], 1000)
        elif options.valuation:
            print getValuation(arguments[0])
        else:
            p.print_help()
    elif len(arguments) == 2:
        lssy(arguments[0], arguments[1])
    else:
        if options.createtable:
            # 创建所需数据表（如果不存在的话）
            createTable()
            conn.commit()
            print "create table end"
        elif options.initconfig:
            # 从配置文件获取stock列表
            initFundList("list")
            conn.commit()
            print "init fund list end"
        elif options.suggest:
            gpdx()
        else:
            p.print_help()

    conn.close()

if __name__ == "__main__":
    main()
