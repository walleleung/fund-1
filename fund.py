#coding=utf-8
import sys
import httplib
import json
import time
import datetime
import sqlite3

class bcolors:
    GREEN = '\033[1;32;40m'
    RED = '\033[1;31;40m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

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

# 连接数据库
conn = sqlite3.connect('db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

#创建所需数据表（如果不存在的话）
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

# 获取stock列表
fund_list = []
file = open("list")
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
conn.commit()
print "get name end"

#获取stock每日数据
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
print "get daily data end"

#获取stock估值
for i in fund_list:
    v= getValuation(i)
    v = json.loads(v)
    v_date = datetime.datetime.strptime(v['gztime'], '%Y-%m-%d %H:%M').strftime("%s")
    cursor.execute('replace into stock_valuation(code,date,gsz,gszzl) values(?,?,?,?)', 
                   (i, v_date, v['gsz'], v['gszzl']))
conn.commit()
print "get valuation end"

#按照累计两天高抛低吸策略给出建议
for i in fund_list:
    cursor.execute('select date,rzzl from stock_daily where code=? order by date desc limit 1', (i,))
    row = cursor.fetchone()
    if row['rzzl'] > 0:
        continue
    date_1 = datetime.date.fromtimestamp(row['date']).strftime('%Y-%m-%d')
    zzl_1 = str(row['rzzl']) + "%"
    cursor.execute('select date,gszzl from stock_valuation where code=? order by date desc limit 1', (i,))
    row = cursor.fetchone()
    if row['gszzl'] > 0:
        continue
    date_2 = datetime.date.fromtimestamp(row['date']).strftime('%Y-%m-%d')
    zzl_2 = str(row['gszzl']) + "%"
    print bcolors.GREEN + i + "\t" + getName(i).ljust(20) + "\t" + date_1 + " " + zzl_1 + "\t" + date_2 + " " + zzl_2 + bcolors.ENDC

conn.close()
