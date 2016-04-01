# coding=utf8
import sys
import httplib
import json
import time
import sqlite3

class bcolors:
    GREEN = '\033[1;32;40m'
    RED = '\033[1;31;40m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def getFundData(fund_code, days):
    curtime = str(time.time()) + '353';
    httpClient = None

    try:
        httpClient = httplib.HTTPConnection('caifu.baidu.com', 80, timeout=10)
        httpClient.request('GET', '/trade/fund/jzlist?fund_code=' + fund_code + '&cur_page=1&page_size=' + str(days) + '&_=' + curtime)

        response = httpClient.getresponse()
#    print response.status
#    print response.reason
        jijin_json = json.loads(response.read())
        data = jijin_json['data']['list']
        cur_money = 10000.0
        last_ljjz = 0.0
        buy_days = 0
        up_days = 0
        down_days = 0
        profit_times = 0
        loss_times = 0

        data = sorted(data, key = lambda x:x['date'])
        show_date = data[0]['date']

        for day_data in data:
            cur_date = day_data['date']
            cur_dwjz = day_data['dwjz']
            cur_ljjz = day_data['ljjz']
            cur_rzzl = 1+float(day_data['rzzl'])/100
            if last_ljjz == 0.0:
                last_ljjz = cur_ljjz
            else:
                if cur_rzzl > 1:
                    up_days += 1
                    down_days = 0
                else:
                    up_days = 0
                    down_days += 1

                if buy_days > 0:
                    buy_days += 1

                if down_days >= 2 and buy_days == 0:
                    buy_days = 1

                if buy_days == 1:
                    cur_money *= 0.9988
                    buy_money = cur_money
                    #print cur_date + ":buy " + str(round(cur_money,2))
                elif buy_days >= 3 and up_days > 0:
                    cur_money *= cur_rzzl
                    cur_money *= 0.995
                    sell_money = cur_money
                    if sell_money > buy_money:
                        profit_times += 1
                    else:
                        loss_times += 1
                    #print cur_date + ":sell " + str(round(cur_money,2)) + ", " + str(round(100*(cur_rzzl-1),2)) + "%"
                    buy_days = 0
                    down_days = 0
                    up_days = 0
                elif buy_days >= 2:
                    cur_money *= cur_rzzl
                    #print cur_date + ":hold " + str(round(100*(cur_rzzl-1),2)) + "%"
                else:
                    tmp=0
                    #print "pass:" + cur_date

                last_ljjz = cur_ljjz

        httpClient = httplib.HTTPConnection('so.hexun.com', 80, timeout=10)
        httpClient.request('GET', '/ajax.do?type=fund&key=' + fund_code)
        response = httpClient.getresponse()
        fund_str=response.read().decode(encoding='GBK')
        index_begin=fund_str.index('[')
        index_end=fund_str.index(']')
        json_data=json.loads(fund_str[index_begin+1:index_end])
        ret = {
                'date' : show_date,
                'name' : json_data['name'],
                'code' : json_data['code'],
                'money' : cur_money,
                'profit' : profit_times,
                'loss' : loss_times
                }
        return ret
    except Exception, e:
        print e
    finally:
        if httpClient:
            httpClient.close()


#fund_code = raw_input("请输入基金代号：")
days = sys.argv[1] #raw_input("请输入天数：")
ret_list=[]
#conn = sqlite3.connect("db")
#cursor = conn.cursor()
#cursor.execute('create table if not exists jj(id varchar(20) primary key, name varchar(20))')
#cursor.execute('insert into jj(id,name) values ("321", "ms")')
#cursor.execute('select * from jj where id=?', "321")
#values = cursor.fetchall()
#values
#sys.exit("sorry, goodbye!")
file = open("list")
fund_list = file.readlines()
for fund_code in fund_list:
    print fund_code.strip()
    fund_code = fund_code.strip()
    ret_list.append(getFundData(fund_code, days))

ret_list = sorted(ret_list, key = lambda x:x['money'])
for ret in ret_list:
    name = ret['name'] + "[" + ret['code'] + "]"
    if ret['money'] > 10000:
        print bcolors.RED + ret['date'] + "\t" + name.ljust(20) + "\t" + str(round(ret['money'],2)).rjust(8) + "\t profit:" + str(ret['profit']) + "\t loss:" + str(ret['loss']) + bcolors.ENDC
    else:
        print bcolors.GREEN + ret['date'] + "\t" + name.ljust(20) + "\t" + str(round(ret['money'],2)).rjust(8) + "\t profit:" + str(ret['profit']) + "\t loss:" + str(ret['loss']) + bcolors.ENDC
