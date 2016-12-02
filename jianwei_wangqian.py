# -*- coding: utf-8 -*-
"""
@author: ziyubiti
@site: http://ziyubiti.github.io
@date: 20150908
"""


from urllib.request import urlopen,Request
from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup
import random
import mysql.connector
from datetime import date

hds=[{'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},\
    {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},\
    {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},\
    {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},\
    {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}]


#=========================setup a database, and conn=================================
def database_init(dbflag='local'):
     if dbflag=='local':
         conn = mysql.connector.connect(user='root', password='password', database='lianjiaSpider',host='localhost')
     else:
         conn = mysql.connector.connect(user='qdm194', password='password', database='qdm194',host='qdm1944.my3w.com')
     dbc = conn.cursor()

     # 创建jianwei_trades表:
     dbc.execute('create table if not exists jianwei_trades_perday (ID int(20) NOT NULL AUTO_INCREMENT primary key,daystr varchar(20) , month int(10), weekno int(10), dayinweek int(5), trades int(20))')
     dbc.execute('create table if not exists jianwei_trades_weekly (ID int(20) NOT NULL AUTO_INCREMENT primary key,dayweekstr varchar(200) , weekno int(10),trades int(20))')
     dbc.execute('create table if not exists jianwei_trades_month (ID int(20) NOT NULL AUTO_INCREMENT primary key, month int(15),trades int(20))')

     conn.commit()
     dbc.close()
     return conn


def jianwei_wangqian_spider():

    url=u"http://www.bjjs.gov.cn/tabid/2167/default.aspx"

    try:
        req = Request(url,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urlopen(req,timeout=50).read()
        soup = BeautifulSoup(source_code,'lxml')
    except HTTPError as e:
        print (e)
        return
    except Exception as e:
        print (e)
        return

    nameList = soup.find("span", {"id":"ess_ctr5112_FDCJY_SignOnlineStatistics_residenceCount4"})
    nameDay = soup.find("span", {"id":"ess_ctr5112_FDCJY_SignOnlineStatistics_timeMark2"})
    nameYear = soup.find("span", {"id":"ess_ctr5112_FDCJY_SignOnlineStatistics_year"})
    nameMonth = soup.find("span", {"id":"ess_ctr5112_FDCJY_SignOnlineStatistics_month"})
    nameMonthList = soup.find("span", {"id":"ess_ctr5112_FDCJY_SignOnlineStatistics_residenceCount3"})


    year = nameYear.get_text()
    month = nameMonth.get_text()
    if int(month)<10:
        month = '0'+month
    fullmonth = year + month

    info_dict = {}
    info_dict.update({u'trades':nameList.get_text()})
    info_dict.update({u'day':nameDay.get_text()})
    info_dict.update({u'month':fullmonth})
    info_dict.update({u'monthtrades':nameMonthList.get_text()})

    return info_dict



def wangqian_insert_mysql(conn,info_dict):

    dayinfo = get_weekno_dayinweek(info_dict[u'day'])

    # update perday table  if found no today record
    cursor = conn.cursor()
    cursor.execute('select * from jianwei_trades_perday where daystr = (%s)',(dayinfo[0],))
    values = cursor.fetchall()         #turple type

    if len(values)==0:        # new record
        t = ( dayinfo[0], dayinfo[1],dayinfo[2],dayinfo[3],int(info_dict[u'trades']))
        cursor.execute('insert into jianwei_trades_perday (daystr,month,weekno,dayinweek,trades) values (%s,%s,%s,%s,%s)', t)
    else:
        pass

    conn.commit()
    cursor.close()


   # update lastweek table  if found no last week record
    cursor = conn.cursor()
    cursor.execute('select * from jianwei_trades_weekly where weekno = (%s) ',(dayinfo[5],))
    values = cursor.fetchall()
    tmpyear = values[-1][1][0:4]
    dayinfoyear = dayinfo[0][0:4]

    if (len(values)!=0) & (tmpyear!=dayinfoyear):        # new record
        cursor.execute('select * from jianwei_trades_perday where weekno = (%s) ',(dayinfo[5],))
        values2 = cursor.fetchall()

        lastweek = values2[-7:]
        weeksum = 0
        for nvs in lastweek:
            weeksum += nvs[5]
        weekstr = lastweek[0][1]+ u' to '+ lastweek[6][1]
        t2 = (weekstr,dayinfo[2]-1,weeksum)
        cursor.execute('insert into jianwei_trades_weekly (dayweekstr,weekno,trades) values (%s,%s,%s)', t2)
    else:
        pass

    conn.commit()
    cursor.close()


    #update lastmonth table  if found no last month record
    cursor = conn.cursor()
    cursor.execute('select * from jianwei_trades_month where month = (%s) ',(info_dict[u'month'],))
    values = cursor.fetchall()

    if (len(values)==0) :        # new record
#        cursor.execute('select * from jianwei_trades_perday where month = (%s) ',(info_dict[u'month'],))
#        values2 = cursor.fetchall()
#
#        monthsum = 0
#        for nvs in values2:
#            monthsum += nvs[5]
        t2 = (info_dict[u'month'],info_dict[u'monthtrades'])
        cursor.execute('insert into jianwei_trades_month (month,trades) values (%s,%s)', t2)
    else:
        pass
    conn.commit()
    cursor.close()





def get_weekno_dayinweek(day):
    d2 = day.split('-')
    d3 = date(int(d2[0]),int(d2[1]),int(d2[2]))
    d4 = d3.isocalendar()
    d5 = d3.isoformat()
    d6 = d5.split('-')
    #lastmonth = 1
    #lastweek = 1
    if d6[1] == '01':                                 # month
        lastmonth = int(str(int(d6[0])-1)+'12')
    else:
        lastmonth = int(d6[0]+d6[1]) - 1
    if d4[1] == 1:                                    # weekno
        if (d6[1] == '01') & (d6[2] == '01'):
            tmp = date(d3.year-1,12,31)
        else:
            tmp = date(d3.year,d3.month,d3.day-1)     # this logic only is valid in d4[2]=1, to be fixed bug.
        lastweek = tmp.isocalendar()[1]
    else:
        lastweek = d4[1]-1                            # so only week 1 when bug occur.
    return [d5,int(d6[0]+d6[1]),d4[1],d4[2],lastmonth,lastweek]        # fullday,fullmonth: 201609,  weekno, dayinweek,lastmonth






def jianwei_wangqian_main():
    info_dict = jianwei_wangqian_spider()   # scrapy data per day
    dbflag = 'local'            # local,  remote
    conn = database_init(dbflag)
    wangqian_insert_mysql(conn,info_dict)   # insert into mysql, update week table once monday, update month table once 1st day
    conn.close()
