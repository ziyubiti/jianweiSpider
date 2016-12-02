# -*- coding: utf-8 -*-
"""
@author: ziyubiti
@site: http://ziyubiti.github.io
@date: 20150908
"""


import matplotlib.pyplot as plt
import numpy as np
from jianwei_wangqian import *


def read_data_from_mysql(conn):
    
    cursor = conn.cursor()   
    # day
    cursor.execute('select * from jianwei_trades_perday')
    values = cursor.fetchall()      
    day = []
    daytrades = []
    for nvs in values:
        day.append(nvs[1])
        daytrades.append(nvs[5])
        
    # week    
    cursor.execute('select * from jianwei_trades_weekly')
    values = cursor.fetchall()    
    weekstr = []
    weektrades = []
    for nvs in values:
        weekstr.append(nvs[1])
        weektrades.append(nvs[3])
        
    # month    
    cursor.execute('select * from jianwei_trades_month')
    values = cursor.fetchall()    
    month = []
    monthtrades = []
    for nvs in values:
        month.append(nvs[1])
        monthtrades.append(nvs[2])
        
    conn.commit()
    cursor.close()

    return [day,daytrades,weekstr,weektrades,month,monthtrades]

    
    

def plot_trades_daily(x, y):    
    n = len(y)
    ind = np.arange(n)    
    width = 0.3
    plt.figure(figsize=[25,5])
    plt.bar(ind,y, width,align="center")
    plt.title('daily trades number')
    plt.ylabel('trades number')
    plt.xlim(-1, n)
    plt.xticks(ind[1:-1:7]-11*width,x[1:-1:7],rotation=45)
    
    
    plt.minorticks_on()
    plt.grid(True)   
    #plt.show()
    plt.tight_layout()
    plt.savefig("daily_trades.png")
    
 

def plot_trades_weekly(x, y):    
    n = len(y)
    ind = np.arange(n)    
    width = 0.5
    plt.figure(figsize=[25,5])
    plt.bar(ind,y, width,align="center")
    plt.title('weekly trades number')
    plt.ylabel('trades number')
    plt.xlim(-1, n)
    plt.xticks(ind-3.0*width,x,rotation=45)
    
    
    plt.minorticks_on()
    plt.grid(True)   
    #plt.show()
    plt.tight_layout()
    plt.savefig("weekly_trades.png")



def plot_trades_month(x, y):    
    n = len(y)
    ind = np.arange(n)    
    width = 0.5
    plt.figure(figsize=[25,5])
    plt.bar(ind,y, width,align="center")
    plt.title('month trades number')
    plt.ylabel('trades number')
    plt.xlim(-1, n)
    plt.xticks(ind,x,rotation=0)
    
    
    plt.minorticks_on()
    plt.grid(True)   
    #plt.show()
    plt.tight_layout()
    plt.savefig("month_trades.png")



def jianwei_plot_main():     
    dbflag = 'local'            # local,  remote
    conn = database_init(dbflag)
    data = read_data_from_mysql(conn)
    conn.close()
    
    plot_trades_daily(data[0],data[1])
    plot_trades_weekly(data[2],data[3])
    plot_trades_month(data[4],data[5])
    

