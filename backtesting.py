# -*- coding: utf-8 -*-
"""
test
loop through list of dictionary
list append 比 dataframe append 快
@author: liush
"""

# import python pacakges
import pandas as pd
import numpy as np
import time
import datetime
import re
import matplotlib.pyplot as plt
import os
import math
from WindPy import *
from Sim_PostTradingAnalysis import *

# import user defined modules
import getdata

def round_nearest(x, a):
    return round(round(x / a) * a, -int(math.floor(math.log10(a))))
    
def mm_trade(df_mkt, params_trade): # 做市策略
    contract_multiplier = params_trade['contract_multiplier']
    contract_tick = params_trade['contract_tick']
    volumeratio = params_trade['volumeratio']
    skew_pos = params_trade['skew_pos']
    skew = params_trade['skew']
    IOC_selective = params_trade['IOC_selective']
    IOC_indicator = params_trade['IOC_indicator']
    IOC_price = params_trade['IOC_price']
    spread_type = params_trade['spread_type']
    spread_udf = params_trade['spread']
    spread_adj = params_trade['spread_adj']
    quote_size = params_trade['quote_size']
    alone_order = params_trade['alone_order']
    hedge_target = params_trade['hedge_target']
    hedge_requote = params_trade['hedge_requote']
    quote_withdraw_indicator = params_trade['quote_withdraw_indicator']
    mkt_list = df_mkt.to_dict('records') # list of dictionary
    trade_report = []
    
    orders_list = [] # list of dictionary keys=['type','B/S','size','price','myqueue','totalqueue'] 
    pos = 0
    bid_quote, ask_quote = 0, 0 # 报价初始化
    for i in range(len(df_mkt)):
        tick_now = mkt_list[i] # 第i个切片的行情数据 -> dict
       
        bid1, ask1, bsize1, asize1 = tick_now['bid1'], tick_now['ask1'], tick_now['bsize1'], tick_now['asize1']
        if i != 0:
            tick_last = mkt_list[i - 1] # 上一个切片的行情数据 -> dict
           
        # # 隔夜撤单 如果日期变了 就把前一晚的单子撤掉 本切片不判断成交
        # if tick_now['date'] != tick_last['date']:
        #     del orders_list[:]
        
        # match previous orders
        if len(orders_list) != 0:
            pos = match_orders(orders_list, tick_now, tick_last, trade_report, pos)              

#---------------------------------------------挂单---------------------------------------------- 

        mkt_spread = round((ask1 - bid1) / contract_tick)
        if spread_type == 1:
            quote_spread = spread_udf
        if spread_type == 2:
            quote_spread = mkt_spread + spread_adj
        if spread_type == 3:
            if abs(pos) < skew_pos:
                quote_spread = mkt_spread
            else:
                quote_spread = mkt_spread + 1
        # determine theo price
        if bsize1 / asize1 > volumeratio:
            theo = ask1
        elif asize1 / bsize1 > volumeratio:
            theo = bid1
        else:
            theo = (bid1 + ask1) / 2
        # 理论价调整
        theo = theo - math.floor( pos / skew_pos) * skew * contract_tick
        # 计算报价
        if theo >= ask1:
            bid_quote_new = bid1
            ask_quote_new = bid_quote_new + quote_spread * contract_tick
        elif theo <= bid1:
            ask_quote_new = ask1
            bid_quote_new = ask_quote_new - quote_spread*contract_tick
        else:
            bid_quote_new = round_nearest(theo - quote_spread / 2 * contract_tick, contract_tick)
            ask_quote_new = bid_quote_new + quote_spread * contract_tick
        # 报价保护
        if bid_quote_new > bid1:
            bid_quote_new = bid1
        if ask_quote_new < ask1:
            ask_quote_new = ask1
        # 唯一单判断
        if bid_quote_new == bid1 and bsize1 <= alone_order:
            bid_quote_new -= contract_tick
        if ask_quote_new == ask1 and asize1 <= alone_order:
            ask_quote_new += contract_tick
        # requote
        if bid_quote_new != bid_quote or ask_quote_new != ask_quote or len(orders_list) != 2: # 先删除已有做市单再挂新的做市单
            del orders_list[:]
            bid_quote = round(bid_quote_new, 2)
            ask_quote = round(ask_quote_new, 2)
            bid_queue = get_queue_neworder(bid_quote, tick_now)
            ask_queue = get_queue_neworder(ask_quote, tick_now)
            orders_list.append({'type': 'mm', 'B/S': 1, 'size': quote_size, 'price': bid_quote, 'myqueue': bid_queue, 'totalqueue': bid_queue})
            orders_list.append({'type': 'mm', 'B/S': -1, 'size': quote_size, 'price': ask_quote, 'myqueue': ask_queue, 'totalqueue': ask_queue})
      
            
    return trade_report
    
def get_queue_neworder(quote_price, tick_now): # 新挂单时候的排队 排在最后一个
    bids = [tick_now['bid' + str(x)] for x in range(1, 6)]
    asks = [tick_now['ask' + str(x)] for x in range(1, 6)]
    if quote_price in bids:
        n = np.sum(quote_price <= np.array(bids))
        return tick_now['bsize' + str(n)]
    elif quote_price in asks:
        n = np.sum(quote_price >= np.array(asks))
        return tick_now['asize' + str(n)]        
    else:
        return 1
    
    
   
def update_queue(orders_list, tick_now): # 如果修改了dict每一个引用都会改变
    # 一定是不成交才需要update queue
    if len(orders_list) == 0:
        return
    bids = [tick_now['bid' + str(x)] for x in range(1, 6)]
    asks = [tick_now['ask' + str(x)] for x in range(1, 6)]
    last = tick_now['last']
    volume = tick_now['volume']
    orders_canceled = [] 
    for i in range(len(orders_list)):
        order = orders_list[i]
        quote_price = order['price']
        myqueue = order['myqueue']
        totalqueue = order['totalqueue']
        trade_direc = order['B/S']
      
        if trade_direc == 1 and quote_price in bids:        
            n = np.sum(quote_price <= np.array(bids))
            newsize = tick_now['bsize' + str(n)]
            if newsize > myqueue:
                 order['totalqueue'] = newsize
            else:
                 order['myqueue'] = math.floor(newsize * myqueue / totalqueue)
                 order['totalqueue'] = newsize
                
        elif trade_direc == -1 and quote_price in asks:
            n = np.sum(quote_price >= np.array(asks))
            newsize = tick_now['asize' + str(n)]   
           
        elif (trade_direc == 1 and quote_price < tick_now['ask1']) or (trade_direc == -1 and quote_price > tick_now['bid1']): 
            order['myqueue'] = 1
            order['totalqueue'] = 1
        else:
            orders_canceled.append(i)
    for counter, index in enumerate(orders_canceled):
        index = index - counter
        orders_list.pop(index)



def get_last_size(last, volume, tick_last): # 在last价格的成交量

    if last >= tick_last['ask1']:
        count = np.sum([tick_last['ask' + str(x)] < last for x in range(1, 6)])
        total_size = np.sum([tick_last['asize' + str(x)] for x in range(1, count + 1)])
        return np.max(volume - total_size, 0)
    elif last <= tick_last['bid1']:
        count = np.sum([tick_last['bid' + str(x)] > last for x in range(1, 6)])
        total_size = np.sum([tick_last['bsize' + str(x)] for x in range(1, count + 1)])
        return np.max(volume - total_size, 0)
    else: # 在盘口之间成交
        return volume
    
def match_orders(orders_list, tick_now, tick_last, trade_report, pos): # return position 判断是否成交 若成交放入成交回报并从orders_list删除
    t = tick_now['time']
    volume = tick_now['volume']        
    if volume > 0 and len(orders_list) != 0:
        orders_filled = []
        for i in range(len(orders_list)):
            order = orders_list[i]
            quote_price = order['price']
            myqueue = order['myqueue']
            totalqueue = order['totalqueue']
            trade_direc = order['B/S']           
            last = tick_now['last']
            
            if trade_direc == 1: # 判断买单
                if last < quote_price or tick_now['ask1'] <= quote_price:
                    # 成交后从orders_list删除，记下位置后一并删除 del
                    m = {'time': t, 'type': order['type'], 'trade': order['size'], 'price': quote_price, 'myqueue': myqueue, 'totalqueue': totalqueue}
                    pos += m['trade']
                    orders_filled.append(i)
                    trade_report.append(m)
                elif last == quote_price:
                    last_volume = get_last_size(last, volume, tick_last)
                    if myqueue <= last_volume:
                        # 成交
                        m = {'time': t, 'type': order['type'], 'trade': order['size'], 'price': quote_price, 'myqueue': myqueue, 'totalqueue': totalqueue}
                        pos += m['trade']
                        orders_filled.append(i)
                        trade_report.append(m)
            else: # 判断卖单
                if last > quote_price or tick_now['bid1'] >= quote_price:
                    # 成交后从orders_list删除，记下位置后一并删除 del
                    m = {'time': t, 'type': order['type'], 'trade': -order['size'], 'price': quote_price, 'myqueue': myqueue, 'totalqueue': totalqueue}
                    pos += m['trade']
                    orders_filled.append(i)
                    trade_report.append(m)
                elif last == quote_price:
                    last_volume = get_last_size(last, volume, tick_last)
                    if myqueue <= last_volume:
                        # 成交
                        m = {'time': t, 'type': order['type'], 'trade': -order['size'], 'price': quote_price, 'myqueue': myqueue, 'totalqueue': totalqueue}
                        pos += m['trade']
                        orders_filled.append(i)
                        trade_report.append(m)
        # 从orders_list删除已经成交的订单
        for counter, index in enumerate(orders_filled):
            index = index - counter
            orders_list.pop(index)
    # 剩下没有成交的orders update queue
    update_queue(orders_list, tick_now)
        
    return pos
    



if __name__ == '__main__':
    #initiate参数
    params_setup = {
        'date_T0':"20220214",# 前一交易日
        'date_T': "20220215", # 今天的日期  
        'exchange_code':"SHF",
        'product_code':"AU",
        'contract':"AU2206",
        'wind_flag':1
        }
    #交易参数
    params_trade = {
        'contract_multiplier':1000,
        'contract_tick':0.02,
        #理论价计算
        'volumeratio':99999,
        'skew_pos':99999,
        'skew':1,
        'IOC_selective':1,
        'IOC_indicator':1,
        'IOC_price':-1,
        'spread_type':2,#1:user defined #2 mkt spread+adj #3 根据净持仓调整宽度
        'spread':5,
        'spread_adj':4,
        'quote_size':1, # 不变
        'alone_order':5,
        'hedge_target':5,
        'hedge_requote':2,
        'quote_withdraw_indicator':0
        }   
    
    params_mm_queue = {
        'contract_multiplier':1000,
        'contract_tick':0.02,
        'volumeratio':2,
        'queue_pos':0.3,
        'last_indicator':1,
        'pos_limit':5,
        'balance_limit':5
        
        }
    #Get Market Data
    df_mkt = getdata.load_data(params_setup['contract'], params_setup['exchange_code'], params_setup['product_code'], params_setup['wind_flag'], params_setup['date_T0'], params_setup['date_T'])
    df_mkt.rename(columns = {'index': 'time'}, inplace = True)
    df_mkt['date'] = df_mkt['time'].map(lambda x: str(x)[:11])
    trade_report = mm_trade(df_mkt, params_trade)
    
    trade_report = pd.DataFrame(trade_report)
    # result = PostTradingAnalysis(params_trade,params_setup,df_mkt,trade_report)

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    