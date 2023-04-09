# -*- coding: utf-8 -*-
"""
Created on Fri Nov 18 15:10:02 2022

@author: akhil
"""

from smartapi import SmartConnect
import pandas as pd
import numpy as np
from time import sleep
import datetime as dt
import traceback


def get_token_from_symbol(symbol, df):
    return int(df[df['symbol']==symbol].iloc[0]['token']), str(df[df['symbol']==symbol]['fyers_sym'].iloc[0])

def get_open_positions(client):
    positions = client.position()['data']
    open_pos = []
    if positions is not None:
        for pos in positions:
            if abs(int(pos['sellqty'])-int(pos['buyqty'])) != 0 and pos['producttype']=='INTRADAY':
                open_pos.append(pos['tradingsymbol'])
    
    return open_pos 


def opposite(trans):
    if trans.upper() == 'BUY':
        return 'SELL'
    elif trans.upper() == 'SELL':
        return 'BUY'


def position_from_orders(orders):
    if orders['data'] is None:
        raise Exception(f"Orders is None. {orders}")
    
    all_ords = orders['data']
    intraday_ords = [i for i in all_ords if i['producttype']=='INTRADAY']
    
    ords_df = pd.DataFrame(intraday_ords)
    ords_df = ords_df[ords_df['status']=='complete']
    ords_df['exchtime'] = ords_df['exchtime'].apply(lambda x: dt.datetime.strptime(x, "%d-%b-%Y %H:%M:%S"))
    ords_df.set_index('exchtime', inplace=True)
    ords_df.sort_index(inplace=True)
    
    pos = []
    transactions = {}
    for i in range(len(ords_df)):
        # print(pos)
        if ords_df['tradingsymbol'].iloc[i] not in pos and ords_df['tradingsymbol'].iloc[i] not in transactions:
            pos.append(ords_df['tradingsymbol'].iloc[i])
            transactions[ords_df['tradingsymbol'].iloc[i]] = [ords_df['transactiontype'].iloc[i], int(ords_df['filledshares'].iloc[i])]
        else:
            trans = opposite(transactions[ords_df['tradingsymbol'].iloc[i]][0])
            if ords_df['transactiontype'].iloc[i] == trans:
                if abs(int(ords_df['filledshares'].iloc[i])) == abs(transactions[ords_df['tradingsymbol'].iloc[i]][1]):
                    pos.remove(ords_df['tradingsymbol'].iloc[i])
                    del transactions[ords_df['tradingsymbol'].iloc[i]]
                
                else:
                    transactions[ords_df['tradingsymbol'].iloc[i]][1] = transactions[ords_df['tradingsymbol'].iloc[i]][1]-int(ords_df['filledshares'].iloc[i])
            
            else:
                if ords_df['tradingsymbol'].iloc[i] in transactions:
                    transactions[ords_df['tradingsymbol'].iloc[i]][1] = transactions[ords_df['tradingsymbol'].iloc[i]][1]+int(ords_df['filledshares'].iloc[i])
    
    return pos
        

def pos_list_from_orders(orders):
    if orders['data'] is None:
        raise Exception(f"Orders is None. {orders}")
    
    all_ords = orders['data']
    intraday_ords = [i for i in all_ords if i['producttype']=='INTRADAY']
    
    ords_df = pd.DataFrame(intraday_ords)
    ords_df = ords_df[ords_df['status']=='complete']
    ords_df['exchtime'] = ords_df['exchtime'].apply(lambda x: dt.datetime.strptime(x, "%d-%b-%Y %H:%M:%S"))
    ords_df.set_index('exchtime', inplace=True)
    ords_df.sort_index(inplace=True)

    pos = {}
    for i in range(len(ords_df)):
        # print(pos)
        if ords_df['tradingsymbol'].iloc[i] not in list(pos.keys()):
            if ords_df['transactiontype'].iloc[i].upper() == 'SELL':
                temp_dict = {
                                'tradingsymbol' : ords_df['tradingsymbol'].iloc[i],
                                'symboltoken' : ords_df['symboltoken'].iloc[i],
                                'producttype' : ords_df['producttype'].iloc[i],
                                'sellqty' : int(ords_df['filledshares'].iloc[i]),
                                'buyqty' : int(0)     
                            }
            elif ords_df['transactiontype'].iloc[i].upper() == 'BUY':
                temp_dict = {
                                'tradingsymbol' : ords_df['tradingsymbol'].iloc[i],
                                'symboltoken' : ords_df['symboltoken'].iloc[i],
                                'producttype' : ords_df['producttype'].iloc[i],
                                'sellqty' : int(0),
                                'buyqty' : int(ords_df['filledshares'].iloc[i])
                            }            
            pos[ords_df['tradingsymbol'].iloc[i]] = temp_dict
        else:
            if ords_df['transactiontype'].iloc[i].upper() == 'SELL':
                pos[ords_df['tradingsymbol'].iloc[i]]['sellqty']+=abs(int(ords_df['filledshares'].iloc[i]))
            elif ords_df['transactiontype'].iloc[i].upper() == 'BUY':
                pos[ords_df['tradingsymbol'].iloc[i]]['buyqty']+=abs(int(ords_df['filledshares'].iloc[i]))
    
    return_pos = []
    for _,p in pos.items():
        return_pos.append(p)
    return return_pos


# pos = pos_list_from_orders(orders)
            


def get_sl_order(client, ce_symbol, pe_symbol):
    print(ce_symbol,'     ',pe_symbol)
    try:
        orders = client.orderBook()['data']
    except:
        sleep(1.2)
        print(traceback.format_exc())
        orders = client.orderBook()['data']
    if orders is not None:
        # print(orders)
        sleep(1.1)
        positions = get_open_positions(client)
        print(positions)
        for order in orders:
            if ce_symbol in positions and order['tradingsymbol'] == ce_symbol and order['ordertype'] == 'STOPLOSS_LIMIT'\
                and order['orderstatus'] == 'trigger pending' and order['producttype'] == 'INTRADAY':
                    print('ce_sl_order')
                    return order['orderid']
            
            elif pe_symbol in positions and order['tradingsymbol'] == pe_symbol and order['ordertype'] == 'STOPLOSS_LIMIT'\
                and order['orderstatus'] == 'trigger pending' and order['producttype'] == 'INTRADAY':
                    print('pe_sl_order')
                    return order['orderid']
        
        return None
    return None


def get_ce_pe_sl(client, ce_symbol, pe_symbol):
    print(ce_symbol,'     ',pe_symbol)
    try:
        orders = client.orderBook()['data']
    except:
        sleep(1.2)
        print(traceback.format_exc())
        orders = client.orderBook()['data']
    ce_sl_ord = None
    pe_sl_ord = None
    ce_sl = False
    pe_sl = False
    if orders is not None:
        sleep(1.1)
        positions = get_open_positions(client)
        print(positions)
        for order in orders:
            if ce_symbol in positions and order['tradingsymbol'] == ce_symbol and order['ordertype'] == 'STOPLOSS_LIMIT'\
                and order['orderstatus'] == 'trigger pending' and order['producttype'] == 'INTRADAY' and\
                    not ce_sl:
                    ce_sl_ord = order['orderid']
                    ce_sl = True
                    
            if pe_symbol in positions and order['tradingsymbol'] == pe_symbol and order['ordertype'] == 'STOPLOSS_LIMIT'\
                and order['orderstatus'] == 'trigger pending' and order['producttype'] == 'INTRADAY' and\
                    not pe_sl:
                    pe_sl_ord = order['orderid']  
                    pe_sl = True
            
            if pe_sl and ce_sl:
                break
        
        for order in orders:
            if ce_sl_ord is not None and pe_sl_ord is not None:
                break
            if ce_sl_ord is None:
                if ce_symbol not in positions and order['tradingsymbol'] == ce_symbol and order['ordertype'] == 'STOPLOSS_LIMIT'\
                                and order['orderstatus'] == 'complete' and order['producttype'] == 'INTRADAY':
                    ce_sl_ord = order['orderid']
            
            if pe_sl_ord is None:
                if pe_symbol not in positions and order['tradingsymbol'] == pe_symbol and order['ordertype'] == 'STOPLOSS_LIMIT'\
                    and order['orderstatus'] == 'complete' and order['producttype'] == 'INTRADAY':
                        pe_sl_ord = order['orderid']
                    
    
    return ce_sl_ord, pe_sl_ord


# ce_symbol = ins['short']['CE']['symbol']
def get_entry_prices(client, ce_symbol, pe_symbol):
    try:
        orders = client.orderBook()['data']
    except:
        sleep(1.2)
        print(traceback.format_exc())
        orders = client.orderBook()['data']        
    if orders is not None:
        ce_ords = [o for o in orders if o['tradingsymbol']==ce_symbol and o['transactiontype']=='SELL']
        pe_ords = [o for o in orders if o['tradingsymbol']==pe_symbol and o['transactiontype']=='SELL']
        
        ce_ords_df = pd.DataFrame(data=ce_ords)
        ce_ords_df = ce_ords_df[ce_ords_df['exchorderupdatetime']!='']
        #print(ce_ords_df.head())
        ce_ords_df['exchorderupdatetime'] = ce_ords_df['exchorderupdatetime'].apply(lambda x: dt.datetime.strptime(x, '%d-%b-%Y %H:%M:%S'))
        ce_ords_df = ce_ords_df.sort_values(by='exchorderupdatetime', ascending=False)
        ce_price = ce_ords_df['averageprice'].iloc[0]
        
        pe_ords_df = pd.DataFrame(data=pe_ords)
        pe_ords_df = pe_ords_df[pe_ords_df['exchorderupdatetime']!='']
        #print(pe_ords_df.head())
        pe_ords_df['exchorderupdatetime'] = pe_ords_df['exchorderupdatetime'].apply(lambda x: dt.datetime.strptime(x, '%d-%b-%Y %H:%M:%S'))
        pe_ords_df = pe_ords_df.sort_values(by='exchorderupdatetime', ascending=False)
        pe_price = pe_ords_df['averageprice'].iloc[0]
        
        return ce_price, pe_price
    
    return 0,0


def position_rows(clients, short_ce, short_pe, long_ce,\
                        long_pe, lot_size, is_exit):
    
    dirs = {short_ce:'SELL', short_pe:'SELL', long_ce:'BUY', long_pe:'BUY'}
    rows = []
    sleep(1.5)
    for i in range(len(clients)):
        client = clients[i]
        # try:

        # Get positions for the client and extract open qty
        # m2m = 0
        short_ce_qty = 0
        short_pe_qty = 0
        long_ce_qty = 0
        long_pe_qty = 0
        
        # sleep(1.5)
        try:
            positions = client['client'].position()['data']
        except Exception as e:
            print(str(e))
            sleep(1.1)
            try:
                ord_data = client['client'].orderBook()
            except:
                sleep(1.1)
                print(traceback.format_exc())
                ord_data = client['client'].orderBook()
            positions = pos_list_from_orders(ord_data)
        if positions is not None:
            for pos in positions:
                # m2m += pos['m2m']
                if pos['tradingsymbol'] == short_ce and pos['producttype'] == 'INTRADAY':
                    short_ce_qty += (int(pos['sellqty'])-int(pos['buyqty']))
                if pos['tradingsymbol'] == short_pe and pos['producttype'] == 'INTRADAY':
                    short_pe_qty += (int(pos['sellqty'])-int(pos['buyqty']))
                if pos['tradingsymbol'] == long_ce and pos['producttype'] == 'INTRADAY':
                    long_ce_qty += (int(pos['buyqty'])-int(pos['sellqty']))
                if pos['tradingsymbol'] == long_pe and pos['producttype'] == 'INTRADAY':
                    long_pe_qty += (int(pos['buyqty'])-int(pos['sellqty']))
            short_ord_qty_ce = short_ord_qty_pe = long_ord_qty_ce = long_ord_qty_pe = 0
            
            # sleep(1.2)
            # orders = client["client"].orderBook()['data']
            
            # for order in orders:
            #     # Match based on the symbol and product
            #     if order['tradingsymbol'] in dirs and order['producttype'] == 'INTRADAY':
            #         # Then match based on trade direction
            #         if dirs[order['tradingsymbol']] == order['transactiontype']:
            #             # If we don't know the priority of a status, set overall
            #             # status to unknown and break out of the loop
            #             if order['status'] == 'trigger pending' or order['status'] == 'open':
            #                 dir_mult = 1 if order['transactiontype'] == 'BUY' else -1
            #                 if order['tradingsymbol'] == short_ce:
            #                     short_ord_qty_ce += int(order['unfilledshares'])*dir_mult
            #                 elif order['tradingsymbol'] == short_pe:
            #                     short_ord_qty_pe += int(order['unfilledshares'])*dir_mult
            #                 elif order['tradingsymbol'] == long_ce:
            #                     long_ord_qty_ce += int(order['unfilledshares'])*dir_mult  
            #                 elif order['tradingsymbol'] == long_pe:
            #                     long_ord_qty_pe += int(order['unfilledshares'])*dir_mult                           
            #                 else:
            #                     raise RuntimeError('this should be impossible')  
    
            # If we're exiting or the client is not enabled, expected qty is 0
            expected_qty = int(client["lots"]*lot_size)
            
            if is_exit:
                expected_qty = 0

            rows.append({
                # 'Client Name' : client.client_name,
                'No'   : (i),
                'Client'   : client['user'],
                'Required Qty' :expected_qty,
                'short_ce' : int(expected_qty - abs(int(short_ce_qty)) - abs(int(short_ord_qty_ce))),
                'short_pe' : int(expected_qty - abs(int(short_pe_qty)) - abs(int(short_ord_qty_pe))),
                'long_ce'  : int(expected_qty - abs(int(long_ce_qty)) - abs(int(long_ord_qty_ce))),
                'long_pe'  :int(expected_qty - abs(int(long_pe_qty)) - abs(int(long_ord_qty_pe)))
            })
            
            # except Exception as e:
            #     print(f'Unable to generate positon alert for {client["Client"].user_id}: {e}')
    
    return rows



