# -*- coding: utf-8 -*-
"""
Created on Wed Oct  5 13:55:32 2022

@author: akhil
"""


txt_file = open('init_pause.txt', 'w')
txt_file.write('1')
txt_file.close()

pause_txt = open('pause.txt', 'w')
pause_txt.write('0')
pause_txt.close()


import requests
import datetime as dt
import math
import re
from angel_init import ltp_authorize
import numpy as np
import pandas as pd
import time
from time import sleep
from threading import Thread
import logging
import gspread
import sys
import os
from functions import get_token_from_symbol, get_sl_order, get_open_positions
from straddle_telegram_alerts import queue_position_alerts, queue_sl_alert, generate_position_alert
import random
from functions import position_rows, get_entry_prices, get_ce_pe_sl
import traceback
from fyers_account import fyersAPI
import threading



cred = 'credentials.json'

gc = gspread.service_account(filename=cred)

sh = gc.open_by_url('')
sheet2 = sh.get_worksheet(0)
sheet3 = sh.get_worksheet(2)
client_sheet = sh.get_worksheet(3)

df = pd.DataFrame(data=sheet2.get_all_records())

constant_sheet = pd.DataFrame(data=sheet3.get_all_records())

# try:
# row = 0
row = int(sys.argv[1])
PATH = str(sys.argv[2])
# except Exception as e:
    # logging.info(str(e))
# logging.info(f'{row}')

# os._exit(1)

name = str(df['Instrument'].iloc[row])
short_premium = float(df['Short Preimium'].iloc[row])
long_premium = float(df['Long Premium'].iloc[row])
STOPLOSS = float(df['SL Percentage'].iloc[row])
ce_reentry = int(df['CE Reentries'].iloc[row])
pe_reentry = int(df['PE Reentries'].iloc[row])
sheet_entry = dt.datetime.strptime(str(df['Entry_Time'].iloc[row]), '%H:%M').time()
entry_time = dt.datetime.combine(dt.datetime.now().date(), sheet_entry)
sheet_exit = dt.datetime.strptime(str(df['Exit_Time'].iloc[row]), '%H:%M').time()
exit_time = dt.datetime.combine(dt.datetime.now().date(), sheet_exit)
# quantity = 1

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

LOG_FILE = f"log/{str(dt.datetime.now().date())+' '+name}.log"
logging.basicConfig(
	format='%(asctime)s:%(threadName)s:(%(levelname)s): %(message)s',
	datefmt='%d-%b-%y %H:%M:%S',
	level=logging.INFO,
	handlers=[logging.FileHandler(LOG_FILE, 'a'), logging.StreamHandler()]
)

logging.info("Started")

logging.info(f'{name}\n{short_premium}\n{long_premium}\n{STOPLOSS}\n{ce_reentry}\n{pe_reentry}\n{entry_time}\n{exit_time}')


stop_thread = False

# os._exit(1)
# angel = ltp_authorize.ltp_login()

# if dt.datetime.now().date() >= dt.date(2022, 12, 30):
#     os.remove('main.py')
#     os.remove('straddle_combined_sl.py')
#     os._exit(1)


if int(df['Exit_Stop'].iloc[row]) == 1:
    logging.info(f'{name} is not set to run today')
    txt_file = open('init_pause.txt', 'w')
    txt_file.write('0')
    txt_file.close()     
    os._exit(1)

client_df = pd.DataFrame(client_sheet.get_all_records())


# client_refresh.loc['KITV1001']

clients = []
for i in range(len(client_df)):
    ser = client_df.iloc[i]
    symbols = ser['symbol'].split(', ')
    if name in symbols:
        try:
            if dt.datetime.strptime(client_df['last_login'].iloc[i], '%d-%m-%Y').date() != dt.datetime.now().date():
                temp_client = ltp_authorize.ltp_login(ser)
                if temp_client.getProfile(temp_client.refresh_token)['status']:
                    print(f"{ser['username']} logged in")
                    clients.append({'client':temp_client, 'lots':int(client_df['lots'].iloc[i]),\
                                    'pe_reentry':0, 'ce_reentry':0, 'pe_entry_price':0, 'ce_entry_price':0, 'user':str(ser['username'])})
                    indexes = list(client_df[client_df['username']==ser['username']].index)
                    for i in indexes:
                        client_sheet.update_cell(int(i)+2, 9, temp_client.access_token)
                        client_sheet.update_cell(int(i)+2, 10, dt.datetime.now().strftime('%d-%m-%Y'))
                        
                else:
                    print(f"Error logging in {ser['username']}")
                    logging.info(f"Error logging in {ser['username']}\n{temp_client.getProfile(temp_client.refresh_token)['status']}")                        
                
            else:
                print(f'Logging in {ser["username"]} using access_token')
                temp_client = ltp_authorize.login_with_access(ser)
                if temp_client.getProfile(temp_client.refresh_token)['status']:
                    print(f"{ser['username']} logged in")
                    clients.append({'client':temp_client, 'lots':int(client_df['lots'].iloc[i]),\
                                    'pe_reentry':0, 'ce_reentry':0, 'pe_entry_price':0, 'ce_entry_price':0, 'user':str(ser['username'])})
                    indexes = list(client_df[client_df['username']==ser['username']].index)
                    
                else:
                    print(f"Error logging in {ser['username']}")
                    logging.info(f"Error logging in {ser['username']}\n{temp_client.getProfile(temp_client.refresh_token)['status']}")                      
                
        except:
            print(f"Error logging in {ser['username']}")
            continue


# clients[0]['client'].access_token

        
if len(clients) == 0:
    print('No Accounts for',name)
    txt_file = open('init_pause.txt', 'w')
    txt_file.write('0')
    txt_file.close()    
    os._exit(1)
    
angel = clients[random.randint(0, (len(clients)-1))]['client']

try:
    fyers = fyersAPI(name, PATH)
    if fyers.client.get_profile()['s'] != 'ok':
        raise Exception('Fyers profile not ok')
except:
    try:
        print('Second time logging into fyers')
        fyers = fyersAPI(name, PATH)
        if fyers.client.get_profile()['s'] != 'ok':
            raise Exception('Fyers profile not ok')
    except Exception as e:
        print(f'Error logging in to fyers account : {str(e)}')

ins = {}


def get_angel_instruments():
    link = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
    angel_csv = requests.get(link)
    json_file = angel_csv.json()
    csv_list = []
    for i in json_file:
        if i['exch_seg'] == 'NFO' and (re.match('NIFTY.*', i['symbol']) or re.match('BANKNIFTY.*',i['symbol']) or\
                            (re.match(name.upper()+'.*', i['symbol'])))\
            and i['instrumenttype']=='OPTIDX':
            temp = i.copy()
            temp['option'] = temp['symbol'][-2:]
            csv_list.append(temp)
    for i in range(len(csv_list)):
        csv_list[i]['strike'] = int(float(csv_list[i]['strike'])/100)
        csv_list[i]['expiry'] = dt.datetime.strptime(csv_list[i]['expiry'], '%d%b%Y').date()
    
    csv_final = []
    for i in range(len(csv_list)):
        if csv_list[i]['expiry'] < dt.datetime.now().date():
            continue
        else:
            csv_final.append(csv_list[i])
            
    
    csv_df = pd.DataFrame(csv_final)
    nearest_exp = csv_df.sort_values(by='expiry')['expiry'].iloc[2]
    csv_df = csv_df[(csv_df['expiry']==nearest_exp) & (csv_df['name']==name.upper())]
    csv_df.reset_index(inplace=True, drop=True)
    
    index = {}
    for i in json_file:
        if i['exch_seg'] == 'NSE' and i['name'] == name:
            index['symbol'] = i['symbol']
            index['token'] = i['token']
    
    return csv_df, index



def get_ltp(symbol):
    global fyers
    return fyers.single_ltp(symbol)

def angel_ltp(client, symbol, token):
    return round(float(client.ltpData('NFO', symbol, token)['data']['ltp']), 1)

def closest_value(df, input_value):

  i = (np.abs(df['ltp'] - input_value)).argmin()
  return df.iloc[i]

def add_ltp(DF):
    global name, fyers
    
    df = DF.copy()
    
    symbols = df['fyers_sym'].tolist()
    ltp_data = fyers.get_ltps(symbols)
    df['ltp'] = float(0)
    for i in range(len(df)):
        print(i)
        df['ltp'].iloc[i] = ltp_data[df['fyers_sym'].iloc[i]]
            
    return df

def get_order(client, orders, ord_id):
    # orders = clients[i]['client'].orderBook()

    for o in orders['data']:
        if o['orderid'] == str(ord_id):
            return o
    return None

def get_order_call_orders(client, ord_id):
    orders = client.orderBook()

    for o in orders['data']:
        if o['orderid'] == str(ord_id):
            return o
    return None

def get_sl_orders(client, pe_ord_id, ce_ord_id):
    orders = client.orderBook()
    pe_order = None
    ce_order = None
    for o in orders['data']:
        if o['orderid'] == str(pe_ord_id):
            pe_order = o
        if o['orderid'] == str(ce_ord_id):
            ce_order = o
    return pe_order, ce_order

def get_open_positions(client):
    positions = client.position()['data']
    open_pos = []
    for pos in positions:
        if abs(int(pos['sellqty'])-int(pos['buyqty'])) != 0 and pos['producttype']=='INTRADAY':
            open_pos.append(pos['tradingsymbol'])
    
    return open_pos

# pos=angel.position()

def place_buy_limit(client, symbol, token, qty, price):
    try:
        orderparams = {
            "variety": "NORMAL",
            "tradingsymbol": str(symbol),
            "symboltoken": str(token),
            "transactiontype": "BUY",
            "exchange": "NFO",
            "ordertype": "LIMIT",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "price": str(price),
            "squareoff": "0",
            "stoploss": "0",
            "quantity": str(qty)
            }
        orderId=client.placeOrder(orderparams)
        print(orderId)        
        return orderId
    except Exception as e:
        raise Exception("Order placement failed: {}".format(e.message))        

def place_sell_limit(client, symbol, token, qty, price):
    try:
        if float(price) <= 1:
            price = 1.0        
        orderparams = {
            "variety": "NORMAL",
            "tradingsymbol": str(symbol),
            "symboltoken": str(token),
            "transactiontype": "SELL",
            "exchange": "NFO",
            "ordertype": "LIMIT",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "price": str(price),
            "squareoff": "0",
            "stoploss": "0",
            "quantity": str(qty)
            }
        orderId=client.placeOrder(orderparams)
        print(orderId)
        # print("The order id is: {}".format(orderId))
        return orderId
    except Exception as e:
        raise Exception("Order placement failed: {}".format(e.message)) 

def place_order(client, symbol, token, qty, trans, price):
    if trans.upper() == 'SELL':
        try:
            orderparams = {
                "variety": "NORMAL",
                "tradingsymbol": str(symbol),
                "symboltoken": str(token),
                "transactiontype": "SELL",
                "exchange": "NFO",
                "ordertype": "MARKET",
                "producttype": "INTRADAY",
                "duration": "DAY",
                "price": str(price),
                "squareoff": "0",
                "stoploss": "0",
                "quantity": str(qty)
                }
            orderId=client.placeOrder(orderparams)
            print(orderId)
            # print("The order id is: {}".format(orderId))
            return orderId
        except Exception as e:
            raise Exception("Order placement failed: {}".format(e.message))      

    elif trans.upper() == "BUY":
        try:
            orderparams = {
                "variety": "NORMAL",
                "tradingsymbol": str(symbol),
                "symboltoken": str(token),
                "transactiontype": "BUY",
                "exchange": "NFO",
                "ordertype": "MARKET",
                "producttype": "INTRADAY",
                "duration": "DAY",
                "price": str(price),
                "squareoff": "0",
                "stoploss": "0",
                "quantity": str(qty)
                }
            orderId=client.placeOrder(orderparams)
            # print("The order id is: {}".format(orderId))
            return orderId
        except Exception as e:
            raise Exception("Order placement failed: {}".format(e.message))
            
def place_stoploss(client, symbol, token, qty, sl_price):
        qty = int(qty)
        sl_price = round(float(sl_price), 1)
        
        try:
            orderparams = {
                "variety": "STOPLOSS",
                "tradingsymbol": str(symbol),
                "symboltoken": str(token),
                "transactiontype": "BUY",
                "exchange": "NFO",
                "ordertype": "STOPLOSS_LIMIT",
                "producttype": "INTRADAY",
                "duration": "DAY",
                "price": str(sl_price),
                "triggerprice": str(sl_price-0.1),
                "squareoff": "0",
                "stoploss": "0",
                "quantity": str(qty)
                } 
            orderId=client.placeOrder(orderparams)
            # print("The order id is: {}".format(orderId))
            return orderId
        except Exception as e:
            raise Exception("Order placement failed: {}".format(e.message))


def modify_sl_order(client, symbol, token, order_id, qty, sl_price):
    qty = abs(int(qty))
    sl_price = round(float(sl_price), 1)
    
    try:
        orderparams = {
            "variety":"STOPLOSS",
            "orderid":str(order_id),
            "ordertype":"STOPLOSS_LIMIT",
            "producttype":"INTRADAY",
            "duration":"DAY",
            "price":str(sl_price),
            "triggerprice": str(sl_price-0.1),
            "quantity":str(qty),
            "tradingsymbol":str(symbol),
            "symboltoken":str(token),
            "exchange":"NFO"
        }
        print(orderparams)
        orderId = client.modifyOrder(orderparams)
        print(orderId)
        return orderId
    except Exception as e:
        raise Exception("Order placement failed: {}".format(e.message))    


def init_order_thread(client, instruments, qty, i):
    global clients, STOPLOSS
    # client = clients[i]['client']
    # orders = None
    sleep(2)
    positions = get_open_positions(client)    
    try:
        if instruments['long']['CE']['symbol'] not in positions and instruments['long']['PE']['symbol'] not in positions:
            clients[i]['init_entry'] = True
            clients[i]['orders'] = {}
            
            ce_long_price = angel_ltp(client, instruments['long']['CE']['symbol'], instruments['long']['CE']['token'])+4
            pe_long_price = angel_ltp(client, instruments['long']['PE']['symbol'], instruments['long']['PE']['token'])+4
                
            try:
                ce_long = place_buy_limit(client, instruments['long']['CE']['symbol'],\
                                       instruments['long']['CE']['token'], qty, ce_long_price)
                clients[i]['orders']['ce_long'] = ce_long
                print(f'{clients[i]["user"]} -- ce_long -- {ce_long}')
            except Exception as e:
                logging.info(f'{clients[i]["user"]} --  ce_long  -- {str(e)}')  
            sleep(1.5)
            
            try:
                pe_long = place_buy_limit(client, instruments['long']['PE']['symbol'],\
                                       instruments['long']['PE']['token'], qty, pe_long_price)
                clients[i]['orders']['pe_long'] = pe_long
                print(f'{clients[i]["user"]} -- pe_long -- {pe_long}')
            except Exception as e:
                logging.info(f'{clients[i]["user"]} -- pe_long  -- {str(e)}')  
            sleep(1.5)
                
            ce_short_price = angel_ltp(client, instruments['short']['CE']['symbol'], instruments['short']['CE']['token'])-4
            pe_short_price = angel_ltp(client, instruments['short']['PE']['symbol'], instruments['short']['PE']['token'])-4
            
            if ce_short_price <= 0:
                ce_short_price = 0.5
            if pe_short_price <= 0:
                pe_short_price = 0.5
            
            try:
                ce_short = place_sell_limit(client, instruments['short']['CE']['symbol'],\
                                       instruments['short']['CE']['token'], qty, ce_short_price)
                clients[i]['orders']['ce_short'] = ce_short
                print(f'{clients[i]["user"]} -- ce_short -- {ce_short}')
            except Exception as e:
                logging.info(f'{clients[i]["user"]} --  ce_short  -- {str(e)}')                  
            sleep(1.5)
            
            try:
                pe_short = place_sell_limit(client, instruments['short']['PE']['symbol'],\
                                       instruments['short']['PE']['token'], qty, pe_short_price)
            
                clients[i]['orders']['pe_short'] = pe_short
                print(f'{clients[i]["user"]} -- pe_short -- {pe_short}')
            except Exception as e:
                logging.info(f'{clients[i]["user"]} --  pe_short  -- {str(e)}')                
            sleep(1.5)
            
            ord_book = client.orderBook()
            
            try:
                ce_ord = get_order(client, ord_book, ce_short)
                if ce_ord is not None:
                    ce_price = round(float(ce_ord['averageprice']), 1)
                    if ce_price != 0:
                        clients[i]['ce_entry_price'] = ce_price
                        ce_sl_price = round(((1+STOPLOSS)*float(ce_price)), 1)
                    else:
                        print(f'{clients[i]["user"]} -- ce_price = 0')
                else:
                    print(f'{clients[i]["user"]} -- ce_ord = None')                       
            except Exception as e:
                logging.info(f'{clients[i]["user"]} ---- {str(e)}')

            try:
                pe_ord = get_order(client, ord_book, pe_short)
                if pe_ord is not None:
                    pe_price = round(float(pe_ord['averageprice']), 1)
                    if pe_price != 0:
                        clients[i]['pe_entry_price'] = pe_price
                        pe_sl_price = round(((1+STOPLOSS)*float(pe_price)), 1)
                    else:
                        print(f'{clients[i]["user"]} -- pe_price = 0')
                else:
                    print(f'{clients[i]["user"]} -- pe_ord = None')
            except Exception as e:
                logging.info(f'{clients[i]["user"]} ---- {str(e)}')  
            
            if ce_sl_price != 0:
                try:
                    ce_sl = place_stoploss(client, instruments['short']['CE']['symbol'],\
                                           instruments['short']['CE']['token'], qty, ce_sl_price)
                    clients[i]['orders']['ce_sl'] = ce_sl
                    print(f'{clients[i]["user"]} -- ce_sl -- {ce_sl}')
                except Exception as e:
                    logging.info(f'{clients[i]["user"]} --  ce_sl  -- {str(e)}')
                    
            if pe_sl_price != 0:
                try:
                    pe_sl = place_stoploss(client, instruments['short']['PE']['symbol'],\
                                       instruments['short']['PE']['token'], qty, pe_sl_price)
                    clients[i]['orders']['pe_sl'] = pe_sl
                    print(f'{clients[i]["user"]} -- pe_sl -- {pe_sl}')
                except Exception as e:
                    logging.info(f'{clients[i]["user"]} --  pe_sl  -- {str(e)}')
            
                # clients[i]['orders']['pe_short'] = pe_short
                # del clients[i]['pe_sl']
    
        else:
            ce_price, pe_price = get_entry_prices(client, instruments['short']['CE']['symbol'],\
                                    instruments['short']['PE']['symbol'])
            clients[i]['ce_entry_price'] = ce_price
            clients[i]['pe_entry_price'] = pe_price
            print(ce_price, '    ce_pe     ',pe_price)            
    except Exception as e:
        logging.info(f"Error = {str(e)}")
        print(str(e))
        

    # clients[i]['instruments'] = instruments
        

def check_entry(client, instruments, i, qty, lot_size):
    global clients, STOPLOSS
    
    sleep(2)
    open_pos = get_open_positions(client)
    
    s = [ins['long']['CE']['symbol'], ins['long']['PE']['symbol'],\
                    ins['short']['CE']['symbol'], ins['short']['PE']['symbol']]
    
    sleep(1.5)
    
    if 'init_entry' in list(clients[i].keys()):
        if clients[i]['init_entry'] == True:
            count = 0
            while True:
                if count >= 4:
                    break
                print(f'Checking Entries for client {i} : {clients[i]["user"]}')
                
                if instruments['long']['CE']['symbol'] not in open_pos:
                    try:
                        client.cancelOrder(clients[i]['orders']['ce_long'], 'NORMAL')
                    except Exception as e:
                        logging.info(f'check_entry cancel order client_{i}  ----  {str(e)}')
                    
                    ce_long_price = angel_ltp(client, instruments['long']['CE']['symbol'], instruments['long']['CE']['token'])+4
                    # pe_long_price = angel_ltp(instruments['long']['PE']['symbol'], instruments['long']['PE']['token'])+4
                    
                    try:
                        ce_long = place_buy_limit(client, instruments['long']['CE']['symbol'],\
                                               instruments['long']['CE']['token'], int(qty*lot_size), ce_long_price)
                        clients[i]['orders']['ce_long'] = ce_long
                    except Exception as e:
                        logging.info(f'{clients[i]["user"]} --  ce_long  -- {str(e)}')                  
                    
                    # pe_long = place_buy_limit(client, instruments['long']['PE']['symbol'],\
                    #                        instruments['long']['PE']['token'], qty, pe_long_price)
                    
                                    
                    sleep(1)
                
                if instruments['long']['PE']['symbol'] not in open_pos:
                    try:
                        client.cancelOrder(clients[i]['orders']['pe_long'], 'NORMAL')
                    except Exception as e:
                        logging.info(f'check_entry cancel order client_{i}  ----  {str(e)}')
                        
                    pe_long_price = angel_ltp(client, instruments['long']['PE']['symbol'], instruments['long']['PE']['token'])+4
                    
                    try:
                        pe_long = place_buy_limit(client, instruments['long']['PE']['symbol'],\
                                                instruments['long']['PE']['token'], int(qty*lot_size), pe_long_price)
                        clients[i]['orders']['pe_long'] = pe_long
                    except Exception as e:
                        logging.info(f'{clients[i]["user"]} --  pe_long  -- {str(e)}')                
                    sleep(1)
                
                if instruments['short']['CE']['symbol'] not in open_pos:
                    try:
                        client.cancelOrder(clients[i]['orders']['ce_short'], 'NORMAL')
                    except Exception as e:
                        logging.info(f'check_entry cancel order client_{i}  ----  {str(e)}')                
                    
                    ce_short_price = angel_ltp(client, instruments['short']['CE']['symbol'], ins['short']['CE']['token'])-4
                    # pe_short_price = angel_ltp(ins['short']['PE']['symbol'], ins['short']['PE']['token'])-4
                    
                    if ce_short_price <= 0:
                        ce_short_price = 0.5
                    # if pe_short_price <= 0:
                    #     pe_short_price = 0.5
                    try:
                        ce_short = place_sell_limit(client, instruments['short']['CE']['symbol'],\
                                               instruments['short']['CE']['token'], int(qty*lot_size), ce_short_price)
                    
                        clients[i]['orders']['ce_short'] = ce_short
                    except Exception as e:
                        logging.info(f'{clients[i]["user"]} --  ce_short  -- {str(e)}')                  
                    sleep(1)
                    
                    try:
                        ce_ord = get_order_call_orders(client, ce_short)
                        if ce_ord is not None:
                            ce_price = round(float(ce_ord['averageprice']), 1)
                            if ce_price != 0:
                                try:
                                    clients[i]['ce_entry_price'] = ce_price
                                    ce_sl_price = round(((1+STOPLOSS)*float(ce_price)), 1)
                                    ce_sl = place_stoploss(client, instruments['short']['CE']['symbol'],\
                                                           instruments['short']['CE']['token'], qty, ce_sl_price)
                                    clients[i]['orders']['ce_sl'] = ce_sl
                                    print(f'{clients[i]["user"]} -- ce_sl -- {ce_sl}')

                                except Exception as e:
                                    logging.info(f'{clients[i]["user"]} --  ce_sl  -- {str(e)}')                                    
                            else:
                                print(f'{clients[i]["user"]} -- ce_price = 0')
                        else:
                            print(f'{clients[i]["user"]} -- ce_order = None')                                
                    except Exception as e:
                        logging.info(f'{clients[i]["user"]} ---- {str(e)}')                
                    sleep(1)
                
                if instruments['short']['PE']['symbol'] not in open_pos:
                    try:
                        client.cancelOrder(clients[i]['orders']['pe_short'], 'NORMAL')
                    except Exception as e:
                        logging.info(f'check_entry cancel order client_{i}  ----  {str(e)}')       
                    
                    pe_short_price = angel_ltp(client, instruments['short']['PE']['symbol'], ins['short']['PE']['token'])-4
                    if pe_short_price <= 0:
                        pe_short_price = 0.5
                    
                    try:
                        pe_short = place_sell_limit(client, instruments['short']['PE']['symbol'],\
                                               instruments['short']['PE']['token'], int(qty*lot_size), pe_short_price)
                        
                        clients[i]['orders']['pe_short'] = pe_short
                    except Exception as e:
                        logging.info(f'{clients[i]["user"]} --  pe_short  -- {str(e)}')                    
                    sleep(1)
                    
                    try:
                        pe_ord = get_order_call_orders(client, pe_short)
                        if pe_ord is not None:
                            pe_price = round(float(pe_ord['averageprice']), 1)
                            if pe_price != 0 :
                                try:
                                    clients[i]['pe_entry_price'] = pe_price
                                    pe_sl_price = round(((1+STOPLOSS)*float(pe_price)), 1)                                    
                                    pe_sl = place_stoploss(client, instruments['short']['PE']['symbol'],\
                                                       instruments['short']['PE']['token'], qty, pe_sl_price)
                                    clients[i]['orders']['pe_sl'] = pe_sl
                                    print(f'{clients[i]["user"]} -- pe_sl -- {pe_sl}')
                                except Exception as e:
                                    logging.info(f'{clients[i]["user"]} --  pe_sl  -- {str(e)}')                                
                            else:
                                print(f'{clients[i]["user"]} -- pe_price = 0')
                        else:
                            print(f'{clients[i]["user"]} -- pe_ord = None')                               
                    except Exception as e:
                        logging.info(f'{clients[i]["user"]} ---- {str(e)}')                
                    sleep(1)
                sleep(1.5)
                open_pos = get_open_positions(client)
                if s[0] in open_pos and s[1] in open_pos and s[2] in open_pos and s[3] in open_pos:
                    break
                sleep(1)
                count+=1
    
def reentry_order(client, fyers_symbol, instrument, token, qty, i, opt):
    global clients, logging
    logging.info('reentry order running')
    sl_placed = False
    # ins = clients[i]['instruments']
    open_pos = get_open_positions(client)
    if instrument not in open_pos:
        sleep(1.2)
        try:
            # price = round((float(get_ltp(fyers_symbol))-5), 1)
            price = round(float((angel_ltp(client, instrument, token)-2)), 1)
            if price < 0.5:
                price = 0.5
            ord_id = place_sell_limit(client, instrument, token, qty, price)
        except Exception as e:
            logging.info(f'Error with reentry order = {str(e)}')
        # ord_id = None
    sleep(1.5)
    if ord_id is not None:
        entry_price = round(float(get_order_call_orders(client, ord_id)['averageprice']), 1)
        if entry_price != 0:
            print('placed reentry order')
            sl_price = round((1+STOPLOSS)*entry_price, 1)
            if sl_price != 0:
                try:
                    sl_ord = place_stoploss(client, instrument, token, qty, sl_price)
                    sl_placed = True
                except Exception as e:
                    logging.info(f'Error = {str(e)}')
                    # sl_ord = None
                sleep(2)
        if sl_ord is None or ord_id is None:
            raise Exception("SL order or Order is None")
        
        if opt == 'CE' and sl_placed:
            clients[i]['ce_reentry']+=1
            clients[i]['orders']['ce_short'] = ord_id
            clients[i]['orders']['ce_sl'] = sl_ord
            clients[i]['ce_entry_price'] = entry_price
            # clients[i]['instruments']['short']['CE']['symbol'] = instrument
            # clients[i]['instruments']['short']['CE']['token'] = token
        elif opt == 'PE' and sl_placed:
            clients[i]['pe_reentry']+=1
            clients[i]['orders']['pe_short'] = ord_id
            clients[i]['orders']['pe_sl'] = sl_ord
            clients[i]['pe_entry_price'] = entry_price
        # clients[i]['instruments']['short']['PE']['symbol'] = instrument
        # clients[i]['instruments']['short']['PE']['token'] = token
# clients[0]['orders']['pe_sl'] = '221025000285107'
def get_open_qty(client, token):
    #Positive Quantity for Short position
    positions = client.position()['data']
    for pos in positions:
        if int(pos['symboltoken']) == int(token) and pos['producttype']=='INTRADAY':
            return int(int(pos['sellqty'])-int(pos['buyqty']))
    return None

def open_qty_positions(token, positions):
    for pos in positions:
        if int(pos['symboltoken']) == int(token) and pos['producttype']=='INTRADAY':
            return int(int(pos['sellqty'])-int(pos['buyqty']))
    return None

def get_order_from_orders(client, orders, ord_id):
    # orders = clients[i]['client'].orderBook()
    order = None
    for o in orders['data']:
        if o['orderid'] == str(ord_id):
            order = o
            break
    return order


def get_open_pos_from_positions(positions):
    open_pos = []
    if positions is None:
        return open_pos
    for pos in positions:
        if abs(int(pos['sellqty'])-int(pos['buyqty'])) != 0 and pos['producttype']=='INTRADAY':
            open_pos.append(pos['tradingsymbol'])
    
    return open_pos


def exit_single(symbol, token, open_positions, positions, symbol_dict, client):
    if symbol in open_positions:
        qty = int(open_qty_positions(int(token), positions))
        print('exiting --- ',qty)
        # if count%3 == 0 and count != 0:
        #     sleep(1)
        # sleep(1.5)
        # while True:
        if qty is None or qty == 0:
            logging.info(f"Open QTY=0 for {symbol}")
            print(f"Open QTY=0 for {symbol}")
        else:
            try:
                if qty > 0:
                    try:
                        # ord_id = place_order(client, pos['tradingsymbol'], pos['symboltoken'], qty, 'BUY', 0)
                        price = round(((float(angel_ltp(client, symbol, token)))+3), 1)
                        # price = round((float(get_ltp(symbol_dict[symbol][0]))+4), 1)
                        ord_id = place_buy_limit(client, symbol, int(token), qty, price)
                    except Exception as e:
                        logging.info(str(e))
                        print(str(e))
                elif qty < 0:
                    qty = abs(qty)
                    try:
                        price = round(((float(angel_ltp(client, symbol, token)))-2), 1)
                        # price = round((float(get_ltp(symbol_dict[symbol][0]))-4), 1)
                        if price < 0.5:
                            price = 0.5
                        ord_id = place_sell_limit(client, symbol, int(token), qty, price)
                        # ord_id = place_order(client, pos['tradingsymbol'], pos['symboltoken'], qty, 'SELL', 0)
                    except Exception as e:
                        logging.info(str(e))
                        print(str(e))                                 
                if ord_id is None:
                    raise Exception(f"Order ID is None for {symbol}")
                # count+=1
                # strategy_pos[pos['tradingsymbol']] = ord_id
                # sleep(2)
                # qty = get_open_qty(client, int(pos['symboltoken']))
                # if qty is None or qty == 0:
                #     break
                # sleep(1.5)
            except Exception as e:
                logging.info(str(e))
                print(str(e))
                sleep(1) 


def exit_positions(i, ins):
    global clients
    
    # ins = clients[i]['instruments']
    client = clients[i]['client']
    positions = client.position()['data']
    print(client.userId)
    if positions is not None:
    
        strategy_pos = {ins['long']['CE']['symbol']:[ins['long']['CE']['fyers_sym'], int(ins['long']['CE']['token'])],\
                        ins['long']['PE']['symbol']:[ins['long']['PE']['fyers_sym'], int(ins['long']['PE']['token'])],\
                        ins['short']['CE']['symbol']:[ins['short']['CE']['fyers_sym'], int(ins['short']['CE']['token'])],\
                        ins['short']['PE']['symbol']:[ins['short']['PE']['fyers_sym'], int(ins['short']['PE']['token'])]}
        # get_ltp(ins['long']['PE']['fyers_sym'])
        # sleep(1)
        count = 0
        open_pos = get_open_pos_from_positions(positions)
        # for pos in strategy_pos:
        exit_single(ins['short']['CE']['symbol'], int(ins['short']['CE']['token']), open_pos, positions, strategy_pos, client)
        exit_single(ins['short']['PE']['symbol'], int(ins['short']['PE']['token']), open_pos, positions, strategy_pos, client)
        exit_single(ins['long']['CE']['symbol'], int(ins['long']['CE']['token']), open_pos, positions, strategy_pos, client)
        exit_single(ins['long']['PE']['symbol'], int(ins['long']['PE']['token']), open_pos, positions, strategy_pos, client)
        
                        # continue
                # sleep(2)
        sleep(2)
        # orders = client.orderBook()
        # sleep(2)
        # pos_symbols = list(strategy_pos.keys())
        # for p in pos_symbols:
        #     ord1 = get_order(client, orders, strategy_pos[p])
        #     if ord1['status'] == 'rejected' and re.match('zero',ord1['text']):
                
        count = 0
        while True:
            if count >= 5:
                break
            open_pos = get_open_positions(client)
            if ins['long']['CE']['symbol'] in open_pos:
                sleep(1)
                qty = get_open_qty(client, int(ins['long']['CE']['token']))
                if qty < 0:
                    qty = abs(qty)
                    sleep(1)
                    try:
                        ord_id = place_order(client, ins['long']['CE']['symbol'], int(ins['long']['CE']['token']), qty, 'SELL', 0)
                    except Exception as e:
                        logging.info(str(e))
                        print(str(e))
            
            if ins['long']['PE']['symbol'] in open_pos:
                sleep(1)
                qty = get_open_qty(client, int(ins['long']['PE']['token']))
                if qty < 0:
                    qty = abs(qty)
                    sleep(1)
                    try:
                        ord_id = place_order(client, ins['long']['PE']['symbol'], int(ins['long']['PE']['token']), qty, 'SELL', 0)
                    except Exception as e:
                        logging.info(str(e))
                        print(str(e)) 
                        
            if ins['short']['CE']['symbol'] in open_pos:
                sleep(1)
                qty = get_open_qty(client, int(ins['short']['CE']['token']))
                if qty > 0:
                    qty = abs(qty)
                    sleep(1)
                    try:
                        ord_id = place_order(client, ins['short']['CE']['symbol'], int(ins['short']['CE']['token']), qty, 'BUY', 0)
                    except Exception as e:
                        logging.info(str(e))
                        print(str(e))

            if ins['short']['PE']['symbol'] in open_pos:
                sleep(1)
                qty = get_open_qty(client, int(ins['short']['PE']['token']))
                if qty > 0:
                    qty = abs(qty)
                    sleep(1)
                    try:
                        ord_id = place_order(client, ins['short']['PE']['symbol'], int(ins['short']['PE']['token']), qty, 'BUY', 0)
                    except Exception as e:
                        logging.info(str(e))
                        print(str(e))
            
            if ins['long']['CE']['symbol'] not in open_pos and ins['long']['PE']['symbol'] not in open_pos and\
                ins['short']['CE']['symbol'] not in open_pos and ins['short']['PE']['symbol'] not in open_pos:
                    print(client.userId,'     exited and  ---  breaking')
                    break
            count+=1
            sleep(2)
                
        print(client.userId,'     exited')
                    
            

# def get_pnl(client):
#     global ins




def reentry_thread(i, ce_series, pe_series, lot):
    global clients
    global logging
    global ce_reentry, pe_reentry
    
    
    client = clients[i]['client']
    orders = client.orderBook()
    ce_order = get_order_from_orders(client, orders, str(clients[i]['orders']['ce_sl']))
    pe_order = get_order_from_orders(client, orders, str(clients[i]['orders']['pe_sl']))
    sleep(1)
    print(f"{abs(ce_series['ltp']-short_premium)}   {abs(pe_series['ltp']-short_premium)}   {(0.1*short_premium)}")
    if ce_order['status'] == 'complete':
        print('ce order complete')
        if clients[i]['ce_reentry'] < ce_reentry and abs(ce_series['ltp']-short_premium)<(0.05*short_premium):
            reentry_order(client, str(ce_series['fyers_sym']), str(ce_series['symbol']), int(ce_series['token']),\
                int(int(clients[i]['lots'])*lot), i, 'CE')
    
    elif pe_order['status'] == 'complete':
        print('pe order complete')
        if clients[i]['pe_reentry'] < pe_reentry and abs(pe_series['ltp']-short_premium)<(0.05*short_premium):
            # print('abc')
            reentry_order(client, str(ce_series['fyers_sym']), str(pe_series['symbol']), int(pe_series['token']),\
                int(int(clients[i]['lots'])*lot), i, 'PE')
    
    logging.info(f"{ce_order['status']}\n{pe_order['status']}")
    

def update_reentry(lock):
    global clients, sheet3, row
    while True:
        lock.acquire()
        temp_sheet = pd.DataFrame(data=sheet3.get_all_records())
        if int(temp_sheet['CE_Reentries'].iloc[row]) != int(clients[0]['ce_reentry']):
            sheet3.update_cell(row+2, 7, int(clients[0]['ce_reentry']))
        if int(temp_sheet['PE_Reentries'].iloc[row]) != int(clients[0]['pe_reentry']):
            sheet3.update_cell(row+2, 8, int(clients[0]['pe_reentry']))
        lock.release()
        sleep(15)
        


def cancel_open_orders(i):
    global clients, ins
    
    strategy_pos = [ins['long']['CE']['symbol'], ins['long']['PE']['symbol'], ins['short']['CE']['symbol'],\
                    ins['short']['PE']['symbol']]
    
    client = clients[i]['client']
    orders = client.orderBook()['data']
    for order in orders:
        if (order['ordertype'] == 'STOPLOSS_LIMIT' or order['ordertype']=='LIMIT') and order['status']=='trigger pending' and\
            str(order['tradingsymbol']) in strategy_pos:
            client.cancelOrder(order['orderid'], order['variety'])
    

def order_cancel_threads():
    global clients
    order_exit = []
    for i in range(len(clients)):
        order_exit.append(
            Thread
            (
                    target = cancel_open_orders,
                    args = [i],
                    name = f"Order_Cancel_{i}"
            )
        )
        order_exit[-1].start()
    
    for thread in order_exit:
        thread.join()
 

def pos_exit_threads():
    global clients, ins
    pos_exit = []
    for i in range(len(clients)):
        pos_exit.append(
            Thread
            (
                target = exit_positions,
                args = (i, ins),
                name = f"Position_exit_{i}"
            )
        )
        pos_exit[-1].start()
    
    for thread in pos_exit:
        thread.join()

# sl_modified = True

def modify_stoploss_values(i, instruments, sl_perc, lot_size):
    global clients
    client = clients[i]['client']
    qty = int(int(clients[i]['lots'])*lot_size)
    ce_price = round((sl_perc*float(clients[i]['ce_entry_price'])), 1)
    pe_price = round((sl_perc*float(clients[i]['pe_entry_price'])), 1)
    
    if ce_price != 0:
        print('modify --- ce_price = ',ce_price)
        modify_sl_order(client, instruments['short']['CE']['symbol'], int(instruments['short']['CE']['token']),\
                        clients[i]['orders']['ce_sl'], qty, ce_price)
        # clients[i]['orders']['ce_sl'] = ce_sl
    sleep(3)
        
    if pe_price != 0:
        print('modify --- pe_price = ',pe_price)
        modify_sl_order(client, instruments['short']['PE']['symbol'], int(instruments['short']['PE']['token']),\
                        clients[i]['orders']['pe_sl'], qty, pe_price)
        # clients[i]['orders']['pe_sl'] = pe_sl
    



def get_data(lot_size):
    global df, exit_time, stop_thread, sheet2, row, STOPLOSS, clients, ins, name, sheet3
    start = time.time()
    prev_sl = STOPLOSS
    while dt.datetime.now() <= dt.datetime.combine(dt.datetime.now().date(), dt.time(15,40)):
        try:
            pause_file = open('pause.txt','r')
            val = int(pause_file.read())
            pause_file.close()
            
            if val != 0:
                while True:
                    temp_pause = open('pause.txt','r')
                    temp_val = int(temp_pause.read())
                    temp_pause.close()
                    print('get_data sleeping')
                    if temp_val == 0:
                        break
                    sleep(3)
            print(STOPLOSS)
            if stop_thread is True:
                break
            df = pd.DataFrame(data=sheet2.get_all_records())
            
            sheet_exit = dt.datetime.strptime(str(df['Exit_Time'].iloc[row]), '%H:%M').time()
            exit_time = dt.datetime.combine(dt.datetime.now().date(), sheet_exit)
            
            STOPLOSS = float(df['SL Percentage'].iloc[row])
            
            if prev_sl != STOPLOSS:
                print('Prev SL != SL')
                pause_file = open('pause.txt','w')
                pause_file.write('1')
                pause_file.close()
                
                print('modify threads starting')
                modify_threads = []
                for i in range(len(clients)):
                    modify_threads.append(
                            Thread
                            (
                                target = modify_stoploss_values,
                                args = [
                                            i,
                                            ins,
                                            float(1+STOPLOSS),
                                            lot_size
                                        ],
                                name = f'modify_order_client_{i}'
                            )
                        )
                    modify_threads[-1].start()
                
                for thread in modify_threads:
                    thread.join()
                
                pause_file = open('pause.txt','w')
                pause_file.write('0')
                pause_file.close()                
                
                

                                
                    
            if int(df['Exit_Stop'].iloc[row]) == 1:
                pause_file = open('pause.txt','w')
                pause_file.write('1')
                pause_file.close()
                
                order_exit = []
                for i in range(len(clients)):
                    order_exit.append(
                        Thread
                        (
                                target = cancel_open_orders,
                                args = [i],
                                name = f"Order_Cancel_{i}"
                        )
                    )
                    order_exit[-1].start()
                
                for thread in order_exit:
                    thread.join()

                pos_exit = []
                for i in range(len(clients)):
                    pos_exit.append(
                        Thread
                        (
                            target = exit_positions,
                            args = (i, ins),
                            name = f"Position_exit_{i}"
                        )
                    )
                    pos_exit[-1].start()
                
                for thread in pos_exit:
                    thread.join()
                
                # sleep(45)
                tel_thread = Thread(
                        target = queue_position_alerts,
                        args = [
                                    'Exit ',
                                    clients,
                                    ins['short']['CE']['symbol'],
                                    ins['short']['PE']['symbol'],
                                    ins['long']['CE']['symbol'],
                                    ins['long']['PE']['symbol'],
                                    lot_size,
                                    True,
                                    5
                                ],
                        name = 'Exit_telegram'
                    )
                
                tel_thread.start()
                tel_thread.join()            
    
                sheet2.update_cell(row+2, 11, 0)
                sheet3.update_cell(row+2, 2, (dt.datetime.now()-dt.timedelta(days=1)).strftime('%d-%m-%Y'))
                
                pause_file = open('pause.txt','w')
                pause_file.write('0')
                pause_file.close()   
                
                stop_thread = True
                os._exit(1)
            
            if dt.datetime.now() >= exit_time:
                pause_file = open('pause.txt','w')
                pause_file.write('1')
                pause_file.close()
                
                order_exit = []
                for i in range(len(clients)):
                    order_exit.append(
                        Thread
                        (
                                target = cancel_open_orders,
                                args = [i],
                                name = f"Order_Cancel_{i}"
                        )
                    )
                    order_exit[-1].start()
                
                for thread in order_exit:
                    thread.join()

                pos_exit = []
                for i in range(len(clients)):
                    pos_exit.append(
                        Thread
                        (
                            target = exit_positions,
                            args = (i, ins),
                            name = f"Position_exit_{i}"
                        )
                    )
                    pos_exit[-1].start()
                
                for thread in pos_exit:
                    thread.join()
                
                # sleep(45)
                tel_thread = Thread(
                        target = queue_position_alerts,
                        args = [
                                    'Exit ',
                                    clients,
                                    ins['short']['CE']['symbol'],
                                    ins['short']['PE']['symbol'],
                                    ins['long']['CE']['symbol'],
                                    ins['long']['PE']['symbol'],
                                    lot_size,
                                    True,
                                    5
                                ],
                        name = 'Exit_telegram'
                    )
                
                tel_thread.start()
                tel_thread.join()            
    
                # sheet2.update_cell(row+2, 11, 0)
                sheet3.update_cell(row+2, 2, (dt.datetime.now()-dt.timedelta(days=1)).strftime('%d-%m-%Y'))
                
                pause_file = open('pause.txt','w')
                pause_file.write('0')
                pause_file.close()   
                
                stop_thread = True
                os._exit(1)
                
            prev_sl = STOPLOSS
                
        except Exception as e:
            print(str(e))
            print(traceback.format_exc())
            continue                
                
                
        # sleep((6)-((time.time()-start)%(6.0)))
        if name.upper() == 'NIFTY':
            sleep(2.5-((time.time()-start)%2.5))
        elif name.upper() == 'BANKNIFTY':
            sleep(3-((time.time()-start)%3.0))
        else:
            sleep(3.3-((time.time()-start)%3.0))    


def clients_add_sl(i, ce_symbol, pe_symbol, ce_reentered, pe_reentered):
    global clients
    client = clients[i]['client']
    # print(client.getProfile(client.refresh_token))
    ce_sl_ord, pe_sl_ord = get_ce_pe_sl(client, ins['short']['CE']['symbol'],ins['short']['PE']['symbol'])
    print('ce_sl ---- ',ce_sl_ord,'   ---- pe_sl   ',pe_sl_ord)
    if 'orders' in clients[i].keys():
        print('orders present')
    else:
        clients[i]['orders'] = {}
    if ce_sl_ord is not None:
        clients[i]['orders']['ce_sl'] = ce_sl_ord
    if pe_sl_ord is not None:
        clients[i]['orders']['pe_sl'] = pe_sl_ord
    clients[i]['ce_reentry'] = int(ce_reentered)
    clients[i]['pe_reentry'] = int(pe_reentered)





csv, index = get_angel_instruments()

csv['index'] = ''
for i in range(len(csv)):
    x = csv.iloc[i]
    csv['index'].iloc[i] = x['name']+str(x['strike'])+str(x['option'])

csv.set_index('index', inplace=True)

csv['fyers_sym'] = fyers.ins['symbol']
# pos = clients[0]['client'].position()

if dt.datetime.now() < (entry_time-dt.timedelta(minutes=4)):
    time_920 = dt.datetime.combine(dt.datetime.now().date(), entry_time)
    to_sleep = (time_920-dt.datetime.now()).seconds
    sleep(to_sleep)


try:
    sheet_date = dt.datetime.strptime(constant_sheet['DATE'].iloc[row], '%d-%m-%Y')
except:
    print('sheet date except')
    sheet_date = dt.datetime(2020, 5, 3)


lock = threading.Lock()

if dt.datetime.now() < entry_time:
    time_920 = dt.datetime.combine(dt.datetime.now().date(), entry_time)
    to_sleep = (time_920-dt.datetime.now()).seconds
    sleep(to_sleep)



if sheet_date.date() == dt.datetime.now().date():
    print('fetching from sheet')
    ins['short'] = {}
    ins['short']['CE'] = {}
    ins['short']['CE']['symbol'] = str(constant_sheet['CE_SHORT'].iloc[row])
    print('step 0\n',ins)
    ins['short']['CE']['token'], ins['short']['CE']['fyers_sym'] = get_token_from_symbol(ins['short']['CE']['symbol'], csv)
    ins['short']['CE']['ltp'] = get_ltp(ins['short']['CE']['fyers_sym'])
    
    ins['short']['PE'] = {}
    ins['short']['PE']['symbol'] = str(constant_sheet['PE_SHORT'].iloc[row])
    ins['short']['PE']['token'], ins['short']['PE']['fyers_sym'] = get_token_from_symbol(ins['short']['PE']['symbol'], csv)
    ins['short']['PE']['ltp'] = get_ltp(ins['short']['PE']['fyers_sym'])
    
    ins['long'] = {}
    ins['long']['CE'] = {}
    ins['long']['CE']['symbol'] = str(constant_sheet['CE_LONG'].iloc[row])
    ins['long']['CE']['token'], ins['long']['CE']['fyers_sym'] = get_token_from_symbol(ins['long']['CE']['symbol'], csv)
    
    ins['long']['PE'] = {}
    ins['long']['PE']['symbol'] = str(constant_sheet['PE_LONG'].iloc[row])
    ins['long']['PE']['token'], ins['long']['PE']['fyers_sym'] = get_token_from_symbol(ins['long']['PE']['symbol'], csv)
    
    ce_re = int(constant_sheet['CE_Reentries'].iloc[row])
    pe_re = int(constant_sheet['PE_Reentries'].iloc[row])
    lot_size = int(csv[csv['symbol']==ins['long']['CE']['symbol']]['lotsize'].iloc[0])
    print('step 1')
    sl_ord_threads = []
    for i in range(len(clients)):
        sl_ord_threads.append(
                Thread(
                        target = clients_add_sl,
                        args = [
                                    i,
                                    ins['short']['CE']['symbol'],
                                    ins['short']['PE']['symbol'],
                                    ce_re,
                                    pe_re
                                ],
                        name = f'add_sl_client{i}'
                    )
            )
        sl_ord_threads[-1].start()
    
    print('threads started')
    for thread in sl_ord_threads:
        thread.join()
    print(ins)
    sleep(2)    
else:
    start = time.time()
    csv = add_ltp(csv)
    print(time.time()-start)
    
    ce_df = csv[csv['option']=='CE']
    pe_df = csv[csv['option']=='PE']
    
    ce_short = closest_value(ce_df, short_premium)
    pe_short = closest_value(pe_df, short_premium)
    
    ce_buy = closest_value(ce_df, long_premium)
    pe_buy = closest_value(pe_df, long_premium)
    
    ins['short'] = {}
    ins['short']['CE'] = {}
    ins['short']['CE']['symbol'] = ce_short['symbol']
    ins['short']['CE']['token'], ins['short']['CE']['fyers_sym'] = get_token_from_symbol(ins['short']['CE']['symbol'], csv)
    ins['short']['CE']['ltp'] = get_ltp(ins['short']['CE']['fyers_sym'])
    
    ins['short']['PE'] = {}
    ins['short']['PE']['symbol'] = pe_short['symbol']
    ins['short']['PE']['token'], ins['short']['PE']['fyers_sym'] = get_token_from_symbol(ins['short']['PE']['symbol'], csv)
    ins['short']['PE']['ltp'] = get_ltp(ins['short']['PE']['fyers_sym'])
    
    ins['long'] = {}
    ins['long']['CE'] = {}
    ins['long']['CE']['symbol'] = ce_buy['symbol']
    ins['long']['CE']['token'], ins['long']['CE']['fyers_sym'] = get_token_from_symbol(ins['long']['CE']['symbol'], csv)
    
    ins['long']['PE'] = {}
    ins['long']['PE']['symbol'] = pe_buy['symbol']
    ins['long']['PE']['token'], ins['long']['PE']['fyers_sym'] = get_token_from_symbol(ins['long']['PE']['symbol'], csv)
    
    lot_size = int(ce_buy['lotsize'])
    
    sheet3.update_cell(row+2, 3, ins['long']['CE']['symbol'])
    sheet3.update_cell(row+2, 4, ins['long']['PE']['symbol'])
    sheet3.update_cell(row+2, 5, ins['short']['CE']['symbol'])
    sheet3.update_cell(row+2, 6, ins['short']['PE']['symbol'])
    sheet3.update_cell(row+2, 2, dt.datetime.now().strftime('%d-%m-%Y'))
    sheet3.update_cell(row+2, 7, 0)
    sheet3.update_cell(row+2, 8, 0)


logging.info(f'{ins}')


pause_file = open('pause.txt','w')
pause_file.write('1')
pause_file.close()


entry_threads = []
for i in range(len(clients)):
    entry_threads.append(
        Thread(
            target = init_order_thread,
            args=(
                clients[i]['client'],
                ins,
                int(clients[i]['lots'])*lot_size,
                i
                ),
            name = f'Initial_Short_client_{i}'
            )
        )
    entry_threads[-1].start()
    
for thread in entry_threads:
    thread.join()


sleep(10)

error_threads = []
for i in range(len(clients)):
    error_threads.append(
        Thread(
            target = check_entry,
            args = (
                    clients[i]['client'],
                    ins,
                    i,
                    clients[i]['lots'],
                    lot_size
                    ),
            name = f'Entry_error_{i}'
            )
        )
    error_threads[-1].start()

for thread in error_threads:
    thread.join()



# str(dt.datetime.now())
tel_thread = Thread(
        target = queue_position_alerts,
        args = [
                    'Entry ',
                    clients,
                    ins['short']['CE']['symbol'],
                    ins['short']['PE']['symbol'],
                    ins['long']['CE']['symbol'],
                    ins['long']['PE']['symbol'],
                    lot_size,
                    False,
                    3
                ],
        name = 'Entry_telegram'
    )

tel_thread.start()
tel_thread.join()

sleep(1)

total_rows = position_rows(
                    clients,
                    ins['short']['CE']['symbol'],
                    ins['short']['PE']['symbol'],
                    ins['long']['CE']['symbol'],
                    ins['long']['PE']['symbol'],
                    lot_size,
                    False
                )

print(total_rows)

remove_clients = []


rem_threads = []
rem_order_exit = []

for r in total_rows:
    if r['short_ce'] != 0 or r['short_pe'] != 0 or r['long_ce'] != 0 or\
        r['long_pe'] != 0:
            print(f'Removing {r["Client"]}')
                
            rem_order_exit.append(
                Thread
                (
                        target = cancel_open_orders,
                        args = [int(r['No'])],
                        name = f"Order_Cancel_{r['Client']}"
                )
            )
            rem_order_exit[-1].start()
            
            sleep(1.5)
            
            rem_threads.append(
                Thread(
                target = exit_positions,
                args = [
                            int(r['No']),
                            ins
                        ],
                name = f'{r["Client"]}_remove'
                )
            )
            remove_clients.append(r['Client'])
            rem_threads[-1].start()

for thread in rem_order_exit:
    thread.join()

for thread in rem_threads:
    thread.join()


tel_thread = Thread(
        target = queue_position_alerts,
        args = [
                    'Entry ',
                    clients,
                    ins['short']['CE']['symbol'],
                    ins['short']['PE']['symbol'],
                    ins['long']['CE']['symbol'],
                    ins['long']['PE']['symbol'],
                    lot_size,
                    False,
                    2
                ],
        name = 'Entry_telegram'
    )

tel_thread.start()
tel_thread.join()


pause_file = open('pause.txt', 'w')
pause_file.write('0')
pause_file.close()

txt_file = open('init_pause.txt', 'w')
txt_file.write('0')
txt_file.close()

clients = [i for i in clients if i['user'] not in remove_clients]

print(clients)

if len(clients) == 0:
    os._exit(1)



data_thread = Thread(
        target = get_data,
        name = 'data_thread',
        args = [lot_size]
    )

reentries_update_thread = Thread(
        target = update_reentry,
        args = [lock],
        name = 'reentries_updater'
    )

data_thread.start()
reentries_update_thread.start()

start = time.time()

while dt.datetime.now() < dt.datetime.combine(dt.datetime.now().date(), dt.time(15,40)):
    try:
        pause_file = open('pause.txt','r')
        val = int(pause_file.read())
        pause_file.close()
        if val == 0:
            try:
                ins['short']['CE']['ltp'], ins['short']['PE']['ltp'] = \
                    fyers.running_ltp(ins['short']['CE']['fyers_sym'], ins['short']['PE']['fyers_sym'])
                # csv = add_ltp(csv)
                # ce_df = csv[csv['option']=='CE']
                # pe_df = csv[csv['option']=='PE']
                
                ce_short = ins['short']['CE']
                pe_short = ins['short']['PE']
                print(ce_short['ltp'],'   -----    ',pe_short['ltp'])
            except Exception as e:
                logging.info(f"Exception in adding LTPs : {str(e)}")
                continue
            
            reentry_list = []
            for i in range(len(clients)):
                reentry_list.append(
                        Thread(
                                target = reentry_thread,
                                args = (
                                            i,
                                            ce_short,
                                            pe_short,
                                            lot_size
                                        ),
                                name = f'reentry_client_{i}'
                            )
                    )
                reentry_list[-1].start()
            
            for thread in reentry_list:
                thread.join()
                
        else:
            while True:
                print('val is not 0')
                temp_pause = open('pause.txt', 'r')
                temp = int(temp_pause.read())
                if temp == 0:
                    break
                temp_pause.close()
                sleep(3)
            
    except KeyboardInterrupt as e:
        stop_thread = True
        os._exit(1)
        data_thread.join()
        reentries_update_thread.join()
        
    if name.upper() == 'NIFTY':
        sleep(8-((time.time()-start)%8))
    elif name.upper() == 'BANKNIFTY':
        sleep(5-((time.time()-start)%5))
    else:
        sleep(7-((time.time()-start)%7))    



# l = [1, 2, 3, 4, 5]

# list(reversed(l))
# clients[0]

# orders = clients[0]['client'].orderBook()
# pos = clients[0]['client'].position()


# ltp = get_ltp(csv['symbol'][0], csv['token'][0])

