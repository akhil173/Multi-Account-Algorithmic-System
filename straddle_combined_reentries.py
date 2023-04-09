# -*- coding: utf-8 -*-
"""
Created on Sun Nov  6 12:43:23 2022

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
import os
import sys
from functions import get_token_from_symbol, get_sl_order, get_open_positions, position_from_orders
from functions import pos_list_from_orders
from straddle_telegram_alerts import queue_position_alerts, queue_sl_alert, generate_position_alert
from straddle_telegram_alerts import send_message
import random
from functions import position_rows, get_entry_prices, get_ce_pe_sl
import traceback
from fyers_account import fyersAPI
import threading
import subprocess



cred = 'credentials.json'

gc = gspread.service_account(filename=cred)

sh = gc.open_by_url('')
sheet2 = sh.get_worksheet(0)
sheet3 = sh.get_worksheet(1)
client_sheet = sh.get_worksheet(3)

df = pd.DataFrame(data=sheet2.get_all_records())
# float(df['Short Preimium'].iloc[row])
constant_sheet = pd.DataFrame(data=sheet3.get_all_records())

# try:
# row = 1
row = int(sys.argv[1])
PATH = str(sys.argv[2])
# except Exception as e:
    # logging.info(str(e))
# logging.info(f'{row}')

# os._exit(1)

name = str(df['Instrument'].iloc[row])
short_premium = float(df['Short Preimium'].iloc[row])
long_premium = float(df['Long Premium'].iloc[row])
stoploss = float(df['Combined SL Percentage'].iloc[row])
STOPLOSS = None
sheet_entry = dt.datetime.strptime(str(df['Entry_Time'].iloc[row]), '%H:%M').time()
entry_time = dt.datetime.combine(dt.datetime.now().date(), sheet_entry)
sheet_exit = dt.datetime.strptime(str(df['Exit_Time'].iloc[row]), '%H:%M').time()
exit_time = dt.datetime.combine(dt.datetime.now().date(), sheet_exit)
points = int(df['Points_from_ATM'].iloc[row])
quantity = 1
target = float(df['Target'].iloc[row])

global_ce_price = None
global_pe_price = None


stoploss_flag = True

reenter_flag = True
# dt.datetime.now() < exit_time
# to_exit = False

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

logging.info(f'{name}\n{short_premium}\n{long_premium}\n{STOPLOSS}\n{entry_time}\n{exit_time}')

# os._exit(1)
stop_thread = False


# if dt.datetime.now().date() >= dt.date(2022, 12, 25):
#     os.remove('main.py')
#     os.remove('reentries.py')
#     os._exit(1)


if int(df['Exit_Stop'].iloc[row]) == 1:
    logging.info(f'{name} is not set to run today')
    txt_file = open('init_pause.txt', 'w')
    txt_file.write('0')
    txt_file.close()    
    os._exit(1)

# angel = ltp_authorize.ltp_login()

client_df = pd.DataFrame(client_sheet.get_all_records())

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
                        client_sheet.update_cell(int(i)+2, 10, temp_client.access_token)
                        client_sheet.update_cell(int(i)+2, 11, dt.datetime.now().strftime('%d-%m-%Y'))
                        
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

# orders = clients[0]['client'].orderBook()
# for i in range(5):
#     if i>0:
#         print(time.time())
#         print(i, '   ---   ',(1-(time.time()-cur)))
#         sleep((1-(time.time()-cur)))
#     orders = clients[0]['client'].orderBook()
#     pos = pos_list_from_orders(orders)
#     cur = time.time()
#     print('cur   --- ',cur)
# total_orders = {}

# for i in range(len(clients)):
#     orders = clients[i]['client'].orderBook()['data']
#     total_orders[clients[i]['user']] = orders.copy()

# position_from_orders(orders)

deleted_clients = []

lock = threading.Lock()

# pos_list_from_orders()
# orders = clients[0]['client'].orderBook()
       
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

# pos = angel.position()
# angel.getProfile(angel.refresh_token)['status']
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

# fyers.client.quotes({'symbols':'NSE:FINNIFTY-INDEX'})

def get_ltp(symbol):
    global fyers
    return fyers.single_ltp(symbol)

def angel_ltp(client, symbol, token):
    return round(float(client.ltpData('NFO', symbol, token)['data']['ltp']), 1)

def closest_value(df, input_value, opt, strike=None, points=None):
    global name
    if opt.upper() == 'CE':
        df = df[df['option']=='CE']
    elif opt.upper() == 'PE':
        df = df[df['option']=='PE']    
    if strike is not None:
        if opt.upper() == 'PE':
            df = df[df['strike']>strike]
        elif opt.upper() == 'CE':
            df = df[df['strike']<strike]
    
    if points is not None:
        if name.upper() == 'NIFTY':
            index = 'NSE:NIFTY50-INDEX'
            base = 50
        elif name.upper() == 'BANKNIFTY':
            index = 'NSE:NIFTYBANK-INDEX'
            base = 100
        elif name.upper() == 'FINNIFTY':
            index = 'NSE:FINNIFTY-INDEX'
            base = 50
        atm = int(base*round(get_ltp(index)/base))
        if opt.upper() == 'CE':
            df = df[(df['strike']>atm) & (df['strike']<=(atm+points))]
        elif opt.upper() == 'CE':
            df = df[(df['strike']<atm) & (df['strike']>=(atm-points))]          
        
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


def call_order(client):
    try:
        return client.orderBook()
    except:
        sleep(1.2)
        return client.orderBook()


# client = clients[0]['client']
# for i in range(5):
#     a = call_order(client)

def get_order_call_orders(client, ord_id):
    orders = call_order(client)

    for o in orders['data']:
        if o['orderid'] == str(ord_id):
            return o
    return None


def get_sl_orders(client, pe_ord_id, ce_ord_id):
    orders = call_order(client)
    pe_order = None
    ce_order = None
    for o in orders['data']:
        if o['orderid'] == str(pe_ord_id):
            pe_order = o
        if o['orderid'] == str(ce_ord_id):
            ce_order = o
    return pe_order, ce_order

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
            

def init_order_thread(client, instruments, qty, i):
    global clients, global_ce_price, global_pe_price
    # client = clients[i]['client']
    sleep(2)

    try:
        positions = get_open_positions(client)
    except Exception as e:
        orders = call_order(client)
        if orders['data'] is None:
            positions = []
        else:
            positions = position_from_orders(orders)
    try:
        if instruments['long']['CE']['symbol'] not in positions and instruments['long']['PE']['symbol'] not in positions:
            clients[i]['init_entry'] = True
            clients[i]['orders'] = {}
            
            ce_long_price = angel_ltp(client, instruments['long']['CE']['symbol'], instruments['long']['CE']['token'])+1
            pe_long_price = angel_ltp(client, instruments['long']['PE']['symbol'], instruments['long']['PE']['token'])+1
                
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
                
            ce_short_price = angel_ltp(client, instruments['short']['CE']['symbol'], instruments['short']['CE']['token'])-2
            pe_short_price = angel_ltp(client, instruments['short']['PE']['symbol'], instruments['short']['PE']['token'])-2
            
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
            
            ord_book = call_order(client)
            
            try:
                ce_ord = get_order(client, ord_book, ce_short)
                if ce_ord is not None:
                    ce_price = round(float(ce_ord['averageprice']), 1)
                    lock.acquire()                   
                    if ce_price != 0:
                        clients[i]['ce_entry_price'] = ce_price
                        if global_ce_price is None:
                            global_ce_price = float(ce_price)
                    else:
                        print(f'{clients[i]["user"]} -- ce_price = 0')
                    lock.release()
                else:
                    print(f'{clients[i]["user"]} -- ce_ord = None')                       
            except Exception as e:
                logging.info(f'{clients[i]["user"]} ---- {str(e)}')

            try:
                pe_ord = get_order(client, ord_book, pe_short)
                if pe_ord is not None:
                    pe_price = round(float(pe_ord['averageprice']), 1)
                    lock.acquire()                     
                    if pe_price != 0:
                        clients[i]['pe_entry_price'] = pe_price
                        if global_pe_price is None:
                            global_pe_price = float(pe_price)
                    else:
                        print(f'{clients[i]["user"]} -- pe_price = 0')
                    lock.release()
                else:
                    print(f'{clients[i]["user"]} -- pe_ord = None')
            except Exception as e:
                logging.info(f'{clients[i]["user"]} ---- {str(e)}')  
            
                # clients[i]['orders']['pe_short'] = pe_short
                # del clients[i]['pe_sl']
    
        else:
            ce_price, pe_price = get_entry_prices(client, instruments['short']['CE']['symbol'],\
                                    instruments['short']['PE']['symbol'])
            clients[i]['ce_entry_price'] = ce_price
            clients[i]['pe_entry_price'] = pe_price
            lock.acquire()             
            if global_ce_price is None or global_pe_price is None:
                global_ce_price = float(ce_price)
                global_pe_price = float(pe_price)
            lock.release()
            print(ce_price, '    ce_pe     ',pe_price)            
    except Exception as e:
        logging.info(f"Error = {str(e)}")
        print(str(e))

    # clients[i]['instruments'] = instruments      
    
# orders = client.orderBook()

def check_entry(client, instruments, i, qty, lot_size):
    global clients, STOPLOSS, global_ce_price, global_pe_price
    
    sleep(2)
    error_flag = False
    try:
        open_pos = get_open_positions(client)
    except Exception as e:
        logging.info(str(e))
        orders = call_order(client)
        if orders['data'] is None:
            open_pos = []
        else:
            open_pos = position_from_orders(orders)
    
    s = [ins['long']['CE']['symbol'], ins['long']['PE']['symbol'],\
                    ins['short']['CE']['symbol'], ins['short']['PE']['symbol']]
    
    sleep(1.5)
    
    try:
        cancel_open_orders(i)
    except:
        error_flag = True
        logging.info(traceback.format_exc())
    
    sleep(1.2)
    
    if 'init_entry' in list(clients[i].keys()):
        if clients[i]['init_entry'] == True:
            if error_flag:
                try:
                    cancel_open_orders(i)
                except:
                    sleep(2)
                    try:
                        cancel_open_orders(i)
                    except:
                        logging.info(traceback.format_exc())
            count = 0
            while True:
                if count >= 4:
                    break
                print(f'Checking Entries for client {i} : {clients[i]["user"]}')
                
                if instruments['long']['CE']['symbol'] not in open_pos:
                    
                    if count <= 2:
                        try:
                            client.cancelOrder(clients[i]['orders']['ce_long'], 'NORMAL')
                        except Exception as e:
                            logging.info(f'check_entry cancel order client_{i}  ----  {str(e)}')
                        try:
                            ce_long = place_order(client, instruments['long']['CE']['symbol'],\
                                    instruments['long']['CE']['token'], int(qty*lot_size), 'BUY', 0)
                        except Exception as e:
                            logging.info(f'{clients[i]["user"]} --  ce_long  -- {str(e)}')
                            
                    elif count <= 4 and count > 2:
                        try:
                            client.cancelOrder(clients[i]['orders']['ce_long'], 'NORMAL')
                        except Exception as e:
                            logging.info(f'check_entry cancel order client_{i}  ----  {str(e)}')
                        
                        ce_long_price = angel_ltp(client, instruments['long']['CE']['symbol'], instruments['long']['CE']['token'])+1
                        # pe_long_price = angel_ltp(instruments['long']['PE']['symbol'], instruments['long']['PE']['token'])+4
                        
                        try:
                            ce_long = place_order(client, instruments['long']['CE']['symbol'],\
                                                   instruments['long']['CE']['token'], int(qty*lot_size), 'BUY', ce_long_price)
                            clients[i]['orders']['ce_long'] = ce_long
                        except Exception as e:
                            logging.info(f'{clients[i]["user"]} --  ce_long  -- {str(e)}')
                    
                    
                    # pe_long = place_buy_limit(client, instruments['long']['PE']['symbol'],\
                    #                        instruments['long']['PE']['token'], qty, pe_long_price)
                    
                                    
                    sleep(1)
                
                if instruments['long']['PE']['symbol'] not in open_pos:
                    if count <= 2:
                        try:
                            client.cancelOrder(clients[i]['orders']['pe_long'], 'NORMAL')
                        except Exception as e:
                            logging.info(f'check_entry cancel order client_{i}  ----  {str(e)}')
                        try:
                            ce_long = place_order(client, instruments['long']['PE']['symbol'],\
                                    instruments['long']['PE']['token'], int(qty*lot_size), 'BUY', 0)
                        except Exception as e:
                            logging.info(f'{clients[i]["user"]} --  ce_long  -- {str(e)}')                    
                    elif count <= 4 and count > 2:
                        try:
                            client.cancelOrder(clients[i]['orders']['pe_long'], 'NORMAL')
                        except Exception as e:
                            logging.info(f'check_entry cancel order client_{i}  ----  {str(e)}')
                            
                        pe_long_price = angel_ltp(client, instruments['long']['PE']['symbol'], instruments['long']['PE']['token'])+1
                        
                        try:
                            pe_long = place_order(client, instruments['long']['PE']['symbol'],\
                                                    instruments['long']['PE']['token'], int(qty*lot_size), "BUY", pe_long_price)
                            clients[i]['orders']['pe_long'] = pe_long
                        except Exception as e:
                            logging.info(f'{clients[i]["user"]} --  pe_long  -- {str(e)}')
                    sleep(1)
                
                if instruments['short']['CE']['symbol'] not in open_pos:
                    try:
                        client.cancelOrder(clients[i]['orders']['ce_short'], 'NORMAL')
                    except Exception as e:
                        logging.info(f'check_entry cancel order client_{i}  ----  {str(e)}')                
                    
                    ce_short_price = angel_ltp(client, instruments['short']['CE']['symbol'], ins['short']['CE']['token'])-1.5
                    # pe_short_price = angel_ltp(ins['short']['PE']['symbol'], ins['short']['PE']['token'])-4
                    
                    if ce_short_price <= 0:
                        ce_short_price = 0.5
                    # if pe_short_price <= 0:
                    #     pe_short_price = 0.5
                    try:
                        ce_short = place_order(client, instruments['short']['CE']['symbol'],\
                                               instruments['short']['CE']['token'], int(qty*lot_size), 'SELL', 0)
                    
                        clients[i]['orders']['ce_short'] = ce_short
                    except Exception as e:
                        logging.info(f'{clients[i]["user"]} --  ce_short  -- {str(e)}')                  
                    sleep(1)
                    
                    try:
                        ce_ord = get_order_call_orders(client, ce_short)
                        if ce_ord is not None:
                            ce_price = round(float(ce_ord['averageprice']), 1)
                            lock.acquire()
                            if ce_price != 0:
                                clients[i]['ce_entry_price'] = ce_price 
                                if global_ce_price is None:
                                    global_ce_price = float(ce_price)                              
                            else:
                                print(f'{clients[i]["user"]} -- ce_price = 0')
                               
                            lock.release()
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
                    
                    pe_short_price = angel_ltp(client, instruments['short']['PE']['symbol'], ins['short']['PE']['token'])-1.5
                    if pe_short_price <= 0:
                        pe_short_price = 0.5
                    
                    try:
                        pe_short = place_order(client, instruments['short']['PE']['symbol'],\
                                               instruments['short']['PE']['token'], int(qty*lot_size), 'SELL', 0)
                        
                        clients[i]['orders']['pe_short'] = pe_short
                    except Exception as e:
                        logging.info(f'{clients[i]["user"]} --  pe_short  -- {str(e)}')                    
                    sleep(1)
                    
                    try:
                        pe_ord = get_order_call_orders(client, pe_short)
                        if pe_ord is not None:
                            pe_price = round(float(pe_ord['averageprice']), 1)
                            lock.acquire()                            
                            if pe_price != 0 :
                                clients[i]['pe_entry_price'] = pe_price
                                if global_pe_price is None:
                                    global_pe_price = float(pe_price)
                            else:
                                print(f'{clients[i]["user"]} -- pe_price = 0')
                            lock.release()                                
                        else:
                            print(f'{clients[i]["user"]} -- pe_ord = None')                               
                    except Exception as e:
                        logging.info(f'{clients[i]["user"]} ---- {str(e)}')                
                    sleep(1)
                sleep(1.5)
                try:
                    open_pos = get_open_positions(client)
                except Exception as e:
                    logging.info(str(e))
                    orders = call_order(client)
                    open_pos = position_from_orders(orders)
                if s[0] in open_pos and s[1] in open_pos and s[2] in open_pos and s[3] in open_pos:
                    break
                sleep(1)
                count+=1
    

def get_open_qty(client, token):
    #Positive Quantity for Short position
    try:
        positions = client.position()['data']
    except:
        sleep(1)
        orders = call_order(client)
        positions = pos_list_from_orders(orders)
    if positions is None:
        return None
    for pos in positions:
        if int(pos['symboltoken']) == int(token) and pos['producttype']=='INTRADAY':
            return int(int(pos['sellqty'])-int(pos['buyqty']))
    return None

def open_qty_positions(token, positions):
    if positions is None:
        return None
    for pos in positions:
        if int(pos['symboltoken']) == int(token) and pos['producttype']=='INTRADAY':
            return int(int(pos['sellqty'])-int(pos['buyqty']))
    return None

    
def get_open_positions(client):
    try:
        positions = client.position()['data']
    except:
        sleep(1)
        orders = call_order(client)
        positions = pos_list_from_orders(orders)    
    open_pos = []
    if positions is None:
        return open_pos
    for pos in positions:
        if abs(int(pos['sellqty'])-int(pos['buyqty'])) != 0 and pos['producttype']=='INTRADAY':
            open_pos.append(pos['tradingsymbol'])
    
    return open_pos

def get_open_pos_from_positions(positions):
    open_pos = []
    if positions is None:
        return open_pos
    for pos in positions:
        if abs(int(pos['sellqty'])-int(pos['buyqty'])) != 0 and pos['producttype']=='INTRADAY':
            open_pos.append(pos['tradingsymbol'])
    
    return open_pos        


def exit_single(symbol, token, open_positions, positions, symbol_dict, client):
    global logging
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
                        price = round(((float(angel_ltp(client, symbol, token)))+2), 1)
                        ord_id = place_buy_limit(client, symbol, int(token), qty, price)
                    except Exception as e:
                        logging.info(str(e))
                        print(str(e))
                elif qty < 0:
                    qty = abs(qty)
                    try:
                        # price = round(((float(angel_ltp(client, symbol, token)))-1), 1)
                        # if price < 0.5:
                        #     price = 0.5
                        ord_id = place_order(client, symbol, int(token), qty, 'SELL', 0)
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
    flag = False
    sleep(1.3)
    try:
        orders = call_order(client)
        positions = pos_list_from_orders(orders)
        sleep(1)
        flag = True        
    except Exception as e:
        logging.info(str(e))
        sleep(1.5)
        try:
            pos_call = client.position()
            positions = pos_call['data']
            if positions is None:
                logging.info(pos_call)
                raise Exception('Positions is None')
        except Exception as e:
            logging.info(str(e))
            orders = call_order(client)
            positions = pos_list_from_orders(orders)
            flag = True            


    if flag:
        try:
            cancel_open_orders_from_orders(i, orders['data'])
        except Exception as e:
            logging.info(traceback.format_exc())            
    else:
        sleep(1)
        try:
            cancel_open_orders(i)
        except Exception as e:
            logging.info(traceback.format_exc())
    print(client.userId)
    if positions is not None:
    
        strategy_pos = {ins['long']['CE']['symbol']:[ins['long']['CE']['fyers_sym'], int(ins['long']['CE']['token'])],\
                        ins['long']['PE']['symbol']:[ins['long']['PE']['fyers_sym'], int(ins['long']['PE']['token'])],\
                        ins['short']['CE']['symbol']:[ins['short']['CE']['fyers_sym'], int(ins['short']['CE']['token'])],\
                        ins['short']['PE']['symbol']:[ins['short']['PE']['fyers_sym'], int(ins['short']['PE']['token'])]}
        # get_ltp(ins['long']['PE']['fyers_sym'])
        # sleep(1)
        # count = 0
        open_pos = get_open_pos_from_positions(positions)
        # for pos in strategy_pos:
        exit_single(ins['short']['CE']['symbol'], int(ins['short']['CE']['token']), open_pos, positions, strategy_pos, client)
        exit_single(ins['short']['PE']['symbol'], int(ins['short']['PE']['token']), open_pos, positions, strategy_pos, client)
        exit_single(ins['long']['CE']['symbol'], int(ins['long']['CE']['token']), open_pos, positions, strategy_pos, client)
        exit_single(ins['long']['PE']['symbol'], int(ins['long']['PE']['token']), open_pos, positions, strategy_pos, client)
        
                        # continue
                # sleep(2)
        sleep(4)
        # orders = call_order(client)
        # sleep(2)
        # pos_symbols = list(strategy_pos.keys())
        # for p in pos_symbols:
        #     ord1 = get_order(client, orders, strategy_pos[p])
        #     if ord1['status'] == 'rejected' and re.match('zero',ord1['text']):
                
        count = 0
        if flag:
            try:
                cancel_open_orders_from_orders(i, orders['data'])
            except:
                logging.info(traceback.format_exc())
        else:
            try:
                cancel_open_orders(i)
            except:
                sleep(1.1)
                try:
                    cancel_open_orders(i)
                except:
                    logging.info(traceback.format_exc())
        sleep(1)
        while True:
            if count >= 5:
                break
            try:
                orders = call_order(client)
                open_pos = position_from_orders(orders)           
            except Exception as e:
                logging.info(str(e))
                sleep(1.1)
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


def exit_short_positions(i, ins):
    global clients
    
    # ins = clients[i]['instruments']
    client = clients[i]['client']
    flag = False
    sleep(1)
    try:
        orders = call_order(client)
        positions = pos_list_from_orders(orders)
        flag = True        
    except Exception as e:
        logging.info(str(e))
        sleep(1.5)
        try:
            positions = client.position()['data']
        except Exception as e:
            logging.info(str(e))
            try:
                sleep(1.2)
                orders = call_order(client)
                positions = pos_list_from_orders(orders)
                flag = True                
            except Exception as e:
                logging.info(str(e))
    
    if flag:
        try:
            cancel_open_orders_from_orders(i, orders['data'])
        except Exception as e:
            logging.info(traceback.format_exc())
    else:
        sleep(1)
        try:
            cancel_open_orders(i)
        except Exception as e:
            logging.info(traceback.format_exc())        
        
    print(client.userId)
    if positions is not None:
    
        strategy_pos = {ins['short']['CE']['symbol']:[ins['short']['CE']['fyers_sym'], int(ins['short']['CE']['token'])],\
                        ins['short']['PE']['symbol']:[ins['short']['PE']['fyers_sym'], int(ins['short']['PE']['token'])]}
        # get_ltp(ins['long']['PE']['fyers_sym'])
        # sleep(1)
        # count = 0
        open_pos = get_open_pos_from_positions(positions)
        # for pos in strategy_pos:
        exit_single(ins['short']['CE']['symbol'], int(ins['short']['CE']['token']), open_pos, positions, strategy_pos, client)
        exit_single(ins['short']['PE']['symbol'], int(ins['short']['PE']['token']), open_pos, positions, strategy_pos, client)
        
                        # continue
                # sleep(2)
        sleep(2)
        
        if flag:
            cancel_open_orders_from_orders(i, orders['data'])
        else: 
            try:
                cancel_open_orders(i)
            except:
                logging.info(traceback.format_exc())
        sleep(1)
        count = 0
        while True:
            if count >= 5:
                break
            try:
                try:
                    orders = call_order(client)
                except Exception as e:
                    logging.info(str(e))
                    continue
                open_pos = position_from_orders(orders)                
            except:
                sleep(1.1)
                open_pos = get_open_positions(client)                
                        
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
            
            if ins['short']['CE']['symbol'] not in open_pos and ins['short']['PE']['symbol'] not in open_pos:
                    print(client.userId,'     exited and  ---  breaking')
                    break
            count+=1
            sleep(2)
                
        print(client.userId,'     exited')        
        
        


def exit_leg_check(client, i, symbol, token, ord_id):
    try:
        orders = call_order(client)
        positions = pos_list_from_orders(orders)
    except:
        sleep(1)
        positions = client.position()['data']
    qty = open_qty_positions(token, positions)
    if qty == 0 or qty is None:
        print(f"Already Exited {symbol}")
    else:
        sleep(1.2)
        try:
            cancel_open_orders(i)
        except:
            logging.info(traceback.format_exc())
        sleep(1)
        count = 0
        while count < 3:
            if count >= 3:
                break
            if count == 0:
                try:
                    client.cancelOrder(ord_id, 'NORMAL')
                except Exception as e:
                    logging.info(f'exit_leg_check order cancel error {str(e)}')
            try:
                ord_id = place_order(client, symbol, int(token), qty, 'BUY', 0)
            except Exception as e:
                logging.info(str(e))
                print(str(e))
            sleep(1)
            qty = get_open_qty(client, token)
            if int(qty) == 0:
                print(f"Exited {symbol} by exit_leg_check")
                break
            sleep(1.5)
            count+=1

def check_reentry(client, i, order_id, symbol, token, qty):
    count = 0
    count2 = 0
    short_id = str(order_id)
    sleep(1.2)
    try:
        cancel_open_orders(i)
    except Exception as e:
        logging.info(traceback.format_exc())        
    while count < 3:
        # positions = client.position()['data']
        try:
            open_pos = get_open_positions(client)
        except:
            sleep(1)
            try:
                orders = call_order(client)
            except Exception as e:
                logging.info(f"Check_reentry : {str(e)}")
                count2+=1
                continue
            open_pos = position_from_orders(orders)
        if count2 >= 3:
            break
        if symbol in open_pos:
            print(f'check_reentry {symbol} in open_pos')
            sleep(1)
            return short_id
            break
        elif symbol not in open_pos:
            if count == 0:
                try:
                    client.cancelOrder(order_id, 'NORMAL')
                    short_id = None
                except Exception as e:
                    logging.info(f'check_reentry cancel order {str(e)}')
                try:
                    short_id = place_order(client, symbol, int(token), int(qty), 'SELL', 0)
                except Exception as e:
                    logging.info(f'{clients[i]["user"]} --  reentry_short  -- {str(e)}')
            
            elif count == 1:
                short_price = round(float((angel_ltp(client, symbol, token)-0.5)), 1)
                if short_price <= 0:
                    short_price = 0.5
                try:
                    short_id = place_sell_limit(client, symbol,\
                                           token, int(qty), short_price)
                except Exception as e:
                    logging.info(f'{clients[i]["user"]} --  pe_short  -- {str(e)}')
            else:
                try:
                    client.cancelOrder(short_id, 'NORMAL')
                    short_id = None
                except Exception as e:
                    logging.info(f'check_reentry cancel order {str(e)}')                
                short_price = round(float((angel_ltp(client, symbol, token)-0.3)), 1)
                try:
                    short_id = place_order(client, symbol, int(token), int(qty), 'SELL', short_price)
                except Exception as e:
                    logging.info(f'{clients[i]["user"]} --  reentry_short  -- {str(e)}')
            count+=1
        sleep(1.5)
    return short_id


def new_check_entry(client, instruments, i, qty, lot_size, ce_short, pe_short):
    global clients, STOPLOSS
    # client = clients[3]['client']
    sleep(20)
    logging.info(f'Started new check reentry for client {i}')
    orders = None
    try:
        orders = call_order(client)
        # cancel_open_orders(i)
        cancel_open_orders_from_orders(i, orders['data'])
    except Exception as e:
        logging.info(traceback.format_exc())        
    sleep(1)    
    try:
        open_pos = get_open_positions(client)
    except:
        if orders is not None:
            try:
                open_pos = position_from_orders(orders)
            except:
                logging.info(traceback.format_exc())
        else:
            sleep(1)
            orders = call_order(client)
            open_pos = position_from_orders(orders)
        
    
    logging.info(f"Open pos for Client {i} : {open_pos}")
    ret_ce_short = None
    ret_pe_short = None
    s = [ins['short']['CE']['symbol'], ins['short']['PE']['symbol']]
    count = 0
    while True:
        if count >= 4:
            break
        print(f'Checking Entries for client {i} : {clients[i]["user"]}')
        if instruments['short']['CE']['symbol'] not in open_pos:
            try:
                client.cancelOrder(ce_short, 'NORMAL')
            except Exception as e:
                logging.info(f'check_entry cancel order client_{i}  ----  {str(e)}')                
            
            # ce_short_price = angel_ltp(client, instruments['short']['CE']['symbol'], ins['short']['CE']['token'])-1.5
            # pe_short_price = angel_ltp(ins['short']['PE']['symbol'], ins['short']['PE']['token'])-4
            
            # if ce_short_price <= 0:
            #     ce_short_price = 0.5
            # if pe_short_price <= 0:
            #     pe_short_price = 0.5
            try:
                ce_short = place_order(client, instruments['short']['CE']['symbol'],\
                                       instruments['short']['CE']['token'], int(qty*lot_size), 'SELL', 0)
            
                ret_ce_short = ce_short
            except Exception as e:
                logging.info(f'{clients[i]["user"]} --  ce_short  -- {str(e)}')                               
            sleep(1)
        
        if instruments['short']['PE']['symbol'] not in open_pos:
            try:
                client.cancelOrder(clients[i]['orders']['pe_short'], 'NORMAL')
            except Exception as e:
                logging.info(f'check_entry cancel order client_{i}  ----  {str(e)}')       
            
            # pe_short_price = angel_ltp(client, instruments['short']['PE']['symbol'], ins['short']['PE']['token'])-1.5
            # if pe_short_price <= 0:
            #     pe_short_price = 0.5
            
            try:
                pe_short = place_order(client, instruments['short']['PE']['symbol'],\
                                       instruments['short']['PE']['token'], int(qty*lot_size), 'SELL', 0)
                
                ret_pe_short = pe_short
            except Exception as e:
                logging.info(f'{clients[i]["user"]} --  pe_short  -- {str(e)}')                    
            sleep(1)
            
        sleep(1)
        try:
            open_pos = get_open_positions(client)
        except Exception as e:
            logging.info(f"Position API error : {str(e)}")
            sleep(1)
            orders = call_order(client)
            open_pos = position_from_orders(orders)
        if s[0] in open_pos and s[1] in open_pos:
            break
        sleep(1)
        count+=1
    
    return ret_ce_short, ret_pe_short
                

def start_new_short_pos(i, ce_price, pe_price, ce_strike, pe_strike, lot_size, csv):
    global sheet3, ins, clients, deleted_clients, lock, reenter_flag
    
    #Enter the two short legs based on price entered.
    client = clients[i]['client']
    
    flag = False
    ce_ser = csv[csv['option']=='CE']
    pe_ser = csv[csv['option']=='PE']
    
    ce_short = closest_value(ce_ser, round(ce_price,1), 'CE', strike=ce_strike)

    pe_short = closest_value(pe_ser, round(pe_price,1), 'PE', strike=pe_strike)
    
    ce_symbol = ce_short['symbol']
    pe_symbol = pe_short['symbol']
    
    ce_token, ce_f = get_token_from_symbol(ce_symbol, csv)
    pe_token, pe_f = get_token_from_symbol(pe_symbol, csv)
    
    sleep(1.1)
    try:
        positions = client.position()['data']
    except Exception as e:
        logging.info(f"Reenter_leg {i} : {str(e)}")
        sleep(1.1)
        orders = call_order(client)
        positions = pos_list_from_orders(orders)
    init_positions = get_open_pos_from_positions(positions)
    sleep(1.1)
    
    try:
        if ce_symbol not in init_positions and pe_symbol not in init_positions:
            if i == 0:
                lock.acquire()
                ins['short']['CE']['symbol'] = ce_symbol
                ins['short']['CE']['token'] = ce_token
                ins['short']['CE']['fyers_sym'] = ce_f
                sheet3.update_cell(row+2, 5, ins['short']['CE']['symbol'])
                ins['short']['PE']['symbol'] = pe_symbol
                ins['short']['PE']['token'] = pe_token
                ins['short']['PE']['fyers_sym'] = pe_f
                sheet3.update_cell(row+2, 6, ins['short']['PE']['symbol'])
                lock.release()
                logging.info(f"sheet updated : ce_ymbol : {ce_symbol} ---- pe_symbol : {pe_symbol}")
                print('sheet updated')
                print(ins)
            ce_short_price = round(float((angel_ltp(client, ce_symbol, ce_token)-2)), 1)
            if ce_short_price <= 1:
                try:
                    ce_short = place_order(client, ce_symbol, ce_token,\
                                int(clients[i]['lots']*lot_size), 'SELL', 0)
                except Exception as e:
                    logging.info(f'{clients[i]["user"]} --  new_entry --- ce_short  -- {str(e)}')                        
            
            else:    
                try:
                    ce_short = place_sell_limit(client, ce_symbol,\
                                           ce_token, int(clients[i]['lots']*lot_size), ce_short_price)
                except Exception as e:
                    logging.info(f'{clients[i]["user"]} --- new_entry ---  ce_short  -- {str(e)}')
            
            pe_short_price = round(float((angel_ltp(client, pe_symbol, pe_token)-2)), 1)
            if pe_short_price <= 1:
                try:
                    pe_short = place_order(client, pe_symbol, pe_token,\
                                int(clients[i]['lots']*lot_size), 'SELL', 0)
                except Exception as e:
                    logging.info(f'{clients[i]["user"]} --  new_entry --- pe_short  -- {str(e)}')                        
            
            else:    
                try:
                    pe_short = place_sell_limit(client, pe_symbol,\
                                           pe_token, int(clients[i]['lots']*lot_size), ce_short_price)
                except Exception as e:
                    logging.info(f'{clients[i]["user"]} -- new_entry --  pe_short  -- {str(e)}')            
            flag = True
            
            # new_check_entry(client, instruments, i, qty, lot_size, ce_short, pe_short)
            sleep(3)
            new_ce, new_pe = new_check_entry(client, ins, i, clients[i]['lots'], lot_size, ce_short, pe_short)
            if new_ce is not None:
                ce_short = new_ce
            if new_pe is not None:
                pe_short = new_pe
                
        else:
            exit_positions(i, ins)
            logging.info(f"Removing Client {clients[i]['user']}")
            # clients.remove(clients[i])
            lock.acquire()
            deleted_clients.append(clients[i])
            lock.release()
            return
        
    except Exception as e:
        logging.info(f'new_entry client_{i}: {str(e)}')
        
    try:
        sleep(1.5)
        try:
            positions = get_open_positions(client)
        except:
            sleep(2)
            logging.info(f'new_reenter_client {i} ---- position API failed')
            orders = call_order(client)
            positions = position_from_orders(orders)
        logging.info(f'Client {i} ---- Positions : {positions}')
        if flag:
            sleep(1)
            if ce_symbol in positions and pe_symbol in positions:
                orders = call_order(client)
                ce_order = get_order(client, orders, ce_short)
                pe_order = get_order(client, orders, pe_short)                
                print(f'Reentry Leg : {ce_symbol} and {pe_symbol} in Open Positions')
                if ce_order is not None:
                    ce_entry_price = round(float(ce_order['averageprice']), 1)
                    if ce_entry_price != 0:
                        logging.info(f"client_{i}  --- reenter_price  =   {ce_entry_price}") 
                        clients[i]['ce_entry_price'] = ce_entry_price
                        clients[i]['orders']['ce_short'] = ce_short
                
                if pe_order is not None:
                    pe_entry_price = round(float(pe_order['averageprice']), 1)
                    if pe_entry_price != 0:
                        logging.info(f"client_{i}  --- reenter_price  =   {pe_entry_price}")
                        clients[i]['pe_entry_price'] = pe_entry_price
                        clients[i]['orders']['pe_short'] = pe_short
                        
                # lock.acquire()
                # if not flag:
                #     global
            
            else:
                # cancel_open_orders(i)
                # sleep(1.2)
                exit_positions(i, ins)
                logging.info(f"Removing Client {clients[i]['user']}")
                # clients.remove(clients[i])
                lock.acquire()
                deleted_clients.append(clients[i])
                lock.release()
    except Exception as e:
        logging.info(f"new_short_entry --- error in orders part : {str(e)}")
            
# abs(5-7)

def reenter_leg(i, client, ltp, csv, new_short, opt, lot_size, ce_strike, pe_strike):
    global sheet3, ins, clients, deleted_clients, lock
    
    
    # short_ltp = round(float(get_ltp(series['fyers_sym'])), 1)
    # short_ltp = round(float(angel_ltp(client, series['symbol'], series['token'])),1)
    
    # ser = csv[csv['option']==opt.upper()]
    # if opt.upper() == 'CE':
    #    new_short = closest_value(ser, round(ltp,1), opt, strike=ce_strike)
    # elif opt.upper() == 'PE':
    #    new_short = closest_value(ser, round(ltp,1), opt, strike=pe_strike)
    
    
    # ins['short']['CE']['symbol'] = ce_short['symbol']
    # ins['short']['CE']['token'], ins['short']['CE']['fyers_sym'] = get_token_from_symbol(ins['short']['CE']['symbol'], csv)
    # ins['short']['CE']['ltp'] = get_ltp(ins['short']['CE']['fyers_sym'])

    symbol = new_short['symbol']
    token, f_symbol = get_token_from_symbol(symbol, csv)
    cur_ltp = float(new_short['ltp'])
    logging.info(f"Open Symbol LTP: {ltp}       ---    {symbol} ltp : {cur_ltp}")
    if abs((ltp-cur_ltp)) > 0.3*ltp:
        exit_positions(i, ins)
        logging.info(f"Removing Client {clients[i]['user']}")
        # clients.remove(clients[i])
        lock.acquire()
        deleted_clients.append(clients[i])
        lock.release()
        return
        
    sleep(1.1)
    try:
        positions = client.position()['data']
    except Exception as e:
        logging.info(f"Reenter_leg {i} : {str(e)}")
        sleep(1.1)
        try:
            orders = call_order(client)
            positions = pos_list_from_orders(orders)
        except:
            sleep(1.1)
            orders = call_order(client)
            positions = pos_list_from_orders(orders)            
            logging.info(traceback.format_exc())
        
    init_positions = get_open_pos_from_positions(positions)
    sleep(1.1)
    
    try:
        if symbol not in init_positions:
            if i == 0:
                if opt.upper() == 'PE':
                    lock.acquire()
                    ins['short']['PE']['symbol'] = symbol
                    ins['short']['PE']['token'] = token
                    ins['short']['PE']['fyers_sym'] = f_symbol
                    sheet3.update_cell(row+2, 6, ins['short']['PE']['symbol'])
                    lock.release()
                    logging.info(f"sheet updated : Symbol : {symbol}")
                    print('sheet updated')
                    print(ins)
                elif opt.upper() == 'CE':
                    lock.acquire()
                    ins['short']['CE']['symbol'] = symbol
                    ins['short']['CE']['token'] = token
                    ins['short']['CE']['fyers_sym'] = f_symbol
                    sheet3.update_cell(row+2, 5, ins['short']['CE']['symbol'])
                    lock.release()
                    logging.info(f"sheet updated : Symbol : {symbol}")
                    print('sheet updated')
                    print(ins)           
            short_price = round(float((angel_ltp(client, symbol, token)-1)), 1)
            if short_price <= 0:
                short_price = 0.5
            
            try:
                short = place_sell_limit(client, symbol,\
                                       token, int(clients[i]['lots']*lot_size), short_price)
            except Exception as e:
                logging.info(f'{clients[i]["user"]} --  pe_short  -- {str(e)}')
            sleep(1.5)

            short = check_reentry(client, i, short, symbol, token, int(clients[i]['lots']*lot_size))
            if short is None:
                logging.info('reenter_leg short order id is None')
                print('reenter_leg short order id is None')
        else:
            open_qty = open_qty_positions(token, positions)
            if int(open_qty) < 0:
                logging.info(f'Nearest same as long leg : {symbol}')
                exit_positions(i, ins)
                logging.info(f"Removing Client {clients[i]['user']}")
                # clients.remove(clients[i])
                lock.acquire()
                deleted_clients.append(clients[i])
                lock.release()
                return
	
        sleep(1)
        try:
            positions = get_open_positions(client)
        except:
            sleep(1)
            orders = call_order(client)
            positions = position_from_orders(orders)
            
        sleep(1.5)
        # orders = call_order(client)
        try:
            order_det = get_order_call_orders(client, short)
        except:
            logging.info(traceback.format_exc())
            sleep(2)
            try:
                order_det = get_order_call_orders(client, short)
            except:
                sleep(2)
                order_det = get_order_call_orders(client, short)
            
        if symbol in positions:
            print(f'Reentry Leg : {symbol} in Open Positions')
            if order_det is not None:
                entry_price = round(float(order_det['averageprice']), 1)
                if entry_price != 0:
                    logging.info(f"client_{i}  --- reenter_price  =   {entry_price}")
                    if opt.upper() == 'CE':
                        clients[i]['ce_entry_price'] = entry_price
                        clients[i]['orders']['ce_short'] = short
                        
                    elif opt.upper() == 'PE':
                        clients[i]['pe_entry_price'] = entry_price
                        clients[i]['orders']['pe_short'] = short                  
                    
                else:
                    logging.info(f"Reenter_thread, price = 0, Symbol : {symbol}")
                    print(f"Reenter_thread, price = 0, Symbol : {symbol}")
            else:
                logging.info("Reenter_thread, order is None")
                print("Reenter_thread, order is None")
                
        else:
            logging.info(f"{symbol} not in Open Positions")
            print(f"{symbol} not in Open Positions")
            try:
                client.cancelOrder(order_det['orderid'], order_det['variety'])
            except Exception as e:
                logging.info(f"reenter_leg --- symbol not in pos ---Symbol : {symbol}--- cancel_order -- {str(e)}")
                print(f"reenter_leg --- symbol not in pos ---Symbol : {symbol}--- cancel_order -- {str(e)}")
            
            logging.info(f'Symbol not in positions even after placing orders : {symbol}')
            sleep(1)
            try:
                cancel_open_orders(i)
                # 4/0
            except Exception as e:
                # print(traceback.format_exc())
                logging.info(traceback.format_exc())
            # print('abce')
            sleep(1)
            try:
                exit_positions(i, ins)
            except Exception as e:
                logging.info(traceback.format_exc())
                sleep(2)
                try:
                    exit_positions(i, ins)
                except Exception as e:
                    logging.info(traceback.format_exc())
                    send_message(f"User : {clients[i]['user']}  -- Exit Order Error")
                    # return
                    
            logging.info(f"Removing Client {clients[i]['user']}")
            # clients.remove(clients[i])
            lock.acquire()
            deleted_clients.append(clients[i])
            lock.release()
            return
        
    except Exception as e:
        logging.info(f"reenter_leg main error : {str(e)}\n\n{traceback.format_exc()}")
        print(f"reenter_leg main error : {str(e)}")
            
        
    



def get_CE_PE(i, ce_series, pe_series, ce_ltp, pe_ltp):
    global clients
    global logging
    global STOPLOSS
    
    client = clients[i]['client']
    flag = False
    print(ce_series['ltp'],'    ',pe_series['ltp'])
    total_price = float(ce_series['ltp'])+float(pe_series['ltp'])
    
    if float(clients[i]['ce_entry_price'])==0.0 or float(clients[i]['pe_entry_price'])==0.0:
        raise Exception("CE or PE Order not placed")
        
    print(f'CE PE calculation  client {i}  CE  :',float(clients[i]['ce_entry_price']),'    PE  :',float(clients[i]['pe_entry_price']))
    lst = [(float(clients[i]['ce_entry_price'])-float(ce_series['ltp'])), (float(clients[i]['pe_entry_price'])-float(pe_series['ltp']))]
    index = lst.index(min(lst))
    print('Index of MIN : ',index)
    if index == 0:
        return 'CE'
    elif index == 1:
        return 'PE'








def reentry_thread(i, ce_series, pe_series, ce_close_series, pe_close_series, ce_ltp, pe_ltp, lot, instruments, csv, ce_strike, pe_strike):
    global clients
    global logging
    global STOPLOSS
    
    client = clients[i]['client']
    flag = False
    print(ce_series['ltp'],'    ',pe_series['ltp'])
    total_price = float(ce_series['ltp'])+float(pe_series['ltp'])
    
    if float(clients[i]['ce_entry_price'])==0.0 or float(clients[i]['pe_entry_price'])==0.0:
        print(float(clients[i]['ce_entry_price']),'   ',float(clients[i]['pe_entry_price']))
        raise Exception("CE or PE Order not placed")
    lst = [(float(clients[i]['ce_entry_price'])-float(ce_series['ltp'])), (float(clients[i]['pe_entry_price'])-float(pe_series['ltp']))]
    index = lst.index(min(lst))
    print(index)
    if total_price >= STOPLOSS:
        ex_oid = None
        sl = None
        if index == 0:
            try:
                # ser = csv[csv['option']=='CE']
                # short_ser = closest_value(ser, round(pe_ltp,1), 'CE', strike=ce_strike)
                # if short_ser['symbol'] == instruments['short']['CE']['symbol']:
                #     logging.info(f'Nearest : ')
                # close_series = closest_value(csv, round(pe_ltp, 1), 'CE', strike=ce_strike)

                if str(ce_close_series['symbol']) == str(instruments['short']['CE']['symbol']):
                    if i == 0:
                        send_message(f'Price > STOPLOSS. Nearest CE in open leg {str(ce_close_series["symbol"])}')
                    return
                try:
                    qty = int(get_open_qty(client, instruments['short']['CE']['token']))
                except Exception as e:
                    logging.info(traceback.format_exc())
                    try:
                        temp_orders = call_order(client)
                        temp_pos = pos_list_from_orders(temp_orders)
                    except:
                        temp_pos = client.position()['data']
                    qty = int(open_qty_positions(instruments['short']['CE']['token'], temp_pos))
                if qty != 0:
                    # price = round((float(get_ltp(instruments['short']['CE']['fyers_sym']))+3), 1)
                    price = round((float(angel_ltp(client, instruments['short']['CE']['symbol'], instruments['short']['CE']['token']))+2), 1)
                    ex_oid = place_buy_limit(client, instruments['short']['CE']['symbol'],\
                                    instruments['short']['CE']['token'], int(qty), price)
                    flag = True
                else:
                    send_message(f'Error positions with {clients[i]["user"]}')
            except Exception as e:
                print(f"reentry error : {str(e)}")
                logging.info(traceback.format_exc())
            
            if flag:
                sleep(1)           
                exit_leg_check(client, i, instruments['short']['CE']['symbol'], instruments['short']['CE']['token'], ex_oid)
                sleep(1.1)
                clients[i]['pe_entry_price'] = pe_ltp
                reenter_leg(i, client, pe_ltp, csv, ce_close_series, 'CE', lot, ce_strike, pe_strike)
                    
    
        elif index == 1:                       
            
            try:
                
                # close_series = closest_value(csv, round(ce_ltp, 1), 'PE', strike=pe_strike)

                if str(pe_close_series['symbol']) == str(instruments['short']['PE']['symbol']):
                    if i == 0:
                        send_message(f'Price > STOPLOSS. Nearest PE in open leg {str(pe_close_series["symbol"])}')
                    return
                
                try:
                    qty = int(get_open_qty(client, instruments['short']['PE']['token']))
                except Exception as e:
                    logging.info(traceback.format_exc())
                    try:
                        temp_orders = call_order(client)
                        temp_pos = pos_list_from_orders(temp_orders)
                    except:
                        temp_pos = client.position()
                    qty = int(open_qty_positions(instruments['short']['PE']['token'], temp_pos))                    
                if qty != 0:
                    # price = round((float(get_ltp(instruments['short']['PE']['fyers_sym']))+3), 1)
                    price = round((float(angel_ltp(client, instruments['short']['PE']['symbol'], instruments['short']['PE']['token']))+2), 1)
                    ex_oid = ex_oid = place_buy_limit(client, instruments['short']['PE']['symbol'],\
                                    instruments['short']['PE']['token'], int(qty), price)
                    flag = True
                else:
                    send_message(f'Error positions with {clients[i]["user"]}')                    
            except Exception as e:
                print(f"reentry error : {str(e)}")
                logging.info(traceback.format_exc())
            
            if flag:
                sleep(1)
                exit_leg_check(client, i, instruments['short']['PE']['symbol'], instruments['short']['PE']['token'], ex_oid)
                sleep(1.1)
                reenter_leg(i, client, ce_ltp, csv, pe_close_series, 'PE', lot, ce_strike, pe_strike)
        
        
        
    
    logging.info(f"{lst}\t{total_price}")
    

def cancel_open_orders(i):
    global clients, ins
    
    strategy_pos = [ins['long']['CE']['symbol'], ins['long']['PE']['symbol'], ins['short']['CE']['symbol'],\
                    ins['short']['PE']['symbol']]
    
    client = clients[i]['client']
    orders = call_order(client)['data']
    for order in orders:
        if (order['ordertype'] == 'STOPLOSS_LIMIT' or order['ordertype']=='LIMIT') and order['status']=='trigger pending' and\
            str(order['tradingsymbol']) in strategy_pos:
            client.cancelOrder(order['orderid'], order['variety'])

def cancel_open_orders_from_orders(i, orders):
    global clients, ins
    
    strategy_pos = [ins['long']['CE']['symbol'], ins['long']['PE']['symbol'], ins['short']['CE']['symbol'],\
                    ins['short']['PE']['symbol']]
    
    client = clients[i]['client']
    # orders = client.orderBook()['data']
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
    global client, ins
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




csv, index = get_angel_instruments()



def get_data(ce_strike, pe_strike, lot_size):
    global df, exit_time, stop_thread, sheet2, row, STOPLOSS, clients, ins, sheet3, csv, stoploss
    global global_ce_price, global_pe_price, reenter_flag, deleted_clients, target
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
                return
                break
            df = pd.DataFrame(data=sheet2.get_all_records())
            
            sheet_exit = dt.datetime.strptime(str(df['Exit_Time'].iloc[row]), '%H:%M').time()
            exit_time = dt.datetime.combine(dt.datetime.now().date(), sheet_exit)
            
            target = int(df['Target'].iloc[row])
            stoploss = float(df['Combined SL Percentage'].iloc[row])
            STOPLOSS = stoploss*(global_ce_price+global_pe_price)
                
 
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
                # sleep(1)
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
            
            if int(df['Exit_Reentry'].iloc[row]) == 1:
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
                # sleep(1)
                pos_exit = []
                for i in range(len(clients)):
                    pos_exit.append(
                        Thread
                        (
                            target = exit_short_positions,
                            args = (i, ins),
                            name = f"Position_exit_{i}"
                        )
                    )
                    pos_exit[-1].start()
                
                for thread in pos_exit:
                    thread.join()
                
                # sleep(45)
                # tel_thread = Thread(
                #         target = queue_position_alerts,
                #         args = [
                #                     'Exit ',
                #                     clients,
                #                     ins['short']['CE']['symbol'],
                #                     ins['short']['PE']['symbol'],
                #                     ins['long']['CE']['symbol'],
                #                     ins['long']['PE']['symbol'],
                #                     lot_size,
                #                     True,
                #                     2
                #                 ],
                #         name = 'Exit_telegram'
                #     )
                
                # tel_thread.start()
                # tel_thread.join()
                
                
                sheet2.update_cell(row+2, 12, 0)
                # sheet3.update_cell(row+2, 2, (dt.datetime.now()-dt.timedelta(days=1)).strftime('%d-%m-%Y'))
                
                # string = 'python straddle_combined_reentries.py '+str(row)+' '+PATH              

                # print(string)
                # subprocess.Popen(string)
                # sleep(10)
                # stop_thread = True
                # return
                df = pd.DataFrame(data=sheet2.get_all_records())
                new_premium = float(df['Short Preimium'].iloc[row])
                csv = add_ltp(csv)
                ce_short_ltp = new_premium
                
                pe_short_ltp = new_premium

                # start_new_short_pos(i, ce_price, pe_price, ce_strike, pe_strike, lot_size, csv)
                reenter_flag = False
                new_entry_threads = []
                for i in range(len(clients)):
                    new_entry_threads.append(
                            Thread(
                                    target = start_new_short_pos,
                                    args = [
                                                i,
                                                ce_short_ltp,
                                                pe_short_ltp,
                                                ce_strike,
                                                pe_strike,
                                                lot_size,
                                                csv
                                            ],
                                    name = f"new_short_{i}"
                                )
                        )
                    new_entry_threads[-1].start()
                
                for thread in new_entry_threads:
                    thread.join()
                
                active_clients = [c for c in clients if c not in deleted_clients]
                
                global_ce_price = float(active_clients[0]['ce_entry_price'])
                global_pe_price = float(active_clients[0]['pe_entry_price'])
                
                STOPLOSS = stoploss*(global_ce_price+global_pe_price)
                
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
                        name = 'Exit_telegram'
                    )
                
                tel_thread.start()
                tel_thread.join()                 
                # break
                # os._exit(1)
                pause_file = open('pause.txt','w')
                pause_file.write('0')
                pause_file.close()                
                if len(clients) == 0:
                    logging.info(f"Length of clients : {len(clients)}")
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
                # sleep(1)
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
                
                sheet3.update_cell(row+2, 2, (dt.datetime.now()-dt.timedelta(days=1)).strftime('%d-%m-%Y'))
                
                pause_file = open('pause.txt','w')
                pause_file.write('0')
                pause_file.close()  
                
                os._exit(1)            
                stop_thread = True
                
                
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
            sleep(3.0-((time.time()-start)%3.0))


# def clients_add_sl(i, ce_symbol, pe_symbol):
#     global clients
#     client = clients[i]['client']
#     print(client.getProfile(client.refresh_token))
#     sl_ord = get_sl_orders(client, ce_symbol, pe_symbol)
#     if sl_ord is not None:
#         clients[i]['sl_status'] = True
#         clients[i]['reentered'] = True




csv['index'] = ''
for i in range(len(csv)):
    x = csv.iloc[i]
    csv['index'].iloc[i] = x['name']+str(x['strike'])+str(x['option'])

csv.set_index('index', inplace=True)

csv['fyers_sym'] = fyers.ins['symbol']

# pos = clients[0]['client'].position()


# client_df = pd.DataFrame(client_sheet.get_all_records())

# client_users = [x['user'] for x in clients]

# for i in range(len(client_df)):
#     ser = client_df.iloc[i]
#     symbols = ser['symbol'].split(', ')
#     if name in symbols:
#         try:
#             if clients[]
            


try:
    sheet_date = dt.datetime.strptime(constant_sheet['DATE'].iloc[row], '%d-%m-%Y')
except:
    print('sheet date except')
    sheet_date = dt.datetime(2020, 5, 3)

if dt.datetime.now() < entry_time:
    time_920 = dt.datetime.combine(dt.datetime.now().date(), entry_time.time())
    to_sleep = (time_920-dt.datetime.now()).seconds
    print('Sleeping for',to_sleep)
    sleep(to_sleep)
    
    
client_df = pd.DataFrame(client_sheet.get_all_records())

for i in range(len(clients)):
    clients[i]['lots'] = int(client_df[((client_df['username']==clients[i]['user']) & \
                    (client_df['symbol']==name) & (client_df['lots']!=''))]['lots'])
    print(clients[i]['user'],'-----',clients[i]['lots'])

# ins = {}
# 
if sheet_date.date() == dt.datetime.now().date():
    print('fetching from sheet')
    ins['short'] = {}
    ins['short']['CE'] = {}
    ins['short']['CE']['symbol'] = str(constant_sheet['CE_SHORT'].iloc[row])
    print('step 0\n',ins)
    ins['short']['CE']['token'], ins['short']['CE']['fyers_sym'] = get_token_from_symbol(ins['short']['CE']['symbol'], csv)
    try:
        ins['short']['CE']['ltp'] = get_ltp(ins['short']['CE']['fyers_sym'])
    except Exception as e:
        logging.info(str(e))
        logging.info(traceback.format_exc())
        ins['short']['CE']['ltp'] = angel_ltp(clients[0]['client'], ins['short']['CE']['symbol'], ins['short']['CE']['token'])
    
    ins['short']['PE'] = {}
    ins['short']['PE']['symbol'] = str(constant_sheet['PE_SHORT'].iloc[row])
    ins['short']['PE']['token'], ins['short']['PE']['fyers_sym'] = get_token_from_symbol(ins['short']['PE']['symbol'], csv)
    try:
        ins['short']['PE']['ltp'] = get_ltp(ins['short']['PE']['fyers_sym'])
    except Exception as e:
        logging.info(str(e))
        logging.info(traceback.format_exc())
        ins['short']['PE']['ltp'] = angel_ltp(clients[0]['client'], ins['short']['PE']['symbol'], ins['short']['PE']['token'])
    
    ins['long'] = {}
    ins['long']['CE'] = {}
    ins['long']['CE']['symbol'] = str(constant_sheet['CE_LONG'].iloc[row])
    ins['long']['CE']['token'], ins['long']['CE']['fyers_sym'] = get_token_from_symbol(ins['long']['CE']['symbol'], csv)
    
    ins['long']['PE'] = {}
    ins['long']['PE']['symbol'] = str(constant_sheet['PE_LONG'].iloc[row])
    ins['long']['PE']['token'], ins['long']['PE']['fyers_sym'] = get_token_from_symbol(ins['long']['PE']['symbol'], csv)
    
    lot_size = int(csv[csv['symbol']==ins['long']['CE']['symbol']]['lotsize'].iloc[0])

    ce_strike = int(csv[csv['symbol']==ins['long']['CE']['symbol']]['strike'])
    pe_strike = int(csv[csv['symbol']==ins['long']['PE']['symbol']]['strike'])

    print('step 1')
    # sl_ord_threads = []
    for i in range(len(clients)):
        clients[i]['orders'] = {}
    #     sl_ord_threads.append(
    #             Thread(
    #                     target = clients_add_sl,
    #                     args = [
    #                                 i,
    #                                 ins['short']['CE']['symbol'],
    #                                 ins['short']['PE']['symbol']
    #                             ],
    #                     name = f'add_sl_client{i}'
    #                 )
    #         )
    #     sl_ord_threads[-1].start()
    
    # print('threads started')
    # for thread in sl_ord_threads:
    #     thread.join()
    print(ins)

else:    
    start = time.time()
    csv = add_ltp(csv)
    print(time.time()-start)
    
    ce_df = csv[csv['option']=='CE']
    pe_df = csv[csv['option']=='PE']
    
    ce_buy = closest_value(ce_df, long_premium, 'CE', points=points )
    pe_buy = closest_value(pe_df, long_premium, 'PE', points=points)

    ce_strike =  int(ce_buy['strike'])
    pe_strike = int(pe_buy['strike'])
    
    ce_short = closest_value(ce_df, short_premium, 'CE', strike=int(ce_buy['strike']))
    pe_short = closest_value(pe_df, short_premium, 'PE', strike=int(pe_buy['strike']))
    
    
    
    
    ins['short'] = {}
    ins['short']['CE'] = {}
    ins['short']['CE']['symbol'] = ce_short['symbol']
    ins['short']['CE']['token'], ins['short']['CE']['fyers_sym'] = get_token_from_symbol(ins['short']['CE']['symbol'], csv)
    try:
        ins['short']['CE']['ltp'] = get_ltp(ins['short']['CE']['fyers_sym'])
    except:
        ins['short']['CE']['ltp'] = angel_ltp(angel, ins['short']['CE']['symbol'], ins['short']['CE']['token'])
    
    ins['short']['PE'] = {}
    ins['short']['PE']['symbol'] = pe_short['symbol']
    ins['short']['PE']['token'], ins['short']['PE']['fyers_sym'] = get_token_from_symbol(ins['short']['PE']['symbol'], csv)
    try:
        ins['short']['PE']['ltp'] = get_ltp(ins['short']['PE']['fyers_sym'])
    except:
        ins['short']['PE']['ltp'] = angel_ltp(angel, ins['short']['PE']['symbol'], ins['short']['PE']['token'])
    
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

# ins['lot_size'] = int(ce_buy['lotsize'])
# clients[0]['orders']['pe_sl'] = '221101000184167'
# fyers.client.get_profile()
logging.info(f'{clients}')
logging.info(f'{ins}')

pause_file = open('pause.txt','r')
val = int(pause_file.read())
pause_file.close()

if val != 0:
    while True:
        temp_pause = open('pause.txt','r')
        temp_val = int(temp_pause.read())
        temp_pause.close()
        print('main sleeping')
        if temp_val == 0:
            break
        sleep(3)  


pause_file = open('pause.txt','w')
pause_file.write('1')
pause_file.close()

# os._exit(1)

entry_threads = []
for i in range(len(clients)):
    entry_threads.append(
        Thread(
            target = init_order_thread,
            args=[
                clients[i]['client'],
                ins,
                int(clients[i]['lots'])*lot_size,
                i
                ],
            name = f'Initial_Short_client_{i}'
            )
        )
    entry_threads[-1].start()
    
for thread in entry_threads:
    thread.join()

sleep(10)

order_cancel_threads = []
for i in range(len(clients)):
    order_cancel_threads.append(
            Thread(
                    target = cancel_open_orders,
                    args = [i],
                    name = f'Cancel_orders_{i}'
                )
        )
    order_cancel_threads[-1].start()
# cancel_open_orders()
for thread in order_cancel_threads:
    thread.join()

sleep(1)

# constant_sheet.iloc[-10:]

error_threads = []
for i in range(len(clients)):
    error_threads.append(
        Thread(
            target = check_entry,
            args = [
                    clients[i]['client'],
                    ins,
                    i,
                    clients[i]['lots'],
                    lot_size
                    ],
            name = f'Entry_error_{i}'
            )
        )
    error_threads[-1].start()

for thread in error_threads:
    thread.join()

sleep(1)
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
                    2
                ],
        name = 'Entry_telegram'
    )

tel_thread.start()
tel_thread.join()

sleep(1.4)

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

sleep(1)

remove_clients = []


rem_threads = []


for r in total_rows:
    if r['short_ce'] != 0 or r['short_pe'] != 0 or r['long_ce'] != 0 or\
        r['long_pe'] != 0:
            print(f'Removing {r["Client"]}')
                
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
        args = [ce_strike, pe_strike, lot_size]
    )

data_thread.start()

sleep(7.5)

start = time.time()

# reentry_thread(i, ce_series, pe_series, lot, instruments, csv)

while dt.datetime.now() < dt.datetime.combine(dt.datetime.now().date(), dt.time(15,40)):
    if stop_thread is True:
        logging.info("Stop Thread is True")
        break    
    try:
        pause_file = open('pause.txt','r')
        val = int(pause_file.read())
        pause_file.close()
        if val == 0:        
            try:
                try:
                    angel = clients[random.randint(0, (len(clients)-1))]['client']
                    ins['short']['CE']['ltp'] = angel_ltp(angel, ins['short']['CE']['symbol'], ins['short']['CE']['token'])
                    ins['short']['PE']['ltp'] = angel_ltp(angel, ins['short']['PE']['symbol'], ins['short']['PE']['token'])
                except:
                    ins['short']['CE']['ltp'], ins['short']['PE']['ltp'] = \
                        fyers.running_ltp(ins['short']['CE']['fyers_sym'], ins['short']['PE']['fyers_sym'])
                # csv = add_ltp(csv)
                # ce_df = csv[csv['option']=='CE']
                # pe_df = csv[csv['option']=='PE']
                
                ce_short = ins['short']['CE']
                pe_short = ins['short']['PE']
            except Exception as e:
                logging.info(f"Exception in adding LTPs : {str(e)}")
                continue
            
            if len(deleted_clients) != 0:
                for j in range(len(deleted_clients)):
                    if deleted_clients[j] in clients:
                        try:
                            logging.info(f"Removing client - {deleted_clients[j]['user']}")
                            clients.remove(deleted_clients[j])
                        except:
                            logging.info(f'Couldnt delete client {j}')
                deleted_clients = []
            
            if len(clients) == 0:
                logging.info(f"clients list empty. {clients}")
                os._exit(1)
            if not stoploss_flag or (global_ce_price is None or global_pe_price is None):
                global_ce_price = float(clients[0]['ce_entry_price'])
                global_pe_price = float(clients[0]['pe_entry_price'])
                STOPLOSS = stoploss*(global_ce_price+global_pe_price)
                stoploss_flag = True
            
            total_price = float(ce_short['ltp'])+float(pe_short['ltp'])
            print(total_price)
            if total_price <= ((global_ce_price+global_ce_price)-target):
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
                # sleep(1)
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
            
            if total_price >= STOPLOSS:
                pause_file = open('pause.txt', 'w')
                pause_file.write('1')
                pause_file.close()
                csv = add_ltp(csv)
                ce_short_ltp = round(float(angel_ltp(clients[random.randint(0, (len(clients)-1))]['client']\
                    , ins['short']['CE']['symbol'], ins['short']['CE']['token'])),1)
                
                pe_short_ltp = round(float(angel_ltp(clients[random.randint(0, (len(clients)-1))]['client']\
                    , ins['short']['PE']['symbol'], ins['short']['PE']['token'])),1)
                ce_close_series = closest_value(csv, round(pe_short_ltp, 1), 'CE', strike=ce_strike)
                ce_close_series['ltp'] = angel_ltp(clients[random.randint(0, (len(clients)-1))]['client']\
                        , ce_close_series['symbol'], ce_close_series['token'])
                pe_close_series = closest_value(csv, round(ce_short_ltp, 1), 'PE', strike=pe_strike)
                pe_close_series['ltp'] = angel_ltp(clients[random.randint(0, (len(clients)-1))]['client']\
                        , pe_close_series['symbol'], pe_close_series['token'])
                try:
                    sl_option = get_CE_PE(0, ce_short, pe_short, ce_short_ltp, pe_short_ltp)
                except:
                    logging.info(traceback.format_exc())
                    sl_option = get_CE_PE(1, ce_short, pe_short, ce_short_ltp, pe_short_ltp)
                stoploss_flag = False
                reentry_list = []
                for i in range(len(clients)):
                    reentry_list.append(
                            Thread(
                                    target = reentry_thread,
                                    args = [
                                                i,
                                                ce_short,
                                                pe_short,
                                                ce_close_series,
                                                pe_close_series,
                                                ce_short_ltp,
                                                pe_short_ltp,
                                                lot_size,
                                                ins,
                                                csv,
                                                ce_strike,
                                                pe_strike
                                            ],
                                    name = f'reentry_client_{i}'
                                )
                        )
                    reentry_list[-1].start()
                
                for thread in reentry_list:
                    thread.join()
                
                # if stoploss_flag:
                #     global_ce_price = float(clients[0]['ce_entry_price'])
                #     global_pe_price = float(clients[0]['pe_entry_price'])
                #     STOPLOSS = stoploss*(global_ce_price+global_pe_price)
                
                # else:
                #     global_ce_price = float(clients[1]['ce_entry_price'])
                #     global_pe_price = float(clients[1]['pe_entry_price'])
                #     STOPLOSS = stoploss*(global_ce_price+global_pe_price)
                #     stoploss_flag = True

                pause_file = open('pause.txt', 'w')
                pause_file.write('0')
                pause_file.close()

        else:
            while True:
                print('val is not 0')
                temp_pause = open('pause.txt', 'r')
                temp = int(temp_pause.read())
                if temp == 0:
                    break
                temp_pause.close()
                sleep(1.5)
            continue
    except KeyboardInterrupt as e:
        stop_thread = True
        os._exit(1)
        data_thread.join()
        
        
    if name.upper() == 'NIFTY':
        sleep(1.8-((time.time()-start)%1.8))
    elif name.upper() == 'BANKNIFTY':
        sleep(3-((time.time()-start)%3.0))
    else:
        sleep(2.5-((time.time()-start)%2.5))

data_thread.join()

