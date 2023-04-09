# -*- coding: utf-8 -*-
"""
Created on Mon Nov 21 23:10:53 2022

@author: akhil
"""

from datetime import datetime
import time
from functions import pos_list_from_orders
from telegram import Bot
from selenium import webdriver
import chromedriver_autoinstaller
import os

PATH = chromedriver_autoinstaller.install(cwd=True)
PATH = os.path.split(PATH)[0][-3:]+'/'+os.path.split(PATH)[1].rstrip('.exe')

def generate_position_alert(clients, short_ce, short_pe, long_ce,\
                        long_pe, lot_size, is_exit):
    
    dirs = {short_ce:'SELL', short_pe:'SELL', long_ce:'BUY', long_pe:'BUY'}
    rows = []
    for i in range(len(clients)):
        client = clients[i]
        # try:

        # Get positions for the client and extract open qty
        # m2m = 0
        short_ce_qty = 0
        short_pe_qty = 0
        long_ce_qty = 0
        long_pe_qty = 0
        try:
            positions = client['client'].position()['data']
        except:
            time.sleep(1.1)
            try:
                ord_data = client.orderBook()
            except:
                time.sleep(1.2)
                ord_data = client.orderBook()
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
            
            time.sleep(1)
            """orders = client["client"].orderBook()['data']
            
            for order in orders:
                # Match based on the symbol and product
                if order['tradingsymbol'] in dirs and order['producttype'] == 'INTRADAY':
                    # Then match based on trade direction
                    if dirs[order['tradingsymbol']] == order['transactiontype']:
                        # If we don't know the priority of a status, set overall
                        # status to unknown and break out of the loop
                        if order['status'] == 'trigger pending' or order['status'] == 'open':
                            dir_mult = 1 if order['transactiontype'] == 'BUY' else -1
                            if order['tradingsymbol'] == short_ce:
                                short_ord_qty_ce += int(order['unfilledshares'])*dir_mult
                            elif order['tradingsymbol'] == short_pe:
                                short_ord_qty_pe += int(order['unfilledshares'])*dir_mult
                            elif order['tradingsymbol'] == long_ce:
                                long_ord_qty_ce += int(order['unfilledshares'])*dir_mult  
                            elif order['tradingsymbol'] == long_pe:
                                long_ord_qty_pe += int(order['unfilledshares'])*dir_mult                           
                            else:
                                raise RuntimeError('this should be impossible')""" 
    
            # If we're exiting or the client is not enabled, expected qty is 0
            expected_qty = int(client["lots"]*lot_size)
            
            if is_exit:
                expected_qty = 0

            rows.append({
                # 'Client Name' : client.client_name,
                'Serial No'   : (i+1),
                'Client ID'   : client['user'],
                'Required Qty' :expected_qty,
                f'Short {short_ce} Rem' : int(expected_qty - abs(int(short_ce_qty)) - abs(int(short_ord_qty_ce))),
                f'Short {short_pe} Rem' : int(expected_qty - abs(int(short_pe_qty)) - abs(int(short_ord_qty_pe))),
                f'Long {long_ce} Rem'  : int(expected_qty - abs(int(long_ce_qty)) - abs(int(long_ord_qty_ce))),
                f'Long {long_pe} Rem'  :int(expected_qty - abs(int(long_pe_qty)) - abs(int(long_ord_qty_pe)))
            })
            
            # except Exception as e:
            #     print(f'Unable to generate positon alert for {client["Client"].user_id}: {e}')
    
    return rows
    

def generate_sl_alert(clients, symbol):
    rows = []
    for i in range(len(clients)):
        client = clients[i]
        # try:

        # Get positions for the client and extract open qty
        # m2m = 0
        qty = 0
        positions = client['client'].position()['data']
        if positions is not None:
            for pos in positions:
                if pos['tradingsymbol'] == symbol and pos['producttype'] == 'INTRADAY':
                    qty += (int(pos['sellqty'])-int(pos['buyqty']))
        
            expected_qty = 0
            rows.append({
                    'Serial No'     : (i+1),
                    'Client ID'     : client['user'],
                    'Required Qty'  : expected_qty,
                    f'SL Exit {symbol} Rem' : int(expected_qty) - abs(int(qty))
                })
    return rows
    
    
def format_alerts_as_text_table(alerts):
    if len(alerts) < 1:
        return 'No data'
    # Convert all data to text and add a dummy header row
    alerts = [{str(k): str(v) for k, v in alert.items()} for alert in alerts]
    alerts = [{key: key for key in alerts[0]}] + alerts
    # Calculate column widths
    col_widths = {
        k: max(len(alerts[i][k]) for i in range(len(alerts)))
        for k in alerts[0]
    }
    return '\n'.join(
        '  '.join(
            (col_widths[key] - len(val))*' ' + val
            for key, val in alert_row.items()
        )
        for alert_row in alerts
    )

STYLING = 'table,th,td{border:1px solid black;border-collapse:collapse}th,td{padding:8px}'
def format_alerts_as_html_table(alerts):
    if len(alerts) < 1:
        return 'No data'
    # Generate header row
    header = '<thead><tr>' + ''.join(
        f'<th>{key}</th>' for key in alerts[0].keys()
    ) + '</tr></thead>'
    # Generate data rows
    data = '<tbody>' + ''.join(
        '<tr>' + ''.join(f'<td>{val}</td>' for val in alert.values()) + '</tr>'
        for alert in alerts
    ) + '</tbody>'
    # Combine header and data and return result
    return f'<style>{STYLING}</style><table>{header}{data}</table>'

def generate_image_from_html(html):
    # Set webdriver options
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--headless')
    options.add_experimental_option('excludeSwitches', ['enable-logging'])

    # Create webdrivre and render the HTML
    driver = webdriver.Chrome(executable_path=PATH,options=options)
    driver.get('data:text/html;charset=utf-8,' + html)

    # Try to ensure that the table is loaded before resizing window
    time.sleep(1)

    # Resize window to actual size of content
    driver.set_window_size(
        driver.execute_script('return document.body.parentNode.scrollWidth'),
        driver.execute_script('return document.body.parentNode.scrollHeight'),
    )

    time.sleep(1)

    driver.set_window_size(
        driver.execute_script('return document.body.parentNode.scrollWidth'),
        driver.execute_script('return document.body.parentNode.scrollHeight'),
    )

    # Save window as png
    image = driver.get_screenshot_as_png()
    with open('temp.png', 'wb') as f:
        f.write(image)
    
    driver.close()

    return image



def queue_sl_alert(
    status_msg, clients, symbol,
    delay
):
    time.sleep(delay)
    print(f'Sending telegram alert for {status_msg}')
    alerts = generate_sl_alert(clients, symbol)
    table_html = format_alerts_as_html_table(alerts)
    telegram_alert(status_msg + 'alert', table_html, True)    


def queue_position_alerts(
    status_msg, clients, short_ce, 
    short_pe, long_ce, long_pe, 
    lot_size, is_exit, init_delay
):
    time.sleep(init_delay)
    # for n in range(repeats + 1):
    print(f'Sending telegram alert for {status_msg}')
    alerts = generate_position_alert(clients, short_ce, short_pe, long_ce,\
                                     long_pe, lot_size, is_exit)
    table_html = format_alerts_as_html_table(alerts)
    telegram_alert(status_msg + 'alert', table_html, True)
    
    

def send_message(msg):
    telegram_alert(msg, alert_msg=True)
    
def telegram_alert(title, body = '', as_web_image = False, alert_msg=False):
    TELEGRAM_TOKEN = ''
    TELEGRAM_CHAT_ID = -1001375231117
    TELEGRAM_NOTIFY = True
    
    if alert_msg is False:
        title = str(datetime.now()) + ' | ' + title
    else:
        title = '****  CRITICAL ALERT  ****'+'\n\n'+title

    
    if as_web_image:
        image = generate_image_from_html(body)
        if TELEGRAM_NOTIFY:
            Bot(token = TELEGRAM_TOKEN).send_photo(
                chat_id = TELEGRAM_CHAT_ID,
                caption = title,
                photo = image,
            )
        print('Sending telegram image:', title)
    else:
        msg_text = title + (body and '\n\n' + body)
        if TELEGRAM_NOTIFY:
            Bot(token = TELEGRAM_TOKEN).send_message(
                chat_id = TELEGRAM_CHAT_ID,
                text = msg_text,
            )
        print('Sending telegram message:', msg_text)