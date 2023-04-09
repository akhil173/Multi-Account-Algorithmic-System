# -*- coding: utf-8 -*-
"""
Created on Fri Nov 25 12:53:37 2022

@author: akhil
"""

from fyers_api import fyersModel
from fyers_api import accessToken
import config
import requests
import urllib.parse as urlparse
import pandas as pd
import numpy as np
import pyotp
import datetime as dt
import os
import re
from time import sleep

from selenium import webdriver
#chrome options class is used to manipulate various properties of Chrome driver
from selenium.webdriver.chrome.options import Options
#waits till the content loads
from selenium.webdriver.support.ui import WebDriverWait
#finds that content
from selenium.webdriver.support import expected_conditions as EC
#find the above condition/conntent by the xpath, id etc.
from selenium.webdriver.common.by import By

# otp_auth.now()
class fyersAPI:
    def __init__(self, name, path):
        self.chromedriver_path = str(path)
        self.app_id = config.app_id
        self.otp_auth = pyotp.TOTP(config.otp_key)
        self.secret_key = config.secret_key
        self.redirect_uri = config.redirect_uri
        self.name = name
        access_file = open('access_file.txt','r')
        contents = access_file.read()
        if len(contents) != 0:
            lst = contents.split()
            if dt.datetime.strptime(lst[0], '%d-%m-%Y').date() == dt.datetime.now().date():
                self.access_token = lst[1]
                self.client = fyersModel.FyersModel(client_id=self.app_id, token=self.access_token, log_path='fyers_log/')
            else:
                self.access_token = self.login()
                self.client = fyersModel.FyersModel(client_id=self.app_id, token=self.access_token, log_path='fyers_log/')
        else:
            self.access_token = self.login()
            self.client = fyersModel.FyersModel(client_id=self.app_id, token=self.access_token, log_path='fyers_log/')            
        access_file.close()
        self.ins = self.get_ins_df()

    def login(self):
        session=accessToken.SessionModel(client_id=self.app_id,
            secret_key=self.secret_key,redirect_uri=self.redirect_uri, 
            response_type='code', grant_type='authorization_code')
        
        response = session.generate_authcode()
        
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        ex_path = os.getcwd().replace('\\','/')+'/'+self.chromedriver_path
        # ex_path = "/usr/bin/chromedriver"
        driver = webdriver.Chrome(executable_path=ex_path,options=options)
        driver.get(response)
        
        form = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//input[@type='text']")))
        driver.find_element_by_xpath("//input[@type='text']").send_keys(config.client_id)
        driver.find_element_by_xpath("//button[@id='clientIdSubmit']").click()
        
        # form = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//div[@class="row mx-auto mt-1"]')))
        # driver.find_element_by_xpath("//input[@type='password']").send_keys(config.password)
        # driver.find_element_by_xpath("//button[@id='loginSubmit']").click()
        
        form = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//div[@id="otp-container"]')))
        # print(self.otp_auth.now())
        auth_key = [i for i in self.otp_auth.now()]
        driver.find_element_by_xpath("/html/body/section[6]/div[3]/div[3]/form/div[3]/input[1]").send_keys(auth_key[0])
        driver.find_element_by_xpath('/html/body/section[6]/div[3]/div[3]/form/div[3]/input[2]').send_keys(auth_key[1])
        driver.find_element_by_xpath('/html/body/section[6]/div[3]/div[3]/form/div[3]/input[3]').send_keys(auth_key[2])
        driver.find_element_by_xpath('/html/body/section[6]/div[3]/div[3]/form/div[3]/input[4]').send_keys(auth_key[3])
        driver.find_element_by_xpath('/html/body/section[6]/div[3]/div[3]/form/div[3]/input[5]').send_keys(auth_key[4])
        driver.find_element_by_xpath('/html/body/section[6]/div[3]/div[3]/form/div[3]/input[6]').send_keys(auth_key[5])
        
        driver.find_element_by_xpath("//button[@id='confirmOtpSubmit']").click()
        # sleep(5)
        
        pin = [i for i in config.pin]
        form = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//div[@id="pin-container"]')))
        driver.find_element_by_xpath("/html/body/section[8]/div[3]/div[3]/form/div[2]/input[1]").send_keys(pin[0])
        driver.find_element_by_xpath("/html/body/section[8]/div[3]/div[3]/form/div[2]/input[2]").send_keys(pin[1])
        driver.find_element_by_xpath("/html/body/section[8]/div[3]/div[3]/form/div[2]/input[3]").send_keys(pin[2])
        driver.find_element_by_xpath("/html/body/section[8]/div[3]/div[3]/form/div[2]/input[4]").send_keys(pin[3])
        driver.find_element_by_xpath("//button[@id='verifyPinSubmit']").click()
        # form = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, '//div[@class="interstitial-wrapper"]')))
        sleep(7)
        # form = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, '//div[@class="interstitial-wrapper"]')))
        auth = driver.current_url
        driver.close()
        parsed = urlparse.urlparse(auth)
        auth_code = urlparse.parse_qs(parsed.query)['auth_code'][0]
        
        session.set_token(auth_code)
        response = session.generate_token()
        
        access_token = response["access_token"]
        
        acces_file = open('access_file.txt','w')
        string = dt.datetime.now().strftime('%d-%m-%Y')+' '+access_token
        acces_file.write(string)
        acces_file.close()
        
        return access_token
    
        
    def get_ins_df(self):
        
        scrip_link = 'https://public.fyers.in/sym_details/NSE_FO.csv'
        response = requests.get(scrip_link)
        txt = open('instrument_text.txt', 'w')
        write_txt = "time,full_name,h1,lot_size,tick_size,nan_val,rand1,start_date,sys_time,symbol,h2,m2,token,ins,ins_token,strike,option_type,sys2\n"+response.text
        txt.write(write_txt)
        txt.close()
        csv_df = pd.read_csv('instrument_text.txt')
        csv_df.reset_index(inplace=True)
        # csv_df.columns
        csv_df = csv_df[['full_name', 'symbol', 'ins', 'lot_size', 'option_type']]
        # csv_df = csv_df[(csv_df['instrumentName']==self.name.upper())]
        csv_df = csv_df[csv_df['ins'] == self.name]
        csv_df['expiry'] = csv_df['full_name'].apply(lambda x: re.findall('[0-9]{2} [A-Z][a-z]{2} [0-9]{2}', x)[0])
        csv_df['expiry'] = csv_df['expiry'].apply(lambda x: dt.datetime.strptime(x, '%y %b %d').date())
        csv_df = csv_df[(csv_df['option_type']=='CE') | (csv_df['option_type']=='PE')]
        csv_df['strike'] = csv_df['full_name'].apply(lambda x: int(re.findall(' [0-9]{3}.+ ', x)[0].lstrip().rstrip()))           
        # csv_df.drop('level_1', axis=1, inplace=True)
        latest = csv_df.sort_values(by='expiry')['expiry'].iloc[0]     
        csv_df = csv_df[csv_df['expiry']==latest]
        csv_df.reset_index(inplace=True, drop=True)
        csv_df['index'] = ''
        for i in range(len(csv_df)):
            x = csv_df.iloc[i]
            csv_df['index'].iloc[i] = x['ins']+str(x['strike'])+str(x['option_type'])        
        csv_df.set_index('index', inplace=True)
        
        return csv_df
       # fyers.client.quotes({'symbols':'NSE:SBIN-EQ'})
    # fyers.client.quotes(fyers_sym)
    def get_ltps(self, symbols):
        fyers_sym = {'symbols':''}
        ltp_data = {}
        j=0
        for i in range(len(symbols)):
            j+=1
            if i == (len(symbols)-1) or j%40 == 0:
                # print(i)
                if i == (len(symbols)-1):
                    fyers_sym['symbols']+=symbols[i]
                fyers_sym['symbols'] = fyers_sym['symbols'].rstrip(',')
                temp = self.client.quotes(fyers_sym)
                for t in temp['d']:
                    ltp_data[t['n']] = t['v']['lp']
                fyers_sym = {'symbols':''}
                j=0
            fyers_sym['symbols']+=symbols[i]+','
        return ltp_data
# fyers.client.quotes({'symbols':'NSE:SBIN-EQ'})    
    def single_ltp(self, symbol):
        sym = {'symbols':symbol}
        ltp = self.client.quotes(sym)
        if ltp['s'] != 'ok':
            raise Exception('Fyers LTP data Error')
        return float(ltp['d'][0]['v']['lp'])
    
    def running_ltp(self, ce_sym, pe_sym):
        sym = {'symbols':ce_sym+','+pe_sym}
        ltp = self.client.quotes(sym)
        if ltp['s'] != 'ok':
            raise Exception('Fyers LTP data Error')
        t = [0,0]
        if ltp['d'][0]['v']['symbol'] == ce_sym:
            t[0] = ltp['d'][0]['v']['lp']
        if ltp['d'][1]['v']['symbol'] == pe_sym:
            t[1] = ltp['d'][1]['v']['lp']
        t = tuple(t)
        return t
        
