# -*- coding: utf-8 -*-
"""
Created on Thu Dec  2 16:12:50 2021

@author: akhil
"""
# from akhil_api_details import details
from smartapi import SmartConnect
import pyotp
import pandas as pd
import datetime as dt

class ltp_authorize():
    def ltp_login(ser):
        app_key = ser['app_key']
        user = ser['username']
        password = ser['password']
        totp_key = ser['totp_key']
        mpin = ser['mpin']
        # i = int(ser['index'])
        # det = details()
        # client = clients.loc[user]
        # if dt.datetime.strptime(client.date, '%d-%m-%Y').date() != dt.datetime.now().date():     
        auth = pyotp.TOTP(totp_key) 
        obj = SmartConnect(api_key=app_key)
        try:
            data = obj.generateSession(user, mpin, auth.now())
        except:
            data = obj.generateSession(user, password, auth.now())
        refreshToken= data['data']['refreshToken']
        
        return obj
    
    
    def login_with_access(ser):
        app_key = ser['app_key']
        token = ser['access_token']
        obj = SmartConnect(api_key=app_key, access_token=token)
        
        return obj
    
    # tok = 'eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6IlI5NDc5MyIsInJvbGVzIjowLCJ1c2VydHlwZSI6IlVTRVIiLCJpYXQiOjE2NzA0MDU3MTEsImV4cCI6MTc1NjgwNTcxMX0.b-8-RIn5_pnyso1V6KzkFqSPOlrwYG4A57JU3AhURUodaY3oAjSvV6na4GS0ZYrj7qpEtNW13EpG_cK0049jQg'
    # abc = SmartConnect(api_key=client_df['app_key'].iloc[11], access_token=tok)
    
    # abc.access_token
