# -*- coding: utf-8 -*-
"""
Created on Tue Nov 15 18:38:04 2022

@author: akhil
"""

import subprocess
from time import sleep
import pandas as pd
import gspread
import os
import openpyxl
import chromedriver_autoinstaller
import datetime as dt

cred = 'credentials.json'

gc = gspread.service_account(filename=cred)

sh = gc.open_by_url('')
sheet2 = sh.get_worksheet(0)

df = pd.DataFrame(data=sheet2.get_all_records())

# with pd.ExcelWriter('symbol_pause.xlsx') as excel:
#     temp_df = pd.DataFrame()
#     temp_df['Instrument'] = df['Instrument']
#     temp_df['pause'] = int(0)
#     temp_df.to_excel(excel)


fyers_log = open(f'fyers_log/{dt.datetime.now().strftime("%Y-%m-%d")}.txt', 'w')
fyers_log.close()


pause_file = open('init_pause.txt','w')
pause_file.write('0')
pause_file.close()

# import datetime as dt
# dt.time(12,50) - dt.timedelta(minutes=4)

# dt.datetime.combine(dt.datetime.now().date(), entry_time)

PATH = chromedriver_autoinstaller.install(cwd=True)
PATH = os.path.split(PATH)[0][-3:]+'/'+os.path.split(PATH)[1].rstrip('.exe')

for i in range(len(df)):
    row = str(i)
    print(row)
    if df['Reentries_or_combined'].iloc[i].upper() == 'REENTRIES':
        string = 'python reentries.py '+row+' '+PATH
    elif df['Reentries_or_combined'].iloc[i].upper() == 'COMBINED':
        string = 'python straddle_combined_reentries.py '+row+' '+PATH
    print(string)
    subprocess.Popen(string)
    sleep(15)
    while True:
        print('while_loop')
        nifty_txt = open('init_pause.txt', 'r')
        val = int(nifty_txt.read())
        if val == 0:
            print('breaking')
            break
        sleep(5)

