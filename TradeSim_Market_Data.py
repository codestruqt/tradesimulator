from kiteconnect import KiteConnect, exceptions
import queue
import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
import time
import numpy as np
from time import sleep
from time import time
# import cash_var
import pdb
import os
import logging
#import yfinance as yf
import threading
from kiteconnect import KiteTicker
import zmq, json
import sqlite3

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from datetime import datetime
from pprint import pprint
from threading import Thread, Lock
import calendar


zerodha_cred = {
                'Rohit':{
                        'user_id' : 'TS4364',
                        'password': "ta400013@",
                        'api_key' : "fsdcnqwxnwqk6xwb",
                        'common_ans' : "400013",
                        'api_secret' :'dhhavlorxpx5uudvwysd1zxy30vcqvl3',
                        'auth_key': 'Y5ASV5HQXX45IKFFTAFI6CCXO22WYBGK'
                        },
                }

cred = credentials.Certificate("algotrading-117-7e9335873f56.json")

firebase_admin.initialize_app(cred, {
  'projectId': 'algotrading-117',
})


def ist_time(sec, what):
    '''sec and what is unused.'''
    _curr_time = datetime.now() + timedelta(hours=5, minutes=30)
    return _curr_time.timetuple()

cur_date=datetime.now().strftime("%y_%m_%d") #  $(date +"%y_%m_%d")

f_name = 'TradeSim_Market_Data_'+str(cur_date)+".log"
# f_name = 'options_data_'+str(cur_date)+".log"

logging.basicConfig(level=logging.INFO, filename=f_name, filemode="a+",
                        format="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logging.Formatter.converter = ist_time
logging.info(cur_date)
logging.info(f"----------Starting Tick Data Process-----")


class ZER_WS():

    firebabse_db = firestore.client()

    def __init__(self, user, instrument, ins_token) -> None:
        self.instrument     =   instrument
        self.ins_token      =   ins_token
        self.user           =   user
        self.client_json    =   {}
        self.kws            =   None

        self.sqllite = {}
        self.ltp = {}

    
    def get_access_token(self):

        self.client_json[self.user] = {}

        self.client_json[self.user]['api_key']      =   zerodha_cred[self.user]['api_key']
        self.client_json[self.user]['api_secret']   =   zerodha_cred[self.user]['api_secret']
        self.client_json[self.user]['kite']         =   KiteConnect(api_key=self.client_json[self.user]['api_key']) 
        
        auth_keys = {}

        try:
            doc_ref = self.firebabse_db.collection(u'daily_auth_keys').document(u'auth_keys')
            _doc = doc_ref.get()
            if _doc.exists:
                auth_keys = _doc.to_dict()
            else:
                print('auth Document not Exists')
        
        except Exception as e:
            print(e)
            print('----------\n')
            pass

        if len(auth_keys)>0:
            access_token    =   auth_keys[self.user]
            self.client_json[self.user]['kite'].set_access_token(access_token)
            # access_token = self.client_json[self.user]['kite'].set_access_token(auth_keys[self.user])
        else:
            print(f" Cannot set access token ")
        


        self.kws = KiteTicker(self.client_json[self.user]['api_key'], access_token)


    def get_banknifty_close_price(self):
        try:
            doc_ref = self.firebabse_db.collection(u'daily_min_update').document(u'BANKNIFTY_1min')
            _doc = doc_ref.get()
            close_price = _doc['close']
      
        except Exception as e:
            close_price = 41000
            print(e)
            print('----------\n')
            pass
        
        return close_price


    def filter_instruments(self):
        """
        Fetch Instruments table and filter instruments
        """
        pdb.set_trace()
        
        logging.info(f"Filtering Instruments")
        instruments = pd.DataFrame(self.client_json[self.user]['kite'].instruments()) 
        
        option_contract_details = self.firebabse_db.collection('1_0_X Configuration').document("Option Contract Details").get().to_dict()
        month_name = option_contract_details['Month'].capitalize()
        month_num = list(calendar.month_abbr).index(month_name)
        expiry_date = datetime(int('20'+option_contract_details['Year']), int(month_num), int(option_contract_details['Week Date']))
        
        BN_instruments = instruments[(instruments.exchange=='NFO') & (instruments.segment=='NFO-OPT') & (instruments.name=='BANKNIFTY') & (instruments.expiry.astype(str)==expiry_date.strftime("%Y-%m-%d"))].reset_index()
        self.BN_instruments_list = BN_instruments.instrument_token.to_list()
        self.BN_instruments_list.append(260105)
        self.token_dictionary = {}
        
        for i in BN_instruments.index:
            self.token_dictionary[BN_instruments.instrument_token.iloc[i]] = BN_instruments.tradingsymbol.iloc[i]
        self.token_dictionary[260105] = 'BANKNIFTY'


    def on_ticks(self,ws, ticks):
        """
        Recieves ticks and puts them in SQLLite tables
        """
        self.ltp['timestamp'] = datetime.now()
        logging.info(f'Received Tick')
        a = time()
        for tick in ticks:
            self.ltp[self.token_dictionary[tick['instrument_token']]] = tick['last_price']
        try:
            doc_ref = self.firebabse_db.collection(u'tick_data').document(u'BANKNIFTY')
            _doc = doc_ref.set(self.ltp)
        except Exception as e:
            print(f'Error in uploading - {e}')        

        logging.info(f"Processed Tick\u001b[0m\t{(time()-a)*1000}")
        print(f"{datetime.now().strftime('%H:%M:%S')}\t\u001b[42;1mRunning\u001b[0m\t{(time()-a)*1000}")


    def on_connect(self,ws, response):

        # {"256265":"NIFTY50","260105":"BANKNIFTY"}
        ws.subscribe(self.BN_instruments_list)

        ws.set_mode(ws.MODE_FULL, self.BN_instruments_list)
        # ws.set_mode(ws.MODE_LTP, self.BN_instruments_list)
        

    def on_close(self,ws, code, reason):
        # On connection close stop the main loop
        # Reconnection will not happen after executing `ws.stop()`
        ws.stop()


    def run_ws(self):
        self.kws.on_ticks   =   self.on_ticks
        self.kws.on_connect =   self.on_connect
        self.kws.on_close   =   self.on_close
        self.auto_reconnect()
        # self.kws.connect()


    def auto_reconnect(self):
        # while True:
        try:
            # self.kws.close()
            self.kws.connect()
        except Exception as e:
            print(f"Connection Issue - {e}")


if __name__ == '__main__':
    user        =   'Rohit'
    instrument  =   'BANKNIFTY'
    ins_token   =   260105

    test_ws = ZER_WS(user,instrument,ins_token)
    test_ws.get_access_token()
    test_ws.filter_instruments()
    
    test_ws.run_ws()