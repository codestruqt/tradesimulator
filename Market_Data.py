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

lock = Lock()

zerodha_cred = {
                    'Paresh':{
                        'user_id' : 'RUR252',
                        'password': "P26127274",
                        'api_key' : "wugcmlzedhkf10nv",
                        'common_ans' : "261272",
                        'api_secret' :'wx2nefolurs7c3xq9pkp4cwr7agfolei',
                        'auth_key': 'U5ZS7UXTYQ3PKZLOQHV4T3GIZ5N3CVSK'
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


class ZER_WS():

    firebabse_db = firestore.client()

    def __init__(self, user, instrument, ins_token) -> None:
        self.instrument     =   instrument
        self.ins_token      =   ins_token
        self.user           =   user
        self.client_json    =   {}
        self.kws            =   None

        self.sqllite = {}

    
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
        instruments = pd.DataFrame(self.client_json[self.user]['kite'].instruments()) 
        
        
        option_contract_details = self.firebabse_db.collection('1_0_X Configuration').document("Option Contract Details").get().to_dict()
        month_name = option_contract_details['Month'].capitalize()
        month_num = list(calendar.month_abbr).index(month_name)
        expiry_date = datetime(int('20'+option_contract_details['Year']), int(month_num), int(option_contract_details['Week Date']))
        
        BN_instruments = instruments[(instruments.exchange=='NFO') & (instruments.segment=='NFO-OPT') & (instruments.name=='BANKNIFTY') & (instruments.expiry.astype(str)==expiry_date.strftime("%Y-%m-%d"))].reset_index()
        close_price = self.get_banknifty_close_price()
        self.BN_instruments_list = BN_instruments.instrument_token.to_list()
        # pdb.set_trace()
        self.BN_instruments_list.append(260105)
        self.token_dictionary = {}
        
        for i in BN_instruments.index:
            self.token_dictionary[BN_instruments.instrument_token.iloc[i]] = BN_instruments.tradingsymbol.iloc[i]
        self.token_dictionary[260105] = 'BANKNIFTY'


    def create_sqllite_tables(self):
        """
        Creates SQLLite tables for each instrument
        """
        conn = sqlite3.connect(f'Market Data/tick_data_' + datetime.now().strftime("%d_%m_%Y") + '.db')
        cursor = conn.cursor()

        for token in self.token_dictionary.keys():
            self.sqllite[self.token_dictionary[token]] = {}

            # Create a cursor
            
            try:
                # Execute the CREATE TABLE statement
                cursor.execute(f"""
                CREATE TABLE {self.token_dictionary[token]} (
                    id INTEGER PRIMARY KEY,
                    LTP REAL,
                    exchange_timestamp TEXT,
                    last_trade_time TEXT
                )
                """)
            except Exception as e:
                print(e)

        # Commit the changes to the database
        conn.commit() 
        conn.close()
        self.sqllite = {}


    def on_ticks(self,ws, ticks):
        """
        Recieves ticks and puts them in SQLLite tables
        """
        # pdb.set_trace()
        for tick in ticks:
            t1 = Thread(target=self.insert_into_sqlite, args=(tick,))
            t1.start()

        print(f"{datetime.now().strftime('%H:%M:%S')}\t\u001b[42;1mRunning\u001b[0m")


    def insert_into_sqlite(self, tick):
        with lock:
            # print(tick)
            conn = sqlite3.connect(f"Market Data/tick_data_" + datetime.now().strftime("%d_%m_%Y") + '.db')
            cursor = conn.cursor()
            
            if tick['instrument_token'] == 260105:
                cursor.execute(f"""
                INSERT INTO {self.token_dictionary[tick['instrument_token']]} (LTP, exchange_timestamp, last_trade_time)
                VALUES (?, ?, ?)
                """, (tick['last_price'], tick['exchange_timestamp'].strftime('%d-%m-%Y %H:%M:%S'), tick['exchange_timestamp'].strftime('%d-%m-%Y %H:%M:%S')))
            else:
                cursor.execute(f"""
                INSERT INTO {self.token_dictionary[tick['instrument_token']]} (LTP, exchange_timestamp, last_trade_time)
                VALUES (?, ?, ?)
                """, (tick['last_price'], tick['exchange_timestamp'].strftime('%d-%m-%Y %H:%M:%S'), tick['last_trade_time'].strftime('%d-%m-%Y %H:%M:%S')))

            conn.commit()
            conn.close()


    def on_connect(self,ws, response):

        # {"256265":"NIFTY50","260105":"BANKNIFTY"}
        ws.subscribe(self.BN_instruments_list)

        # Set RELIANCE to tick in `full` mode.
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
    user        =   'Paresh'
    instrument  =   'BANKNIFTY'
    ins_token   =   260105

    test_ws = ZER_WS(user,instrument,ins_token)
    test_ws.get_access_token()
    test_ws.filter_instruments()
    test_ws.create_sqllite_tables()
    
    test_ws.run_ws()