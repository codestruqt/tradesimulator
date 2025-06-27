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
                'Rohit':{
                    'user_id' : 'WR6232',
                    'password': "ta400013@",
                    'api_key' : "aeu0vtikzvazyt1l",
                    'common_ans' : "060189",
                    'api_secret' :'2pua9bx7rs7m0wqn0iq2ejihvzhnplyi',
                    'auth_key': 'JQYRNXM5UK6BIRXAHLEVI3LWOI3GTDAQ'
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
        for token in self.token_dictionary.keys():
            self.sqllite[self.token_dictionary[token]] = {}

            # Create a cursor
            self.sqllite[self.token_dictionary[token]]['conn'] = sqlite3.connect(f'Market Data/{self.token_dictionary[token]}_' + datetime.now().strftime("%d_%m_%Y") + '.db')
            self.sqllite[self.token_dictionary[token]]['cursor'] = self.sqllite[self.token_dictionary[token]]['conn'].cursor()
            
            try:
                # Execute the CREATE TABLE statement
                self.sqllite[self.token_dictionary[token]]['cursor'].execute(f"""
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
            self.sqllite[self.token_dictionary[token]]['conn'].commit() 
            self.sqllite[self.token_dictionary[token]]['conn'].close()
        self.sqllite = {}


    def Upload_data(self):
        previous_second = datetime.now().second
        conn = sqlite3.connect(f'Market Data/tick_data_' + datetime.now().strftime("%d_%m_%Y") + '.db')
        self.ltp={}
        while True:
            if previous_second!=datetime.now().second:
                previous_second = datetime.now().second
                print(f"{datetime.now().strftime('%H:%M:%S')}\tUploaded")
                self.ltp['timestamp'] = datetime.now()
                a = time()
                for token in self.token_dictionary.keys():
                #     threads.append(Thread(target=self.dictionary_writer, args=(token,)))
                    
                # for a_thread in threads:
                #     a_thread.start()

                # for a_thread in threads:
                #     a_thread.join()    
                    """
                    ticker = self.token_dictionary[token]

                    # tick_data = pd.read_sql_query(f"SELECT * FROM {ticker}", conn)  
                    # tick_data = pd.read_sql_query(f"SELECT * FROM {ticker} WHERE exchange_timestamp >= datetime('now', '-10 hours')", conn)
                    # tick_data = pd.read_sql_query(f"SELECT * FROM {ticker} WHERE strftime('%s', exchange_timestamp) >= strftime('%s', 'now', '-10 hours')", conn)
                    tick_data = pd.read_sql_query(f"SELECT * FROM {ticker} WHERE exchange_timestamp >= strftime('%d-%m-%Y %H:%M:%S', 'now', '-10 seconds')", conn)
                    


                    print(tick_data)
                    if len(tick_data)>0:
                        tick_data['Datetime'] = pd.to_datetime(tick_data['exchange_timestamp'], format='%d-%m-%Y %H:%M:%S')
                        LTP = tick_data['LTP'].iloc[-1]
                        ltp[ticker] = LTP
                    """
                    ticker = self.token_dictionary[token]
                    tick_data = pd.read_sql_query(f"SELECT * FROM {ticker} ORDER BY rowid DESC LIMIT 1", conn)    
                    # tick_data['Datetime'] = pd.to_datetime(tick_data['exchange_timestamp'],format='%d-%m-%Y %H:%M:%S')
                    # tick_data_point = tick_data[tick_data['Datetime']>(datetime.now()-timedelta(hours=10))].reset_index()
                    LTP = tick_data['LTP'].iloc[0]
                    self.ltp[ticker] = LTP
                    
                print(f"Time = {(time()-a)*1000}")
                try:
                    doc_ref = self.firebabse_db.collection(u'tick_data').document(u'BANKNIFTY')
                    _doc = doc_ref.set(self.ltp)
                except Exception as e:
                    print(f'Error in uploading - {e}')
                tick_data = pd.DataFrame()


    def dictionary_writer(self, token):
        conn = sqlite3.connect(f'Market Data/tick_data_' + datetime.now().strftime("%d_%m_%Y") + '.db')
        ticker = self.token_dictionary[token]

        tick_data = pd.read_sql_query(f"SELECT * FROM {ticker}", conn)    
        tick_data['Datetime'] = pd.to_datetime(tick_data['exchange_timestamp'],format='%d-%m-%Y %H:%M:%S')
        tick_data_point = tick_data[tick_data['Datetime']>(datetime.now()-timedelta(hours=10))].reset_index()
        LTP = tick_data_point['LTP'].iloc[-1]
        # print(LTP)
        lock.acquire(timeout=20)
        try:
            self.ltp.update({ticker: LTP})
        finally:
            lock.release()




if __name__ == '__main__':
    user        =   'Rohit'
    instrument  =   'BANKNIFTY'
    ins_token   =   260105

    test_ws = ZER_WS(user,instrument,ins_token)
    test_ws.get_access_token()
    test_ws.filter_instruments()
    
    test_ws.Upload_data()