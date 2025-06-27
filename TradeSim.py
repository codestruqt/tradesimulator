# VERSION 26.02-1
#       - Addition to incorporate Trades and Trades with BB

# VERSION HISTORY 
#           10.02-1
#           10.02-1 - Pilot

from cgitb import reset
from http.client import HTTP_VERSION_NOT_SUPPORTED
import pickle
from pprint import pprint
from signal import signal
import threading
from datetime import date, datetime, timedelta
import datetime as dt
import numpy as np
import logging
import time
from time import sleep
from kiteconnect import KiteConnect
import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import pdb, os, requests
from tabulate import tabulate
import art
from inspect import currentframe, getframeinfo  # FOR LINE NUMBER
from pprint import pprint
from threading import Thread 
import sqlite3
from Configuration import *
import random
import math
import json
import ast


class User_Input():
    """
    User Input Class for NSE 1-0-X
    
    Parameters:
        firebase_db (obj)       : Firestore Client Object
        instrument (str)        : Instrument Details
        user_list (list)        : List of All Users 
        zerodha_table (bool)    : Toggle On/Off for Zerodha Positions in Terminal Output 
        options_type (str)      : BUY or SELL
    """
    def __init__(self,instrument='BANKNIFTY', zerodha_table=False, options_type = 'SELL'):
        self.instrument             = instrument
        self.last_operations        = []
        self.last_operations_size   = 10
        self.display_message        = ''
        self.zerodha_table          = zerodha_table
        self.options_type           = options_type
        self.Create_SQLlite_Tables(options_type)


    def Create_SQLlite_Tables(self, options_type):
        """
        Create SQLlite to start with
        """
        conn = sqlite3.connect('Files/Input.db')
        c = conn.cursor()
        
        dict_data = {
                     'Options_Type' : options_type,
                     'ITM_Range'    : 1,
                     'Quantity'     : 500,
                     'Direction'    : None,
                     'Match_Quantity': 0
                     }
        
        c.execute("CREATE TABLE IF NOT EXISTS Input (key text, value text)")
        for key, value in dict_data.items():
            c.execute("INSERT INTO Input (key, value) VALUES (?,?)", (key, value))
        conn.commit()
        conn.close()


    def Modify_Table(self, key, new_value, table_name='Input'):
        """
        Modifies SQLlite table
        """
        conn = sqlite3.connect('Files/Input.db')
        c = conn.cursor()
        c.execute(f"UPDATE {table_name} SET value=? WHERE key=?", (new_value, key))
        conn.commit()
        conn.close()    


    def Read_Table(self, table_name='Input'):
        """
        Reads SQLlite Table and returns the original dictionary
        """
        conn = sqlite3.connect('Files/Input.db')
        c = conn.cursor()
        c.execute(f"SELECT * FROM {table_name}")
        list_of_tuples = c.fetchall()
        return dict(list_of_tuples)


    def Update_Signal_table(self, Direction):
        """
        Updates Signal_Info to DB for Backend Order Process

        Parameters:
            Direction (str)       : Long or Short or Exit
        """
        input_table = self.Read_Table()
        
        self.Modify_Table('Direction', Direction)
        self.Modify_Table('Match_Quantity', input_table['Quantity'])

        if Direction.lower() == 'long':
            self.display_message = f"{ASCII.OKBLUE}{Direction.upper()} SIGNAL SENT{ASCII.Reset}"
        else:
            self.display_message = f"{ASCII.Red}{Direction.upper()} SIGNAL SENT{ASCII.Reset}"
 

    def last_operations_add(self, user_input):
        """
        Add User_input to last operations list
        """
        self.last_operations.append(str(user_input))
        if len(self.last_operations) > self.last_operations_size:
            self.last_operations.pop(0)


    def Welcome_Message(self):
        """
        Displays welcome message on screen along with Current Firebase Status
        """
        os.system("cls")
        print(ASCII.BackgroundWhite,'                          TradeSim                          ',ASCII.Reset,"\n")
        # SECTION 1 -   CLOCK
        print(ASCII.BackgroundWhite,datetime.strftime(datetime.now(),"%H : %M : %S"),ASCII.Reset,"\n")
        
        input_table = self.Read_Table()
        print(f"INPUT TABLE\n")
        print("\u001b[38;5;46m",tabulate(input_table.items(), headers=['Key', 'Value'], tablefmt = 'rounded_grid', numalign="center",stralign='center'),ASCII.Reset,"\n")

        print(f"Last {self.last_operations_size} command inputs = {self.last_operations}\n\n","_"*60,"\n")
        
        print("\u001b[48;5;235m Commands List",ASCII.Reset)
        print("\t1 \t\t- Long")
        print("\t0 \t\t- Short")
        print("\tX \t\t- Exit Trade")
        print("\tQ, Match\t- Match Trade Quantity")
        print("\tQuantity\t- Update Base Quantity Config")
        print("\tRange\t\t- Update ITM Range Config")
        print("_"*60,"\n\n")
        if self.display_message != '':
            print(f'{ASCII.BackgroundBrightWhite} UPDATE  {ASCII.Reset}  {self.display_message}\n\n')


    def Modify_Quantity(self, user_input):
        """
        Modify Quantity
        """
        quantity = int(user_input.split(" ")[1])
        self.Modify_Table('Quantity', int(int(quantity)/25)*25)          
        self.display_message = f'{ASCII.BrightYellow}Updated Quantity = {quantity}{ASCII.RESET_CMD}'


    def ITM_Range_Edit(self, user_input):
        """
        Updates ITM Range
        """
        range = int(user_input.split(" ")[1])
        self.Modify_Table('ITM_Range', int(range))            
        self.display_message = f'{ASCII.BrightYellow}Updated ITM Range = {range}{ASCII.RESET_CMD}'


    def Add_Quantity(self, user_input):
        """
        Matches to the final quantity specified in User Input
        If existing quantity is 500, 
        User Input is 'match 700',
        Then final quantity in Broker account will be 700,
        for that additional order of 200 qty will be placed
        """
        final_qty = int(user_input.split(" ")[1])
        final_qty = int(final_qty/25)*25
        self.Modify_Table('Match_Quantity', final_qty)
        self.display_message = f'{ASCII.BrightYellow}Updated Match Quantity\tQty = {final_qty}{ASCII.RESET_CMD}'


    def Main_Function(self):
        while True: 
            user_input=''
            self.Welcome_Message()

            try:
                user_input =input(f"{ASCII.Blue}Enter the Trade\t= {ASCII.Reset}")
                logging.info(f"User Input = {user_input}")
            except Exception as e:
                self.display_message = f'{ASCII.WARNING}Entered Invalid Parameters - {e}{ASCII.RESET_CMD}'
                logging.info(f"Invalid User Input Error {e}")
            
            if user_input.lower() in ['long','buy','1']: 
                self.last_operations_add(user_input)
                self.Update_Signal_table('Long')
         
            elif user_input.lower() in ['short', 'sell', '0']:
                self.last_operations_add(user_input)
                self.Update_Signal_table('Short')

            elif user_input in ['x','exit']:
                self.last_operations_add(user_input)
                self.Update_Signal_table('Exit')
                self.display_message = f'{ASCII.ENDC}EXIT SIGNAL SENT{ASCII.RESET_CMD}'

            elif 'quantity ' in user_input.lower():
                self.Modify_Quantity(user_input)

            elif 'range' in user_input.lower() or \
                 'itm_range' in user_input.lower() or \
                 'itm' in user_input.lower():
                self.ITM_Range_Edit(user_input)

            elif 'add ' == user_input.lower()[:4] or \
                 'q ' == user_input.lower()[:2] or \
                 'match ' in user_input.lower()[:6]:
                self.Add_Quantity(user_input)

            elif user_input == '':
                self.display_message = f'{ASCII.Green}Refreshed{ASCII.RESET_CMD}'
            
            else:
                self.display_message = "\u001b[31mINVALID USER INPUT, PLEASE ENTER VALID COMMAND\u001b[0m"


class Broker_System(): 
    """
    Supports backtesting of NSE Intraday Options Trading
       
    Attributes:
        indicator_list (list)   : List of tuples of indicator parameters. 
                                    SMA     - ('SMA', tf, type, lag)
                                    BB      - ('BB', type, lag, std dev)
        symbol (str)                    : Traded Symbol
        start_date (str)                : Format - YYYY-mm-dd
        end_date (str)                  : Format - YYYY-mm-dd 
        start_time (str)                : Format - HH:MM:SS
        end_time (str)                  : Format - HH:MM:SS
        time_resolution (str)           : s1 / s5 / m1
        initial_capital (int)           : In INR
        slippage (int)                  : Points for end of the day
        indicator_list (list)           : List of tuples, see class documentation for details
        print_every_minute (bool)       : To print backtest on every clock iteration
        output_path (str)               : Default will be taken from Configuration File
        custom_print1 (any)             : Assign any variable, will be printed on Terminal
        custom_print2 (any)             : Assign any variable, will be printed on Terminal
        candle_requirement_list         : Specifiy list of required timeframes e.g. ['m1', 'm5', 'm7']
        margin_type                     : ATM / OTM75000 / OTM80000
        candle_length (int)             : If not specified then Candle Data will be formed in regular manner, 
                                            For specified value i.e. 55 or 58, 1-minute candle length will be  55 or 58 seconds for Noncomplete candle
        quantity_freeze_limit (int)     : Maximum number (quantity) of contracts that a market participant can buy or sell in a single order, default is 1200 
        default_freeze_quantity (int)   : Default Freeze Quantity as per NSE norms
        options_candle_data (bool)      : Enable or Disable loading Options Candle Data into Engine, default is False

    Notes:
        self.exception_list             : Contains list of Exceptional Expiry Days List
    """
    def __init__(
                 self, 
                 symbol                 = 'BANKNIFTY', 
                 start_date             = datetime.now(), 
                 end_date               = datetime.now() + timedelta(days=1), 
                 file_path              = '', 
                 filename               = "rules", 
                 initial_capital        = 100000, 
                 slippage               = 0, 
                 print_every_minute     = False, 
                 start_time             = '09:15:00', 
                 end_time               = '15:25:00', 
                 time_resolution        = 'm1', 
                 small_market_book      = True, 
                 candle_requirement_list= ['m1'], 
                 margin_type            = 'ATM',
                 candle_length          = None,
                 quantity_freeze_limit  = 900,
                 default_freeze_quantity= 900,
                 options_candle_data_switch = False,
                 ):
        
        # PRIMARY VARIABLES  --------------------------------------------------------------------------------------------------
        
        cred = credentials.Certificate("algotrading-117-7e9335873f56.json")
        app = firebase_admin.initialize_app(cred, {
                                                    'projectId': 'algotrading-117',
                                                    })        
        self.firebase = firestore.client(app)

        self.capital        = initial_capital
        self.symbol         = symbol
        self.lot_size       = 25
        self.fname          = filename
        self.file_path      = file_path
        self.order_id       = 1000000
        self.custom_print1  = None
        self.custom_print2  = None
        self.Quantity       = 0
        self.Slippage       = slippage
        self.Margin_Requirement    = {'ATM' : 150000, 'OTM' : 80000, 'OTM80000' : 80000, 'OTM75000' : 75000}
        self.margin         = self.Margin_Requirement[margin_type]
        self.margin_type    = margin_type
        self.quantity_freeze_limit = quantity_freeze_limit
        self.default_freeze_quantity = default_freeze_quantity
        self.report_id      = datetime.strftime(datetime.now(),"%d%m%y%H%M%S%f_") + str(random.randrange(1, 1000)) 

        self.clock_time = datetime.now()
        self.start_date = start_date
        self.end_date   = end_date
        self.start_time = start_time
        self.end_time   = end_time
        self.time_resolution = time_resolution
        
        # FETCH DETAILS FROM CONFIGURATION FILE
        self.output_path                = ''
        
        # ---------------------------------------------------------------------------------------------------------------------------

        # BLANK ORDER BOOK AND POSITIONS---------------------------------------------------------------------------------------------
        self.trade_df = pd.DataFrame()
        self.order_book = pd.DataFrame(columns=['order_timestamp',
                                                'exchange_timestamp', 
                                                'order_id', 
                                                'status', 
                                                'status_message',
                                                'order_type', 
                                                'tradingsymbol', 
                                                'transaction_type', 
                                                'quantity', 
                                                'quantity_id', 
                                                'average_price',
                                                'spot_price', 
                                                'filled_quantity',	
                                                'pending_quantity', 
                                                'cancelled_quantity', 
                                                'tag',
                                                'trade_pnl',
                                                'custom_column1',
                                                'custom_column2',
                                                'custom_column3'])
        self.complete_order_book = self.order_book.copy()
        self.custom_orderbook_column1 = np.nan
        self.custom_orderbook_column2 = np.nan
        self.custom_orderbook_column3 = np.nan

        self.broker_positions = {}
        self.Net_PnL   = 0
        self.ticker_position_template = {'quantity'             : 0,
                                         'average_entry_price'  : 0,
                                         'previous_LTP'         : 0,
                                         'LTP'                  : 0,
                                         'PnL'                  : 0,
                                         'contract_PnL'         : 0}
        
        self.day_performance_statistics = pd.DataFrame(columns=['Date', 'Day PnL', 'Brokerage', 'Slippage', 'Equity']) 
        # ---------------------------------------------------------------------------------------------------------------------------
        
        # CREATE BLANK MARKET BOOK --------------------------------------------------------------------------------------------------
        self.small_market_book = small_market_book
        self.broker_market_book = pd.DataFrame()        
        seconds_list = []
        _temp_time = self.clock_time
        # TODO - Create Blank Template in Advance 
        self.time_resolution_delta = {'s1':1, 's5':5, 's30':30, 's40':40, 's45':45, 's50':50, 'm1':60, 's70':70, 's75':75,  }
        while self.end_date > _temp_time:
            seconds_list.append(_temp_time)
            _temp_time += timedelta(seconds=self.time_resolution_delta[time_resolution])
            if _temp_time.time() > datetime.strptime(end_time,"%H:%M:%S").time():
                seconds_list.append(_temp_time)
                _temp_time += timedelta(days=1)
                _temp_time = datetime.strptime(datetime.strftime(_temp_time, "%Y-%m-%d ") + start_time, "%Y-%m-%d %H:%M:%S")
        self.broker_market_book['Clock_time'] = seconds_list
        self.broker_market_book['Net_PnL'] = np.nan
        self.broker_market_book['Strategy_PnL'] = np.nan
        self.broker_market_book.set_index('Clock_time',inplace=True)
        # ---------------------------------------------------------------------------------------------------------------------------
        
        # INDEX AND OPTIONS DATA ------------------------------------------------------------------------------------------------------
        # TODO - Add all timeframes here
        self.index_LTP              = None
        self.index_tick_data        = None
        self.options_tick_data      = {}
        self.options_LTP            = {}
        self.options_candle_data_source = {}
        self.options_candle_data    = {}
        self.options_candle_data_switch = options_candle_data_switch

       
        # READ INDEX TICK DATA
        self.index_tick_data_day = None
        self.index_tick_data = pd.DataFrame()
        self.spot_LTP = None

        # ---------------------------------------------------------------------------------------------------------------------------
        self.print_tables = print_every_minute
        pd.options.mode.chained_assignment = None 

        """
        # READ EXISTING DATA OF DIFFERENT VARIABLES
        sqllite3_conn = sqlite3.connect("Files/Backend.db")
        try:
            position_table = pd.read_sql_query(f"SELECT * from Positions{datetime.now().strftime('_%d_%m_%Y')}", sqllite3_conn) 
            position_dict = {}
            if len(position_table)>0:
                for i in position_table.index:
                    position_dict[position_table['key'].iloc[i]] = ast.literal_eval(position_table['value'].iloc[i])
            self.broker_positions = position_dict    
            if len(self.broker_positions)>0:
                for ticker in self.broker_positions.keys():
                    self.Update_Options_Database(ticker)
        except:
            pass
        try:
            orders_table = pd.read_sql_query(f"SELECT * from Order_Book{datetime.now().strftime('_%d_%m_%Y')}", sqllite3_conn) 
            if len(orders_table)>0:
                self.order_book = orders_table
        except:
            pass
        """
        
        # Write the object to a pickle file
        try:
            with open(f"Files/Positions{datetime.now().strftime('_%d_%m_%Y.pickle')}", "rb") as f:
                position_dict = pickle.load(f)        
                self.broker_positions = position_dict    
                if len(self.broker_positions)>0:
                    for ticker in self.broker_positions.keys():
                        self.Update_Options_Database(ticker)
        except:
            pass
        try:
            with open(f"Files/Order_Book{datetime.now().strftime('_%d_%m_%Y.pickle')}", "rb") as f:
                orders_table = pickle.load(f)    
                if len(orders_table)>0:
                    self.order_book = orders_table
        except:
            pass

        self.Create_SQLlite_Tables()


    def data_supply(self):
        """
        Fetch all timeframes data every minute 
        """
        try:
            doc_ref = self.firebase.collection(u'tick_data').document(u'BANKNIFTY')
            db_data = doc_ref.get().to_dict()

            # INDEX TICK DATA
            self.spot_LTP = db_data['BANKNIFTY']
            
            # OPTINS DATA UPDATE
            for ticker in self.options_LTP.keys():
                self.options_LTP[ticker] = db_data[ticker]
        except Exception as e:
            print(f"Data Supply error - {e}")
 

    def data_supply_old(self):
        """
        Fetch all timeframes data every minute 
        """
        # INDEX TICK DATA
        conn = sqlite3.connect(self.index_tick_data_path + 'BANKNIFTY' + datetime.now().strftime("_%d_%m_%Y.db"))
        index_tick_data = pd.read_sql_query(f"SELECT * FROM BANKNIFTY", conn)    
        index_tick_data['Datetime'] = pd.to_datetime(index_tick_data['exchange_timestamp'],format='%d-%m-%Y %H:%M:%S')
        # TODO
        # tick_data_point = index_tick_data[index_tick_data['Datetime']>(self.clock_time-timedelta(seconds=10))].reset_index()
        tick_data_point = index_tick_data[index_tick_data['Datetime']>(self.clock_time-timedelta(hours=10))].reset_index()
        LTP = tick_data_point['LTP'].iloc[-1]
        self.spot_LTP = LTP
        
        # OPTINS DATA UPDATE --------------------------------------------------------------------------------------------------
        for ticker in self.options_LTP.keys():
            # OPTION TICK DATA UPDATE
            try:
                conn = sqlite3.connect(self.options_tick_data_path + ticker + datetime.now().strftime("_%d_%m_%Y.db"))
                tick_data = pd.read_sql_query(f"SELECT * FROM {ticker}", conn)    
                tick_data['Datetime'] = pd.to_datetime(tick_data['exchange_timestamp'],format='%d-%m-%Y %H:%M:%S')
                # TODO
                # tick_data_point = tick_data[tick_data['Datetime']>(self.clock_time-timedelta(seconds=10))].reset_index()
                tick_data_point = tick_data[tick_data['Datetime']>(self.clock_time-timedelta(hours=10))].reset_index()
                LTP = tick_data_point['LTP'].iloc[-1]
                self.options_LTP[ticker] = LTP
            except Exception as e:
                try:
                    conn = sqlite3.connect(self.options_tick_data_path + ticker + datetime.now().strftime("_%d_%m_%Y.db"))
                    tick_data = pd.read_sql_query(f"SELECT * FROM {ticker}", conn)    
                    tick_data['Datetime'] = pd.to_datetime(tick_data['exchange_timestamp'],format='%d-%m-%Y %H:%M:%S')
                    tick_data_point = tick_data[tick_data['Datetime']>(self.clock_time-timedelta(seconds=30))].reset_index()
                    LTP = tick_data_point['LTP'].iloc[-1]
                    self.options_LTP[ticker] = LTP
                except Exception as e:
                    print(f'Error in Options Data - {ticker}')
            

    def df_to_sqlite_table(self, df, table_name):
        # Create a connection to SQLite database
        conn = sqlite3.connect('Files/Backend.db')
        
        # Convert the pandas dataframe to SQLite table
        df.to_sql(table_name, conn, if_exists='replace', index=False)

        conn.close()


    def store_dict_in_sqlite(self, dictionary,table_name):
        conn = sqlite3.connect('Files/Backend.db')
        c = conn.cursor()
        
        # Drop existing table if it exists
        c.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Create table
        c.execute(f"CREATE TABLE {table_name} (key text, value text)")
        
        # Insert values into the table
        for key, value in dictionary.items():
            c.execute(f"INSERT INTO {table_name} (key, value) VALUES (?, ?)", (key, str(value)))
        
        # Commit changes and close connection
        conn.commit()
        conn.close()


    def Create_SQLlite_Tables(self):
        """
        Create SQLlite to start with
        """
        conn = sqlite3.connect('Files/Backend.db')
        c = conn.cursor()

        dict_data   = { 'Direction'     : None,
                        'Quantity'      : 0,
                        'Match_Quantity': 0,
                        'Ticker'        : 0,
                        'Order ID'      : None,
                        'Order Time'    : None }  
       
        c.execute("CREATE TABLE IF NOT EXISTS Backend (key text, value text)")
        for key, value in dict_data.items():
            c.execute("INSERT INTO Backend (key, value) VALUES (?,?)", (key, str(value)))
        conn.commit()
        conn.close()


    def Modify_Table(self, key, new_value, table_name='Backend'):
        """
        Modifies SQLlite table
        """
        conn = sqlite3.connect('Files/Backend.db')
        c = conn.cursor()
        c.execute(f"UPDATE {table_name} SET value=? WHERE key=?", (new_value, key))
        conn.commit()
        conn.close()    


    def Read_Backend_Table(self, table_name='Backend'):
        """
        Reads SQLlite Table and returns the original dictionary
        """
        conn = sqlite3.connect('Files/Backend.db')
        c = conn.cursor()
        c.execute(f"SELECT * FROM {table_name}")
        list_of_tuples = c.fetchall()
        return dict(list_of_tuples)


    def Read_Input_Table(self, table_name='Input'):
        """
        Reads SQLlite Table and returns the original dictionary
        """
        conn = sqlite3.connect('Files/Input.db')
        c = conn.cursor()
        c.execute(f"SELECT * FROM {table_name}")
        list_of_tuples = c.fetchall()
        return dict(list_of_tuples)


    def Read_Input_Table_from_FIREBASE(self, user='Sumit'):
        """
        Reads SQLlite Table and returns the original dictionary
        """
        input_table = {}
        input_table['Options_Type'] = 'BUY'
        try:
            doc_ref = self.firebase.collection(u'1_0_X Configuration').stream()
            for doc in doc_ref:
                if doc.id=='ITM Range':
                    input_table['ITM_Range'] = doc.to_dict()['Range']
                elif doc.id==user:
                    input_table['Match_Quantity'] = doc.to_dict()['Quantity']
                    input_table['Quantity'] = doc.to_dict()['Quantity']

            signal_doc = self.firebase.collection(u'NSE_1_0_X_Signal_Book').document(u'BANKNIFTY').get().to_dict()
            input_table['Direction'] = signal_doc['Direction']
        
        except Exception as e:
            print(f"Fatal errpr om reading Input Table from Firebase - {e}")

        return input_table


    def Read_Input_Table_from_Firebase_Market_Book(self, user='Sumit'):
        """
        Reads SQLlite Table and returns the original dictionary
        """
        input_table = {}
        try:
            market_book = self.firebase.collection(u'NSE_1_0_X_Market_Book_'+user).document(u'BANKNIFTY').get().to_dict()
            input_table['Match_Quantity'] = market_book['Match_Quantity']
            input_table['Quantity'] = market_book['Quantity']
            input_table['Direction'] = market_book['Direction']
            input_table['Ticker'] = market_book['Ticker']
            input_table['Options_Type'] = 'BUY'   
        except Exception as e:
            print(f"Fatal errpr om reading Input Table from Firebase - {e}")

        return input_table


    def run(self):
        """
        Runs the strategy on 1-min close 
        """
        # sqllite3_conn = sqlite3.connect("Files/Backend.db")
        # while True:
        # os.system('cls')
        # print(f"Running Backend")
        self.clock_time = datetime.now()
        
        # STEP 1 - DATA
        self.data_supply()

        # STEP 2 - UPDATE MARKET BOOK AND ORDER BOOK
        self.update_order_positions()

        # STEP 3 - CHECK FOR SIGNAL AND MATCH QUANTITY
        input_table = self.Read_Input_Table_from_Firebase_Market_Book() # TODO - Change for Trade with BB
        backend_table = self.Read_Backend_Table()
        
        if len(input_table)>0:  # TODO - Changes for Trade with BB
            # SIGNAL CHANGE
            if input_table['Direction']!=backend_table['Direction']:
                if input_table['Direction']!=None and input_table['Direction'] in ['Long', 'Short']:
                    if input_table['Direction'] == 'Long':
                        options_type = 'PE' if input_table['Options_Type']=='SELL' else 'CE'
                    elif input_table['Direction'] == 'Short':
                        options_type = 'PE' if input_table['Options_Type']=='BUY' else 'CE'
                    self.Exit_All_Positions()
                    # ticker = self.Find_Ticker(option_type=options_type, strike=int(input_table['ITM_Range'])) # TODO - Change for Trade with BBB
                    ticker = input_table['Ticker']
                    self.place_order(ticker=ticker,
                                        quantity=int(input_table['Match_Quantity']),
                                        transaction_type=input_table['Options_Type'],
                                        tag=input_table['Direction'])
                    for key_value in [('Direction',input_table['Direction']),
                                        ('Quantity', input_table['Match_Quantity']),
                                        ('Match_Quantity', input_table['Match_Quantity']),
                                        ('Ticker',ticker),
                                        ('Order Time', datetime.now())]:
                        self.Modify_Table(*key_value)
                elif input_table['Direction']!=None and input_table['Direction'] in ['Exit']:
                    self.Exit_All_Positions()
                    for key_value in [('Direction','Exit'),
                                        ('Quantity', 0),
                                        ('Match_Quantity', 0),
                                        ('Ticker',''),
                                        ('Order Time', datetime.now())]:
                        self.Modify_Table(*key_value)
                elif input_table['Direction']!=None:
                    print(f"{datetime.now().strftime('%H:%M:%S')}\tSignal is None") 
            
            # MATCH QUANTITY
            elif backend_table['Direction']!='Exit' and input_table['Match_Quantity'] != backend_table['Match_Quantity']:
                self.Modify_Table('Match_Quantity', input_table['Match_Quantity'])
                qty_difference = int(input_table['Match_Quantity']) - int(backend_table['Quantity'])
                if input_table['Options_Type'] == 'SELL':
                    transaction_type = 'SELL' if qty_difference>0 else 'BUY'
                elif input_table['Options_Type'] == 'BUY':
                    transaction_type = 'BUY' if qty_difference>0 else 'SELL'
                self.place_order(ticker=backend_table['Ticker'],
                                    quantity=abs(int(qty_difference)),
                                    transaction_type=transaction_type,
                                    tag=input_table['Direction'] + " Match Qty")
                self.Modify_Table('Quantity', input_table['Match_Quantity'])
                pass
            
            else:
                print(f"{datetime.now().strftime('%H:%M:%S')}\tSignal is unchanged\tSignal {input_table['Direction']} = Market {backend_table['Direction']}") 
        
        # STEP 4 - PRINT TABLES
        print(f"{datetime.now().strftime('%H:%M:%S')}\tRunning ")
        # self.Print_Tables()

        # STEP 5 - CALCULATE TRADEBOOK
        try:
            self.trade_df = self.order_book[['transaction_type', 'order_timestamp', 'tradingsymbol', 'quantity', 'average_price', 'spot_price', 'tag']]
            self.trade_df['exit_price'] = self.trade_df['average_price'].shift(-1)
            self.trade_df['exit_spot_price'] = self.trade_df['spot_price'].shift(-1)
            self.trade_df['exit_order_timestamp'] = self.trade_df['order_timestamp'].shift(-1)
            self.trade_df['options_points'] = np.where((self.trade_df['tag']=='Short') | (self.trade_df['tag']=='Long') , self.trade_df['exit_price'] - self.trade_df['average_price'], np.nan)
            self.trade_df['pnl'] = self.trade_df['options_points']*self.trade_df['quantity']
            self.trade_df['spot_points'] = np.where(self.trade_df['tag']=='Short',
                                    self.trade_df['spot_price'] - self.trade_df['exit_spot_price'],
                                    np.where(self.trade_df['tag']=='Long',
                                                self.trade_df['exit_spot_price'] - self.trade_df['spot_price'],
                                                np.nan))
            self.trade_df.dropna(inplace=True)
            self.trade_df = self.trade_df[['tag', 'order_timestamp', 'exit_order_timestamp', 'tradingsymbol', 'spot_price', 'exit_spot_price', 'average_price', 'exit_price', 'spot_points', 'options_points', 'pnl']]
            self.trade_df.rename({   'tag': 'Tag', 
                                'order_timestamp': 'Entry Time', 
                                'exit_order_timestamp': 'Exit Time', 
                                'tradingsymbol': 'Instrument', 
                                'spot_price': 'Spot Entry Price', 
                                'exit_spot_price': 'Spot Exit Price', 
                                'average_price': 'Entry Price', 
                                'exit_price': 'Exit Price', 
                                'spot_points': 'Spot Points', 
                                'options_points': 'Options Points', 
                                'pnl' : 'PnL'}, 
                                axis=1, inplace=True)
        except Exception as e:
            print(f"Error in creating Trades Table - {e}")
        
        # STEP 6 - SAVE ORDER BOOK
        try:
            self.order_book.to_csv(f"Order_Book_{datetime.now().strftime('%d_%m_%Y')}.csv")
        except Exception as e:
            print(f"Error in saving Order Book - {e}")
        


    def print_colored_table(self,df):
        color_map = {'highlight': 'red', 'normal': 'white'}
        colors = [color_map['highlight'] if x < 0 else color_map['normal'] for x in df['P&L']]
        print(tabulate(df, headers='keys', tablefmt='fancy_grid', showindex=False, numalign="center", stralign='center',
                    colalign=('center', 'center', 'center', 'center', 'center'),
                    attr=["bgcolor={}".format(color) for color in colors]))

    def Print_Tables(self):
        print(f"\u001b[41;1m                                     Positions                                    \u001b[0m")
        position_table = pd.DataFrame(columns=['Instrument','Qty.','Avg.','LTP','P&L'])
        if len(self.broker_positions)>0:
            for ticker in self.broker_positions:
                position_table.loc[len(position_table)] = [ ticker, 
                                                            self.broker_positions[ticker]['quantity'],
                                                            self.broker_positions[ticker]['average_entry_price'],
                                                            self.broker_positions[ticker]['LTP'],
                                                            self.broker_positions[ticker]['PnL']]
        position_table.sort_values(by='Qty.', ascending=False ,inplace=True)
        position_table.loc[len(position_table)] = [ '', 
                                                    '',
                                                    '',
                                                    'Total',
                                                    self.Net_PnL]

        print(ASCII.BrightYellow,tabulate(position_table, headers = 'keys', tablefmt = 'psql', showindex=False, numalign="center",stralign='center'),ASCII.Reset,"\n")
        
        print_order_book = self.order_book[['order_timestamp','transaction_type','tradingsymbol','quantity','average_price', 'spot_price','tag']].copy()
        
        print_order_book.rename({'order_timestamp'  : 'Time' ,
                                 'transaction_type' : 'Type',
                                 'tradingsymbol'    : 'Instrument',
                                 'quantity'         : 'Qty.',
                                 'average_price'    : 'Avg. Price', 
                                 'spot_price'       : 'Spot Price',
                                 'tag'              : 'Tag'}, axis=1, inplace=True)
        print_order_book.sort_values(by='Time', ascending=False, inplace=True)

        print("\n\n",ASCII.BackgroundBrightWhite,f"                                     Orders                                    ",ASCII.Reset)
        print(ASCII.BrightWhite,tabulate(print_order_book, headers = 'keys', tablefmt = 'psql', showindex=False, numalign="center",stralign='center'),ASCII.Reset,"\n")

    
    def Find_Ticker(self, option_type, strike=0, expiry_type='Weekly', strike_price = None):
        """
        Finds Options Ticker

                Parameters:
                        options_type (str)  : CE or PE
                        strike (int)        : Number of strikes away from ATM strike 
                        expiry_type (str)   : Weekly of Monthly or Next_Week
                        strike_price (float): Specific Strike Price
                
                Returns:
                        ticker (str)        : Option Contract
        """
        Strike_Price = 0
        ticker = ''

        if strike_price == None:
            ltp = self.spot_LTP
        else:
            ltp = strike_price
        
        ATM_Strike_Price = int(ltp/100)*100 if ltp%100 <50 else int(ltp/100)*100 + 100
        ATM_Strike_Price = int(ltp/100)*100 # TODO - Strike Change
        
        if option_type == 'CE':
            Strike_Price = ATM_Strike_Price - strike*100
        elif option_type == 'PE':
            Strike_Price = ATM_Strike_Price + strike*100

        if option_contract_details['Monthly Expiry'] == True:
            ticker = self.symbol + option_contract_details['Year'] + option_contract_details['Month'] +str(Strike_Price) + option_type
        else:
            ticker = self.symbol + option_contract_details['Year'] + option_contract_details['Month Number'] + option_contract_details['Week Date'] + str(Strike_Price) + option_type
        
        response = self.Update_Options_Database(ticker)
        if response=='Ticker does not exist' or response=='Ticker is not tradable':
            return response
        return ticker


    def Update_Options_Database(self, ticker):
        """
        Reads New Options Contract and adds it to options_candle_data_source and Options_candle_data
        """
        try:
            if ticker not in self.options_LTP.keys():
                doc_ref = self.firebase.collection(u'tick_data').document(u'BANKNIFTY')
                db_data = doc_ref.get().to_dict()
                self.options_LTP[ticker] = db_data[ticker]                
        except FileNotFoundError as e:
            return 'Ticker does not exist'


    def Update_Options_Database_old(self, ticker):
        """
        Reads New Options Contract and adds it to options_candle_data_source and Options_candle_data
        """
        try:
            if ticker not in self.options_LTP.keys():
                # ADD TO TICK DATA
                conn = sqlite3.connect(self.options_tick_data_path + ticker + datetime.now().strftime("_%d_%m_%Y.db"))
                tick_data = pd.read_sql_query(f"SELECT * FROM {ticker}", conn)    
                tick_data['Datetime'] = pd.to_datetime(tick_data['exchange_timestamp'],format='%d-%m-%Y %H:%M:%S')
                # TODO
                # tick_data_point = tick_data[tick_data['Datetime']>(self.clock_time-timedelta(seconds=10))].reset_index()
                tick_data_point = tick_data[tick_data['Datetime']>(self.clock_time-timedelta(hours=10))].reset_index()
                LTP = tick_data_point['LTP'].iloc[-1]
                self.options_LTP[ticker] = LTP
        except FileNotFoundError as e:
            return 'Ticker does not exist'


    def place_order( self,
                     ticker = None,
                     order_type = "Market", 
                     quantity = 0,
                     quantity_id = 'Q1',
                     transaction_type  = 'Buy',
                     tag = "" ):
        """
        Updates Order Book for new entries

                Parameters:
                        ticker (str)            : Options Ticker
                        order_type (str)        : Market or Limit Order 
                        quantity (int)          : Quantity of Order
                        quantity_id (str)       : Quantity ID (tag) which can be replaced later
                        tag (str)               : Tag
                        transaction_type (str)  : Buy or Sell
                
                Returns:
                        order_id_list (list)    : Order ID List of All Placed Orders
                        entry_price (float)     : Entry Price for the order
        """
        # TODO - ADD SPOT PRICES
        self.order_id += 1
        order_id_list = []
        self.Update_Options_Database(ticker)
        option_price = self.options_LTP[ticker]
        quantity = abs(quantity) if transaction_type.upper() == 'BUY' else -1*abs(quantity)

        try:
            ltp = self.candle['m1']['Close'].iloc[-1] #SPOT LTP
        except:
            try:
                ltp = self.spot_LTP
            except:
                ltp = self.candle[self.candle_requirement_list[0]]['Close'].iloc[-1]

        if order_type == "Market":
            filled_quantity = 0
            while filled_quantity != abs(quantity):
                if abs(quantity) - filled_quantity > self.quantity_freeze_limit:        
                    order_data = {
                                    'order_timestamp'   : self.clock_time,
                                    'exchange_timestamp': self.clock_time, 
                                    'order_id'          : self.order_id, 
                                    'status'            : "COMPLETE",  
                                    'status_message'    : "", 
                                    'order_type'        : order_type, 
                                    'tradingsymbol'     : ticker, 
                                    'transaction_type'  : transaction_type,  
                                    'quantity'          : self.quantity_freeze_limit, 
                                    'quantity_id'       : quantity_id, 
                                    'average_price'     : option_price, 
                                    'spot_price'        : ltp, 
                                    'filled_quantity'   : self.quantity_freeze_limit, 	
                                    'pending_quantity'  : 0,  
                                    'cancelled_quantity': 0,  
                                    'tag'               : tag,
                                    'trade_pnl'         : 0,
                                    'custom_column1'    : self.custom_orderbook_column1,
                                    'custom_column2'    : self.custom_orderbook_column2,
                                    'custom_column3'    : self.custom_orderbook_column3,
                                }
                    self.order_book = self.order_book.append(order_data, ignore_index = True)
                    filled_quantity += self.quantity_freeze_limit        
                    order_id_list.append(self.order_id)
                    self.order_id += 1
                else:
                    order_data = {
                                    'order_timestamp'   : self.clock_time,
                                    'exchange_timestamp': self.clock_time, 
                                    'order_id'          : self.order_id, 
                                    'status'            : "COMPLETE",  
                                    'status_message'    : "", 
                                    'order_type'        : order_type, 
                                    'tradingsymbol'     : ticker, 
                                    'transaction_type'  : transaction_type,  
                                    'quantity'          : abs(quantity) - filled_quantity,
                                    'quantity_id'       : quantity_id, 
                                    'average_price'     : option_price,
                                    'spot_price'        : ltp, 
                                    'filled_quantity'   : abs(quantity) - filled_quantity, 	
                                    'pending_quantity'  : 0,  
                                    'cancelled_quantity': 0,  
                                    'tag'               : tag,
                                    'trade_pnl'         : 0,
                                    'custom_column1'    : self.custom_orderbook_column1,
                                    'custom_column2'    : self.custom_orderbook_column2,
                                    'custom_column3'    : self.custom_orderbook_column3,
                                }
                    self.order_book = self.order_book.append(order_data, ignore_index = True)
                    filled_quantity += abs(quantity) - filled_quantity       
                    order_id_list.append(self.order_id)

            # UPDATE BROKER POSITIONS
            if ticker in self.broker_positions.keys():
                if self.broker_positions[ticker]['quantity'] == 0:
                    self.broker_positions[ticker]['quantity'] = quantity
                    self.broker_positions[ticker]['average_entry_price'] = option_price
                    self.broker_positions[ticker]['LTP'] = option_price
                elif self.broker_positions[ticker]['quantity'] + quantity == 0:
                    self.broker_positions[ticker]['quantity'] = 0
                    self.broker_positions[ticker]['LTP'] = option_price
                else:
                    self.broker_positions[ticker]['average_entry_price'] = (self.broker_positions[ticker]['average_entry_price']*self.broker_positions[ticker]['quantity'] + option_price*quantity)/(self.broker_positions[ticker]['quantity'] + quantity)                   
                    self.broker_positions[ticker]['quantity'] += quantity
                    self.broker_positions[ticker]['LTP'] = option_price
            else:
                self.broker_positions[ticker] = self.ticker_position_template.copy()
                self.broker_positions[ticker]['quantity'] = quantity
                self.broker_positions[ticker]['average_entry_price'] = option_price
                self.broker_positions[ticker]['previous_LTP'] = option_price
                self.broker_positions[ticker]['LTP'] = option_price

        return order_id_list, option_price

  
    def update_order_positions(self):
        """
        Updates Order Book, Market Book
        """
        if len(self.broker_positions) > 0:
            self.Net_PnL = 0
            for ticker in self.broker_positions.keys():
                self.broker_positions[ticker]['previous_LTP'] = self.broker_positions[ticker]['LTP'] 
                if math.isnan(self.options_LTP[ticker]) == False:
                    self.broker_positions[ticker]['LTP'] = self.options_LTP[ticker]
                    self.broker_positions[ticker]['PnL'] = (self.options_LTP[ticker] - self.broker_positions[ticker]['average_entry_price'])*self.broker_positions[ticker]['quantity']
                    self.broker_positions[ticker]['contract_PnL'] += (self.broker_positions[ticker]['LTP'] - self.broker_positions[ticker]['previous_LTP'])*self.broker_positions[ticker]['quantity']

                    self.Net_PnL += self.broker_positions[ticker]['contract_PnL']
                else:
                    print(f'Issue with data of Ticker {ticker}')

                ticker_column = False
                for column_name in self.broker_market_book.columns:
                    if ticker in column_name:
                        ticker_column = True

                if ticker_column == False:
                    self.broker_market_book[ticker + '_quantity']               = np.nan
                    self.broker_market_book[ticker + '_average_entry_price']    = np.nan
                    self.broker_market_book[ticker + '_PnL']                    = np.nan
                self.broker_market_book.loc[self.clock_time, ticker + '_quantity']              = self.broker_positions[ticker]['quantity']
                self.broker_market_book.loc[self.clock_time, ticker + '_average_entry_price']   = self.broker_positions[ticker]['average_entry_price']
                self.broker_market_book.loc[self.clock_time, ticker + '_LTP']                   = self.broker_positions[ticker]['LTP']
                self.broker_market_book.loc[self.clock_time, ticker + '_PnL']                   = self.broker_positions[ticker]['PnL']
                self.broker_market_book.loc[self.clock_time, ticker + '_contract_PnL']          = self.broker_positions[ticker]['contract_PnL']

        self.broker_market_book.loc[self.clock_time, 'Net_PnL'] = self.Net_PnL
        self.broker_market_book.loc[self.clock_time, 'Strategy_PnL'] = self.Net_PnL + self.capital

        # Write the object to a pickle file
        with open(f"Files/Positions{datetime.now().strftime('_%d_%m_%Y.pickle')}", "wb") as f:
            pickle.dump(self.broker_positions, f)        

        with open(f"Files/Order_Book{datetime.now().strftime('_%d_%m_%Y.pickle')}", "wb") as f:
            pickle.dump(self.order_book, f)           
        
        self.store_dict_in_sqlite(self.broker_positions, f"Positions")
        self.df_to_sqlite_table(self.order_book, f"Order_Book")
        return None


    def Place_OTM_Options_Order(self):
        """
        Places OTM Options Order for 20 strikes away Options
        """
        self.place_order(   ticker             = self.Find_Ticker(option_type='PE',strike=-20),
                            quantity           = abs(self.Quantity),
                            transaction_type   = 'Buy',
                            tag                = 'OTM PE')                   
        self.place_order(   ticker             = self.Find_Ticker(option_type='CE',strike=-20),
                            quantity           = abs(self.Quantity),
                            transaction_type   = 'Buy',
                            tag                = 'OTM CE')    


    def report(self):
        """
        Saves report of backtest
            Saves:
                1. Order Book as .csv
                2. Rules as .txt
        """
        try:
            print("Saving Report Files")
            # SAVE REPORT EXCEL 
            self.broker_market_book = self.broker_market_book.dropna(how='all')

            # SAVING REPORT TO EXCEL ----------------------------------------------------------------------------------------------
            # Create a Pandas Excel writer using XlsxWriter as the engine.
            prefix = os.path.basename(sys.argv[0]).split('_')[0] + '_' + self.report_id 
            suffix = '_' + self.start_date.strftime('%d%b%Y') + '_to_'  + self.end_date.strftime('%d%b%Y')
            self.fname = prefix + '_' +  self.fname + suffix
            savefile_path = self.output_path + self.fname + '_Report' + '.xlsx'
            
            try:
                writer = pd.ExcelWriter(savefile_path, engine='xlsxwriter')
            except:
                try:       
                    writer = pd.ExcelWriter('Output/' + self.fname + '_Report' + '.xlsx', engine='xlsxwriter')
                except:
                    for character_length in range(250, 1,-1):
                        try:
                            savefile_path = self.output_path + self.fname 
                            savefile_path = savefile_path[:character_length] + '_Report' + '.xlsx'
                            writer = pd.ExcelWriter(savefile_path, engine='xlsxwriter')
                            print(f'{ASCII.BrightGreen}Saved at Character Length = {character_length}{ASCII.Reset}')
                            break
                        except:
                            pass   

            # Convert the dataframe to an XlsxWriter Excel object.
            self.complete_order_book.to_excel(writer, sheet_name='OrderBook')
            self.broker_market_book.to_excel(writer, sheet_name='Market_Book')
            self.day_performance_statistics.to_excel(writer, sheet_name='Day Performance Statistics')

            # Get the xlsxwriter workbook and worksheet objects.
            workbook  = writer.book
            worksheet = writer.sheets['Day Performance Statistics']

            # Insert an image.
            try:
                image_file_name = self.plot_line_graph(self.day_performance_statistics, file_name=self.fname, output_location=self.output_path)
                worksheet.insert_image('G3', image_file_name)
                self.Telegram_Send_Photo(image_file_name)
            except Exception as e:
                print(f"Error - {e}")

            # Close the Pandas Excel writer and output the Excel file.
            writer.save()
            # ---------------------------------------------------------------------------------------------------------------------

            # TODO :  Save informative log in either .csv or .txt format

            # SAVE RULES IN .txt FILE
            _file = open(os.getcwd().replace("\\","/") + "/" + self.file_path, 'r')
            Lines = _file.readlines()
            _file.close()

            code_lines = []
            append = True

            for line in Lines:
                if "def rules" in line:
                    append = True
                elif "#end_of_rules" in line:
                    append = True # False
                
                if append:
                    code_lines.append(line)

            try:
                _file = open(self.output_path + self.fname +'.txt', 'w')
                _file.writelines(code_lines[:])
                _file.close()
            except Exception as e:
                print(f"Error in saving Text files - {e}\nAttempting trimming txt filename")
                try:
                    _file = open(self.output_path + self.fname[:150] +'.txt', 'w')
                    _file.writelines(code_lines[:])
                    _file.close()
                except Exception as e:
                        print(f"Error in saving Text files - {e}\nAttempting to change save location of txt file")
                        try:
                            _file = open(self.fname[:150] +'.txt', 'w')
                            _file.writelines(code_lines[:])
                            _file.close()
                        except Exception as e:
                            print(f"Failed to save Text files - {e}")
            print(f'{ASCII.BackgroundBrightGreen}                Saved Report Files              {ASCII.Reset}')
            try:
                self.complete_order_book.to_csv(self.output_path +  self.fname + '_OrderBook.csv') 
            except:
                try:
                    self.complete_order_book.to_csv(self.output_path + self.fname[:110] + '_OrderBook.csv')        
                except:
                    self.complete_order_book.to_csv(self.fname[:110] + '_OrderBook.csv')        

            if self.azure_storage_upload:
                try:
                    azure_file_name = prefix + '.csv'
                    self.complete_order_book.to_csv(self.output_path + azure_file_name)
                    blob = BlobClient.from_connection_string(conn_str=self.azure_connection_string, container_name=self.azure_container_name, blob_name=azure_file_name)
                    with open(self.output_path + azure_file_name, "rb") as data:
                        blob.upload_blob(data)                
                except Exception as e:
                    print("Error with uploading reports to Azure Blob Storage = {e}")
        
        except Exception as e:
            print(f"Error while saving report - {e}")
            try:
                self.complete_order_book.to_csv(self.fname[:100] + '_OrderBook.csv')
                self.broker_market_book.to_csv(self.fname[:100] + '_Market_Book.csv')
                self.day_performance_statistics.to_csv(self.fname[:100] +'_Day_Performance_Statistics.csv')            
            except Exception as e:
                print(f"Error in saving CSV files - {e}")
            print(f'{ASCII.BackgroundBrightRed} Failed to save Report Files {ASCII.Reset}')
            pdb.set_trace()
            print(f'{ASCII.BackgroundBrightWhite} Press continue one more time to quit {ASCII.Reset}')
            pdb.set_trace()


    def Brokerage_and_Commission_Single_Trade(self, buy_price, sell_price, quantity):
        """
        Calculate Brokerage and Commission on NFO

                Parameters:
                        buy_price (float)   : Buying Price
                        sell_price (float)  : Selling Price
                        quantity (int)      : Quantity traded
                
                Returns:
                        total_chrgs (float)  : Total Brokerage and Charges
        """
        brokerage   = 40
        stt         = 0.0005
        txc_cost    = 0.00053
        gst_perc    = 0.18
        sebi_charg  = 0.000001
        stamp_chrg  = 0.00003
        lot_traded  = quantity/self.lot_size

        if lot_traded > 48:
            total_brokerage = (int(lot_traded/48) + 1) * brokerage
        else:
            total_brokerage = brokerage
        
        stt_opt     = sell_price * lot_traded * self.lot_size * stt
        trnscsn_opt = (sell_price + buy_price) * lot_traded * self.lot_size * txc_cost
        gst_charge  = (brokerage + trnscsn_opt) * gst_perc
        sebi_chrgs  = (sell_price + buy_price) * lot_traded * self.lot_size * sebi_charg
        stamp_chrgs = buy_price * lot_traded * self.lot_size * stamp_chrg
        total_chrgs = total_brokerage + stt_opt + trnscsn_opt + gst_charge + sebi_chrgs + stamp_chrgs
        # print(f"stt_opt -  {stt_opt} transction - {trnscsn_opt} gst charge - {gst_charge} sebi charge {sebi_charg} stamp charge {stamp_chrg} total brokerage {total_brokerage}")
        
        return total_chrgs


    def Brokerage_and_Commission_Order_Book(self):
        """
        Calculate Brokerage for Entire Order Book
        """
        ORDER_DF = self.order_book
        # date = str(ORDER_DF['exchange_timestamp'].iloc[0]).split(" ")[0]
        values = ORDER_DF.average_price * ORDER_DF.quantity
        ORDER_DF['Values'] = values.where(ORDER_DF.transaction_type.str.upper() == 'SELL', other=-values)
        ORDER_DF['stt_val_opt_SELL'] = values.where(ORDER_DF.transaction_type.str.upper() == 'SELL', other=0)
        ORDER_DF['stt_val_opt_BUY'] = values.where(ORDER_DF.transaction_type.str.upper() == 'BUY', other=0)
        ORDER_DF['amount'] = values
        
        try:
            ORDER_DF['order_split_units'] = np.where(ORDER_DF.quantity%self.default_freeze_quantity!=0, ORDER_DF.quantity//self.default_freeze_quantity+1, ORDER_DF.quantity//self.default_freeze_quantity)
            # count= int(ORDER_DF['status'].value_counts()['COMPLETE'])
            count = ORDER_DF['order_split_units'].sum()
        except:
            count = 0 
                
        total_val_opt = ORDER_DF['amount'].sum()
        sell_val_opt =  ORDER_DF['stt_val_opt_SELL'].sum()
        buy_val_opt =  ORDER_DF['stt_val_opt_BUY'].sum()

        gross_pnl = ORDER_DF['Values'].sum()
        brokerage = count * 20
        stt_otp = sell_val_opt * 0.0005
        trnscsn_opt = total_val_opt * 0.00053
        GST = (brokerage + trnscsn_opt) * 0.18
        sebi_chrgs = total_val_opt * 0.00000118
        stamp_chrgs = buy_val_opt * 0.00003

        total_chrgs = brokerage + stt_otp + trnscsn_opt + GST + sebi_chrgs + stamp_chrgs
        net_pnl = gross_pnl - total_chrgs
        
        # ORDER_DF.to_excel('Output/' + self.fname + "_OrderBook_wBROKERAGE" + datetime.strftime(datetime.now(),"%H_%M_%S") + ".xlsx")
        return total_chrgs


    def PnL_Brokerage_and_Commission_Single_Order_Book(order_book, default_freeze_quantity=1200):
        """
        Calculate Brokerage for Single Order Book and returns PnL, Brokerage, Commission
        """
        ORDER_DF = order_book
        # date = str(ORDER_DF['exchange_timestamp'].iloc[0]).split(" ")[0]

        values = ORDER_DF.average_price * ORDER_DF.quantity
        ORDER_DF['Values'] = values.where(ORDER_DF.transaction_type == 'Sell', other=-values)
        ORDER_DF['stt_val_opt_SELL'] = values.where(ORDER_DF.transaction_type == 'Sell', other=0)
        ORDER_DF['stt_val_opt_BUY'] = values.where(ORDER_DF.transaction_type == 'Buy', other=0)
        ORDER_DF['amount'] = values
        
        try:
            ORDER_DF['order_split_units'] = np.where(ORDER_DF.quantity%default_freeze_quantity!=0, ORDER_DF.quantity//default_freeze_quantity+1, ORDER_DF.quantity//default_freeze_quantity)
            # count= int(ORDER_DF['status'].value_counts()['COMPLETE'])
            count = ORDER_DF['order_split_units'].sum()
        except:
            count = 0 
                
        total_val_opt = ORDER_DF['amount'].sum()
        sell_val_opt =  ORDER_DF['stt_val_opt_SELL'].sum()
        buy_val_opt =  ORDER_DF['stt_val_opt_BUY'].sum()

        gross_pnl = ORDER_DF['Values'].sum()
        brokerage = count * 20
        stt_otp = sell_val_opt * 0.0005
        trnscsn_opt = total_val_opt * 0.00053
        GST = (brokerage + trnscsn_opt) * 0.18
        sebi_chrgs = total_val_opt * 0.000001
        stamp_chrgs = buy_val_opt * 0.00003

        total_chrgs = brokerage + stt_otp + trnscsn_opt + GST + sebi_chrgs + stamp_chrgs
        net_pnl = gross_pnl - total_chrgs
        
        return gross_pnl, total_chrgs


    def Exit_All_Positions(self, position_type = 'All'):
        """
        Exit all existing position

                Parameters:
                        position_type (str) : Buy / Sell / All
        """
        for ticker in self.broker_positions.keys():
            tag = '_LONG_EXIT' if self.broker_positions[ticker]['quantity'] > 0 else '_SHORT_EXIT'
            tag = 'PE'+tag if 'PE' in ticker else 'CE'+tag 
            tag = 'Exit'
            if position_type == 'All':
                if self.broker_positions[ticker]['quantity'] != 0:
                    self.place_order(ticker            = ticker,
                                    quantity           = abs(self.broker_positions[ticker]['quantity']),
                                    transaction_type   = 'Buy' if self.broker_positions[ticker]['quantity']<0 else 'Sell',
                                    tag                = tag)   
            elif position_type == 'Buy':
                if self.broker_positions[ticker]['quantity'] > 0:
                    self.place_order(ticker            = ticker,
                                    quantity           = abs(self.broker_positions[ticker]['quantity']),
                                    transaction_type   = 'Buy' if self.broker_positions[ticker]['quantity']<0 else 'Sell',
                                    tag                = tag)   
            elif position_type == 'Sell':
                if self.broker_positions[ticker]['quantity'] < 0:
                    self.place_order(ticker            = ticker,
                                    quantity           = abs(self.broker_positions[ticker]['quantity']),
                                    transaction_type   = 'Buy' if self.broker_positions[ticker]['quantity']<0 else 'Sell',
                                    tag                = tag)   






class ASCII:
    HEADER                      = '\033[95m'
    OKBLUE                      = '\033[94m'
    OKCYAN                      = '\033[96m'
    OKGREEN                     = '\033[92m'
    WARNING                     = '\033[93m'
    FAIL                        = '\033[91m'
    ENDC                        = '\033[0m'
    BOLD                        = '\033[1m'
    UNDERLINE                   = '\033[4m'
    RESET_CMD                   = '\u001b[0m'
    Black                       = '\u001b[30m'
    Red                         = '\u001b[31m'
    Green                       = '\u001b[32m'
    Yellow                      = '\u001b[33m'
    Blue                        = '\u001b[34m'
    Magenta                     = '\u001b[35m'
    Cyan                        = '\u001b[36m'
    White                       = '\u001b[37m'
    Reset                       = '\u001b[0m'
    BackgroundBlack             = '\u001b[40m'
    BackgroundRed               = '\u001b[41m'
    BackgroundGreen             = '\u001b[42m'
    BackgroundYellow            = '\u001b[43m'
    BackgroundBlue              = '\u001b[44m'
    BackgroundMagenta           = '\u001b[45m'
    BackgroundCyan              = '\u001b[46m'
    BackgroundWhite             = '\u001b[47m'
    BackgroundBrightBlack       = '\u001b[40;1m'
    BackgroundBrightRed         = '\u001b[41;1m'
    BackgroundBrightGreen       = '\u001b[42;1m'
    BackgroundBrightYellow      = '\u001b[43;1m'
    BackgroundBrightBlue        = '\u001b[44;1m'
    BackgroundBrightMagenta     = '\u001b[45;1m'
    BackgroundBrightCyan        = '\u001b[46;1m'
    BackgroundBrightWhite       = '\u001b[47;1m'
    BrightBlack                 = '\u001b[30;1m'
    BrightRed                   = '\u001b[31;1m'
    BrightGreen                 = '\u001b[32;1m'
    BrightYellow                = '\u001b[33;1m'
    BrightBlue                  = '\u001b[34;1m'
    BrightMagenta               = '\u001b[35;1m'
    BrightCyan                  = '\u001b[36;1m'
    BrightWhite                 = '\u001b[37;1m'   







"""
    def Hedge_Ticker_List(self, number_of_strikes=2):
        
        Updates Hedge Market Book 
        
        Parameters:
            number_of_strikes (int)     : Number of Strikes above and below ATM Strikes
        
        Returns:
            hedge_ticker_list (list)    : List of Tickers for Hedge Position
        
        # TODO - Understand Current Logic of Hedged Ticker
        ltp = self.Get_LTP_Multiple_Sources()
        ATM_strike = int(ltp/100)*100
        hedge_ticker_list = []
        
        for i in range(0, int(number_of_strikes)):
            for option_type in ['CE', 'PE']:
                for strike_side in [1, -1]:
                    hedge_ticker_list.append(
                                            self.Get_Instrument(strike=ATM_strike + 100*i*strike_side,
                                                                opt_type=option_type)       
                                            )
        
        return hedge_ticker_list



    def Read_Hedge_Market_Book(self):
        
        Reads Hedge Market Book
        
        self.Hedge_Market_Book    = {}
        try:
            doc_ref = self.firebase_db.collection('BB_Hedge').document('Market_Book')
            _doc    = doc_ref.get()
            if _doc.exists:
                self.Hedge_Market_Book = _doc.to_dict()
                logging.info(f"Read Hedge Market Book = {self.Hedge_Market_Book}")
            else:
                logging.info('Hedge Market Book Document does not exist')
        except Exception as e:
            logging.error(f"Error in Reading Hedge Market Book - {e}")   


    def Update_Hedge_Market_Book(self, ticker, quantity):
        
        Updates Hedge Market Book 
        
        Parameters:
            ticker (str)    : Options Contract
            quantity (int)  : Quantity
                
        # TODO
        pass

"""