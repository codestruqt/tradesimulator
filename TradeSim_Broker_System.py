# VERSION 26.02-1
#       - Addition to incorporate Trades and Trades with BB

# VERSION HISTORY 
#       15.02-1 - Pilot

import dash
import pandas as pd
import numpy as np
from dash.dependencies import Input, Output
import random
import time
from dash import Dash , html, dcc, dash_table
import sqlite3
import ast
from TradeSim import Broker_System
import pdb
import warnings
warnings.simplefilter("ignore")

zerodha = Broker_System()
print(f"Starting Dash App")

app = dash.Dash()

header = html.Div([
    html.Div([
        html.Img(src=app.get_asset_url("kite_white.png"), height='40px')
    ], style={'float': 'left'}),
    html.Div([
        html.A('Dashboard', href='/menu1', style={'marginRight': '20px', 'font-family': 'Arial','text-decoration': 'none', 'color': '#444444', 'font-size' : '12px'}),
        html.A('Orders', href='/menu2', style={'marginRight': '20px', 'font-family': 'Arial','text-decoration': 'none', 'color': '#444444', 'font-size' : '12px'}),
        html.A('Holdings', href='/menu3', style={'marginRight': '20px', 'font-family': 'Arial','text-decoration': 'none', 'color': '#444444', 'font-size' : '12px'}),
        html.A('Positions', href='/menu4', style={'marginRight': '20px', 'font-family': 'Arial','text-decoration': 'none', 'color': '#444444', 'font-size' : '12px'}),
        html.A('Funds', href='/menu4', style={'marginRight': '20px', 'font-family': 'Arial','text-decoration': 'none', 'color': '#444444', 'font-size' : '12px'}),
        html.A('Apps', href='/menu4', style={'marginRight': '20px', 'font-family': 'Arial','text-decoration': 'none', 'color': '#444444', 'font-size' : '12px'}),
        # html.Img(src=app.get_asset_url("kite_white.png"), height='40px'),
        html.A('XX0711', href='/menu5', style={'marginRight': '20px', 'font-family': 'Arial','text-decoration': 'none', 'color': '#444444', 'font-size' : '12px'})
    ], style={'float': 'right', 'height': '40px', 'lineHeight': '40px', 'font-family': 'Arial'}),
], style={'height': '40px', 'backgroundColor': '#f5f5f5'})


app.layout =  html.Div([header, 
                html.Div(children=[
                                # html.H1(children='Table 1'),
                                html.Div(id='positions'),
                                html.Hr(),
                                html.Div(id='trades'),
                                html.Hr(),
                                html.Div(id='orders'),
                                dcc.Interval(id='interval-component', interval=1000, n_intervals=0)
                                ]),
            ])

@app.callback([Output('positions', 'children'),
               Output('trades', 'children'),
               Output('orders', 'children')],
              [Input('interval-component', 'n_intervals')])
def update_table_1(n):

    zerodha.run()

    # READ EXISTING DATA OF DIFFERENT VARIABLES
    # sqllite3_conn = sqlite3.connect("Files/Backend.db")
    position_table = pd.DataFrame(columns=['Instrument','Qty','Avg.','LTP','P&L'])
    trades_table = pd.DataFrame(columns=['Instrument','Qty','Avg.','LTP','P&L'])
    try:
        # position_table_sql = pd.read_sql_query(f"SELECT * from Positions", sqllite3_conn) 
        # position_dict = {}
        # if len(position_table_sql)>0:
        #     for i in position_table_sql.index:
        #         position_dict[position_table_sql['key'].iloc[i]] = ast.literal_eval(position_table_sql['value'].iloc[i])
        position_dict = zerodha.broker_positions.copy()
        
        if len(position_dict)>0:
            for ticker in position_dict:
                position_table.loc[len(position_table)] = [ ticker, 
                                                            position_dict[ticker]['quantity'],
                                                            position_dict[ticker]['average_entry_price'],
                                                            position_dict[ticker]['LTP'],
                                                            position_dict[ticker]['PnL']]
        position_table.sort_values(by='Qty', ascending=False ,inplace=True)
        position_table['Avg.'] = position_table['Avg.'].round(2)
        position_table['P&L'] = position_table['P&L'].astype(int)
        position_table['Qty'] = position_table['Qty'].astype(int)
        
        try:
            brokerage_and_charges = zerodha.Brokerage_and_Commission_Order_Book()
        except:
            brokerage_and_charges = 0

        # position_table.loc[len(position_table)] = [ '', 
        #                                             '',
        #                                             '',
        #                                             'Brokerage',
        #                                             int(brokerage_and_charges)]

        position_table.loc[len(position_table)] = [ f'Brokerage = {int(brokerage_and_charges)}', 
                                                    '',
                                                    '',
                                                    'Total',
                                                    int(zerodha.Net_PnL)]
    except:
        pass


    try:
        # orders_table = pd.read_sql_query(f"SELECT * from Order_Book", sqllite3_conn) 
        orders_table = zerodha.order_book.copy()
        if len(orders_table)>0:
            orders_table = orders_table[['order_timestamp','transaction_type','tradingsymbol','quantity','average_price', 'spot_price','tag']]
            orders_table.rename({'order_timestamp'  : 'Time' ,
                                    'transaction_type' : 'Type',
                                    'tradingsymbol'    : 'Instrument',
                                    'quantity'         : 'Qty',
                                    'average_price'    : 'Avg. Price', 
                                    'spot_price'       : 'Spot Price',
                                    'tag'              : 'Tag'}, axis=1, inplace=True)
            orders_table.sort_values(by='Time', ascending=False, inplace=True)
            orders_table['Type'] = np.where(orders_table['Type']=='Sell','SELL',orders_table['Type'])
            orders_table['Time'] = pd.to_datetime(orders_table['Time'])
            orders_table['Time'] = orders_table['Time'].dt.strftime("%H:%M:%S")
        else:
            orders_table = pd.DataFrame(columns=['order_timestamp','transaction_type','tradingsymbol','quantity','average_price', 'spot_price','tag'])
    except Exception as e:
        print(f"Issue in Orders table -{e}")
    
    position_dash_table = html.Div( html.Div([ html.Div(html.Div( html.H2('Positions'), 
                                                        style = {   
                                                                    'color'             :'#666666',
                                                                    'backgroundColor'   : '#FFFFFF',                                                                                 
                                                                    'font-size'         : '12px',
                                                                    'textAlign'         : 'left',
                                                                    'font-family'       : 'Arial'}),
                                            style={'margin-left':  '10px'} ),
                                    dash_table.DataTable( position_table.to_dict('records'), [{"name": i, "id": i} for i in position_table.columns],
                                                        style_cell = {'textAlign': 'center',
                                                                        'padding': '3px',
                                                                        'font-size': '12px',
                                                                        'backgroundColor': '#FCFCFC',
                                                                        'color': '#444444',
                                                                        'font-family': 'Arial',
                                                                        # 'border': 'none'
                                                                        },

                                                        style_header = {'backgroundColor': '#FFFFFF',
                                                                        'color': '#000000',
                                                                        # 'fontWeight': 'bold',
                                                                        'font-size': '10px' ,
                                                                        'border': '#FFFFFF', 
                                                                        'font-family': 'Arial',
                                                                        },

                                                        style_data_conditional=[
                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{Qty} < 0',
                                                                                'column_id'     : 'Qty'
                                                                                },
                                                                                'color'         : 'tomato',
                                                                                'fontWeight'    : 'regular'
                                                                    },                                                                    
                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{Qty} > 0',
                                                                                'column_id'     : 'Qty'
                                                                                },
                                                                                'color'         : '#44b884',
                                                                                'fontWeight'    : 'regular'
                                                                    },                                                                    
                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{Qty} eq 0',
                                                                                'column_id'     : position_table.columns.values.tolist()
                                                                                },
                                                                                'color'         : '#C9C9C9',
                                                                                'fontWeight'    : 'regular'
                                                                    },                                                                    
                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{P&L} < 0',
                                                                                'column_id'     : 'P&L',
                                                                                'row_index': len(position_table)-1
                                                                                },
                                                                                'color'         : 'tomato',
                                                                                'fontWeight'    : 'bold'
                                                                    },                                                                    
                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{P&L} > 0',
                                                                                'column_id'     : 'P&L',
                                                                                'row_index': len(position_table)-1
                                                                                },
                                                                                'color'         : '#44b884',
                                                                                'fontWeight'    : 'bold'
                                                                    },                                                                       
                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{HB_2} contains true',
                                                                                'column_id'     : 'HB_2'
                                                                                },
                                                                                'color'         : '#FFFF00',
                                                                                'fontWeight'    : 'regular'
                                                                    },                                                                    
                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{HC_1} contains true',
                                                                                'column_id'     : 'HC_1'
                                                                                },
                                                                                'color'         : '#FFFF00',
                                                                                'fontWeight'    : 'regular'
                                                                    },                                                                    
                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{HC_2} contains true',
                                                                                'column_id'     : 'HC_2'
                                                                                },
                                                                                'color'         : '#FFFF00',
                                                                                'fontWeight'    : 'regular'
                                                                    },                                                                    

                                                            ]
                                                            )], 
                                    style = {   'color'         : '#a39898',
                                                'height'        : '400px', 
                                                'margin-left'   : '15px'} 
                                )
                        )  

    try:
        trades_table = zerodha.trade_df.copy()
        trades_table['Entry Time'] = trades_table['Entry Time'].dt.strftime("%H:%M:%S")
        trades_table['Exit Time'] = trades_table['Exit Time'].dt.strftime("%H:%M:%S")
        trades_table.sort_values(by='Entry Time', ascending=False, inplace=True)
    except Exception as e:
        print(f"Error in fetching trades table - {e}")

    trades_dash_table = html.Div( html.Div([ html.Div(html.Div( html.H2('Trades'), 
                                                        style = {   
                                                                    'color'             :'#666666',
                                                                    'backgroundColor'   : '#FFFFFF',                                                                                 
                                                                    'font-size'         : '12px',
                                                                    'textAlign'         : 'left',
                                                                    'font-family'       : 'Arial'}),
                                            style={'margin-left':  '10px'} ),
                                    dash_table.DataTable( trades_table.to_dict('records'), [{"name": i, "id": i} for i in trades_table.columns],
                                                        style_cell = {'textAlign': 'center',
                                                                        'padding': '3px',
                                                                        'font-size': '12px',
                                                                        'backgroundColor': '#FCFCFC',
                                                                        'color': '#444444',
                                                                        'font-family': 'Arial',
                                                                        # 'border': 'none'
                                                                        },

                                                        style_header = {'backgroundColor': '#FFFFFF',
                                                                        'color': '#000000',
                                                                        # 'fontWeight': 'bold',
                                                                        'font-size': '10px' ,
                                                                        'border': '#FFFFFF', 
                                                                        'font-family': 'Arial',
                                                                        },

                                                        style_data_conditional=[
                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{PnL} < 0',
                                                                                'column_id'     : 'PnL'
                                                                                },
                                                                                'color'         : 'tomato',
                                                                                'fontWeight'    : 'regular'
                                                                    },                                                                    
                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{PnL} > 0',
                                                                                'column_id'     : 'PnL'
                                                                                },
                                                                                'color'         : '#44b884',
                                                                                'fontWeight'    : 'regular'
                                                                    },                                                                    

                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{Tag} eq Short',
                                                                                'column_id'     : 'Tag'
                                                                                },
                                                                                'color'         : 'tomato',
                                                                                'fontWeight'    : 'regular'
                                                                    },                                                                    
                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{Tag} eq Long',
                                                                                'column_id'     : 'Tag'
                                                                                },
                                                                                'color'         : '#44b884',
                                                                                'fontWeight'    : 'regular'
                                                                    },  

                                                            ]
                                                            )], 
                                    style = {   'color'         : '#a39898',
                                                'height'        : '400px', 
                                                'margin-left'   : '15px'} 
                                )
                        )  


    orders_dash_table = html.Div( html.Div([ html.Div(html.Div( html.H2('Orders'), 
                                                        style = {   
                                                                    'color'             :'#666666',
                                                                    'backgroundColor'   : '#FFFFFF',                                                                                 
                                                                    'font-size'         : '12px',
                                                                    'textAlign'         : 'left',
                                                                    'font-family'       : 'Arial'}),
                                            style={'margin-left':  '10px'} ),
                                    
                                    dash_table.DataTable( orders_table.to_dict('records'), [{"name": i, "id": i} for i in orders_table.columns],
                                                        style_cell = {'textAlign': 'center',
                                                                        'padding': '3px',
                                                                        'font-size': '12px',
                                                                        'backgroundColor': '#FCFCFC',
                                                                        'color': '#444444',
                                                                        'font-family': 'Arial',
                                                                        },

                                                        style_header = {'backgroundColor': '#FFFFFF',
                                                                        'color': '#8F8F8F',
                                                                        # 'fontWeight': 'bold',
                                                                        'font-size': '10px' ,
                                                                        'border': '#FFFFFF', 
                                                                        'font-family': 'Arial',
                                                                        },
                                                        style_data_conditional=[
                                                                           
                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{Type} eq BUY',
                                                                                'column_id'     : 'Type'
                                                                                },
                                                                                'backgroundColor': '#ECF2FE',
                                                                                'color'         : '#9AA2F4',
                                                                                'fontWeight'    : 'regular',
                                                                                'font-size': '10px',
                                                                    },                                                                    
                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{Type} eq SELL',
                                                                                'column_id'     : 'Type'
                                                                                },
                                                                                'backgroundColor' : '#FCEDED',
                                                                                'color'         : '#E59490',
                                                                                'fontWeight'    : 'regular',
                                                                                'font-size': '10px',
                                                                    },                                                                    
                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{last_price} contains P&L && {pnl} > 0',
                                                                                'column_id'     : 'pnl'
                                                                                },
                                                                                'color'         : '#44b884',
                                                                                'fontWeight'    : 'regular'
                                                                    },                                                                    
                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{HB_2} contains true',
                                                                                'column_id'     : 'HB_2'
                                                                                },
                                                                                'color'         : '#FFFF00',
                                                                                'fontWeight'    : 'regular'
                                                                    },                                                                    
                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{HC_1} contains true',
                                                                                'column_id'     : 'HC_1'
                                                                                },
                                                                                'color'         : '#FFFF00',
                                                                                'fontWeight'    : 'regular'
                                                                    },                                                                    
                                                                    {
                                                                        'if':   {
                                                                                'filter_query'  : '{HC_2} contains true',
                                                                                'column_id'     : 'HC_2'
                                                                                },
                                                                                'color'         : '#FFFF00',
                                                                                'fontWeight'    : 'regular'
                                                                    },                                                                    

                                                            ]
                                                            )], 
                                    style = {   'color'         : '#a39898',
                                                'height'        : '400px', 
                                                'margin-left'   : '15px'} 
                                )
                        )  

    return position_dash_table, trades_dash_table, orders_dash_table


if __name__ == '__main__':
    app.run_server(debug=True)
