import os

# OPTIONS TYPE ----------------------------------------------------------------------------------------------------------------
options_type    = {'options_type' :'BUY'}
# -----------------------------------------------------------------------------------------------------------------------------

# OUTPUT SAVING LOCATION ------------------------------------------------------------------------------------------------------
option_contract_details =       {
                                'Month'         : 'MAR',
                                'Month Number'  : '3',
                                'Monthly Expiry': False,
                                'Week Date'     : '02',
                                'Year'          : '23'
                                }
# -----------------------------------------------------------------------------------------------------------------------------

# MARKET DATA LOCATIONS -------------------------------------------------------------------------------------------------------
market_data_path = {
                    'HP-PC'         : { 'index_candle_data_path'    : 'Market Data/',
                                        'index_tick_data_path'      : 'Market Data/',
                                        'index_custom_tf_data_path' : 'Market Data/',
                                        'options_candle_data_path'  : 'Market Data/',
                                        'options_tick_data_path'    : 'Market Data/',},
                    'PREDATOR'      : { 'index_candle_data_path'    : '',
                                        'index_tick_data_path'      : '',
                                        'options_candle_data_path'  : '',
                                        'options_tick_data_path'    : '',},
                    'COOLERMASTER'  : { 'index_candle_data_path'    : 'Market Data/',
                                        'index_tick_data_path'      : 'Market Data/',
                                        'options_candle_data_path'  : 'Market Data/',
                                        'options_tick_data_path'    : 'Market Data/',},
                    }
# -----------------------------------------------------------------------------------------------------------------------------

# OUTPUT SAVING LOCATION ------------------------------------------------------------------------------------------------------
output_path =       {
                    'HP-PC'         : '',
                    'PREDATOR'      : '',
                    'COOLERMASTER'  : ''
                    }
# -----------------------------------------------------------------------------------------------------------------------------