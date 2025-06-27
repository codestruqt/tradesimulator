# TradeSim User Input

from TradeSim import *
from Configuration import *
from Configuration import options_type

# ORDERS CLASS AND FUNCTION -------------------------------------------------
TradeSim_User_Input = User_Input(          
                                instrument     = 'BANKNIFTY',
                                options_type   = options_type['options_type']
                                )   

TradeSim_User_Input.Main_Function()
# ---------------------------------------------------------------------------