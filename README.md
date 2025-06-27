# TradeSimulator

A Python-based trade simulator that emulates a real-time trading environment — similar to **Zerodha Kite** — for placing and managing trades on **NSE-like** markets. It is ideal for strategy testing, mock trading, and UI/backend prototyping without using real capital or APIs.


## Features

* Simulated trading environment with exchange-like behavior
* Place and manage **Market**, **Limit**, and **Stop** orders
* Track positions, virtual balance, PnL, and trade history
* Works on historical candle/tick data (CSV format)
* Mimics order placement experience of platforms like **Zerodha Kite**
* Lightweight and customizable core engine


## Folder Structure (Modular)

```
tradesimulator/
├── main.py                 # Entry point
├── config/settings.py      # Initial capital, fees, etc.
├── data/sample_data.csv    # Historical price data
├── engine/simulator.py     # Core execution logic
├── orders/order_manager.py # Place, modify, cancel orders
├── portfolio/account.py    # Balance, PnL tracking
├── utils/logger.py         # Logging and utilities
└── tests/test_simulator.py # Unit tests


You can define your trade parameters (like price, order type, quantity) via script or CLI.

## Use Cases

* Paper trading without capital or live account
* Backtesting simple trade logic and position management
* Prototyping trading bots without using broker APIs
* Educational sandbox for understanding order behavior


## Future Scope

* Web UI for interactive trading
* Strategy plug-in interface
* Risk analysis module
* Real-time simulation from live feeds (optional)
