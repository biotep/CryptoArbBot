# CryptoArbBot

The script polls ETHUSDT prices from two exchanges, Kucoin and Binance. Whenever the spread (price difference from ask and bid in this case between exchanges) are wide enough - counting in the expected commissions - and our available balance would cover the estimated trade + commisssions,  two order will be sent, a BUY and SELL.

Then, we check the status of the orders, and if both executed then we generate a status email of the profit. 

If any of the orders not got executed, just sitting in the book, then the order will be cancelled, and we also need to  initiate a new trade on the successfully traded side, to revert back the balance so availble fund will be there when the next opportunity would come.

AH.


