import asyncio
import ccxt.async as ccxt
import time
import pandas as pd
from datetime import datetime
import os, sys, time, traceback
import logging
import glob
import numpy as np
from ccxt.base import errors

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root + '/python')

RUNONGEMINI = False

if RUNONGEMINI == False:
    # mac:
    savedir = '/Documents/Python/CryptoBot/data/'
    logdir = '/Documents/Python/CryptoBot/logs/'
else:
    # gemini:
    savedir = '/home/gemini/CryptoArb/data/'
    logdir = '/home/gemini/CryptoArb/logs/'

# Logger for this module
log = logging.getLogger(__name__)
log_format = '%(asctime)s %(levelname)-5.5s [%(name)s-%(funcName)s:%(lineno)d][%(threadName)s] %(message)s'
log_filename = datetime.now().strftime('SpreadFinder.%Y%m%d-%H%M%S.log')
logfile = logdir + log_filename
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', filename=logfile, level=logging.INFO)


starttime = time.time()
hitbtc = ccxt.hitbtc({
    'apiKey': '',
    'secret': ''
})

kucoin = ccxt.kucoin({
    'apiKey': '',
    'secret': ''
})

binance = ccxt.binance({
    'apiKey': '',
    'secret': ''
})

poloniex = ccxt.poloniex({
    'apiKey': '',
    'secret': ''
})

okex = ccxt.okex()
okex.name = 'okex'
huobi = ccxt.huobi()


symbol = 'NEO/ETH'
exchanges = [hitbtc, binance, kucoin, okex]
ioloop = asyncio.get_event_loop()

bids_df = pd.DataFrame()
asks_df = pd.DataFrame()

bids_filename = savedir + datetime.now().strftime('SpreadFinder_BID.%Y%m%d-%H%M%S.csv')
asks_filename = savedir + datetime.now().strftime('SpreadFinder_ASK.%Y%m%d-%H%M%S.csv')

cn = ['Time']

for e in exchanges[:]:
    cn.append(e.name)
    cn.append(e.name + "_vol")

pd.DataFrame([cn]).to_csv(bids_filename, index=False, header=False)
pd.DataFrame([cn]).to_csv(asks_filename, index=False, header=False)

async def get_orders(exchange, symbol):
    k = {}
    try:
        k[exchange] = await exchange.fetch_order_book(symbol)
    except (errors.ExchangeNotAvailable, errors.ExchangeError):
        log.error(' Ooops! %s', exchange.name, ' is not available.')
        k = {}
        return k
    except errors.RequestTimeout:
        log.error("Exchange %s Timeout: ", exchange.name)
        k = {}
        return k
    return k


async def poll():
    global exchanges
    global starttime
    global bids_df
    global asks_df

    while True:
        starttime = time.time()
        k = {}
        await asyncio.sleep(max(e.rateLimit for e in exchanges) / 1000)

        futures = [get_orders(exchange, symbol) for exchange in exchanges]
        for future in (asyncio.as_completed(futures)):
            s = await future
            if len(s) > 0:
                k.update(s)

        ts = pd.Timestamp(datetime.now())
        bids = []
        asks = []
        try:
            for e in exchanges[:]:
                bids.append(k[e]['bids'][0][0])
                bids.append(k[e]['bids'][0][1])
                asks.append(k[e]['asks'][0][0])
                asks.append(k[e]['asks'][0][1])
            #bids_df = bids_df.append(pd.DataFrame([bids], index=[ts] ))
            #asks_df = asks_df.append(pd.DataFrame([asks], index=[ts] ))
            # bids_df = bids_df.append(pd.DataFrame([bids], index=[ts], columns=cn))
            # asks_df = asks_df.append(pd.DataFrame([asks], index=[ts], columns=cn))
            # bids_df.to_csv(bids_filename, index=True)
            # asks_df.to_csv(asks_filename, index=True)

            pd.DataFrame([asks], index=[ts]).to_csv(asks_filename, mode='a', index=True, header=False)
            pd.DataFrame([bids], index=[ts]).to_csv(bids_filename, mode='a', index=True, header=False)

        except (IndexError, KeyError):
            continue


ioloop.run_until_complete(poll())
ioloop.close()

