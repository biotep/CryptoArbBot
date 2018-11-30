import os, sys, time, traceback
from ccxt.base import errors
from datetime import datetime
import pandas as pd
import glob
import logging
import asyncio
import ccxt.async as ccxt
import time
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root + '/python')
starttime = time.time()
endtime = time.time()



savedir = '/Python/CryptoBot/data/'
logdir = '/Python/CryptoBot/logs/'


column_names = (
    "BID_EXC", "ASK_EXC", "BID_PRICE", "ASK_PRICE", "BID_VOL", "ASK_VOL", "TRADE_AMT", "SPREAD", "BID_EX_SPREAD",
    "ASK_EX_SPREAD", "PROFIT", "EXECUTED", "BINANCE_REMAIN", "KUCOIN_REMAIN", "ALL_PROFIT", "MAX_PROFIT", "BINANCE_ETH",
    "BINANCE_USDT", "KUCOIN_ETH", "KUCOIN_USDT")
# Logger for this module
log = logging.getLogger(__name__)
log_format = '%(asctime)s %(levelname)-5.5s [%(name)s-%(funcName)s:%(lineno)d][%(threadName)s] %(message)s'
log_filename = datetime.now().strftime('CryptoArb.%Y%m%d-%H%M%S.log')
logfile = logdir + log_filename
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', filename=logfile, level=logging.INFO)

loop = asyncio.get_event_loop()

binance = ccxt.binance()
binance.load_markets()
# hitbtc = ccxt.hitbtc()
kucoin = ccxt.kucoin()
kucoin.load_markets()
# poloniex = ccxt.poloniex()
# kraken = ccxt.kraken()
# okex = ccxt.okex()
# huobi = ccxt.huobi()

# hitbtc = ccxt.hitbtc({
#     'apiKey': '',
#     'secret': ''
# })

kucoin = ccxt.kucoin({
    'apiKey': '',
    'secret': ''
})

binance = ccxt.binance({
    'apiKey': '',
    'secret': ''
})


allexchanges = [binance, kucoin]
exchanges = allexchanges
balances = {}
mid_price = 0

symbol = 'ETH/USDT'
filename = savedir + datetime.now().strftime('CryptoArb.%Y%m%d-%H%M%S.csv')
isDatafileExist = bool(glob.glob(filename))
datafile = pd.DataFrame()

if isDatafileExist:
    datafile = pd.read_csv(filename, index_col=0)
else:
    datafile = pd.DataFrame([[0] * len(column_names)], columns=column_names,
                            index=pd.date_range(datetime.now().strftime("%d-%m-%Y %I:%M:%S"),
                                                periods=1, frequency='H'))
    datafile.to_csv(filename, index=True)


async def poll():
    while True:
        k = {}
        await asyncio.sleep(1.6)
        try:
            for exchange in exchanges[:]:
                k[exchange] = await exchange.fetch_order_book(symbol)
        except (errors.ExchangeNotAvailable, errors.ExchangeError):
            log.error(' Ooops! %s', exchange.name, ' is not available.')
            k = {}
            yield k
        except errors.RequestTimeout:
            log.error("Exchange %s Timeout: ", exchange.name)
            k = {}
            yield k
        yield k


async def execute_orders(exchange_from_buy, exchange_on_sell, amt_lot, buy_price, sell_price):
    status = "Pending"
    remaining = [amt_lot, amt_lot]
    global starttime
    global endtime
    kucoin_side = None
    binance_side = None
    binance_orders, kucoin_orders = [], []
    kucoin_orderid = None
    binance_orderid = None
    sendordertime = time.time()

    tasks = [exchange_on_sell.create_limit_sell_order(symbol, amt_lot, sell_price),
             exchange_from_buy.create_limit_buy_order(symbol, amt_lot, buy_price)]

    z = await asyncio.gather(*tasks, return_exceptions=True)
    status = "Sent"

    endtime = time.time()
    log.info("executing orders: %s %s %s %s %s", exchange_from_buy.name, exchange_on_sell.name, amt_lot, buy_price,
             sell_price)
    log.info("Time elapsed since polling and order: %s %s", endtime - starttime, endtime - sendordertime)
    log.info(z)

    if exchange_from_buy == kucoin:
        kucoin_orderid = z[1]['id']
        binance_orderid = z[0]['id']
        kucoin_side = 'BUY'
        binance_side = 'SELL'
    elif exchange_from_buy == binance:
        kucoin_side = 'SELL'
        binance_side = 'BUY'
        binance_orderid = z[1]['id']
        kucoin_orderid = z[0]['id']
    log.info("binance_orderid, kucoin_orderid: %s %s", binance_orderid, kucoin_orderid)
    # kucoin_order_status = kucoin.fetch_order(kucoin_orderid, symbol = symbol, params={'type' : kucoin_side})['status']
    # log.info("kucoin_order_status: %s ", kucoin_order_status)

    await asyncio.sleep(max(e.rateLimit for e in exchanges) / 1000)

    tasks = [binance.fetch_open_orders(symbol), kucoin.fetch_open_orders(symbol)]
    open_orders = await asyncio.gather(*tasks, return_exceptions=True)
    binance_orders = open_orders[0]
    kucoin_orders = open_orders[1]
    log.info("open orders: %s", open_orders)
    counter = 0
    if (binance_orders or kucoin_orders):
        status = "Pending"
    else:
        status = "Closed"
        remaining = [0, 0]

    while len(binance_orders) > 0 or len(kucoin_orders) > 0:
        await asyncio.sleep(max(e.rateLimit for e in exchanges) / 1000)
        counter += 1
        log.info("Waiting for fills until timeout : %s", counter)
        tasks = [binance.fetch_open_orders(symbol), kucoin.fetch_open_orders(symbol)]
        while True:
            fail_at_parsing = False
            try:
                open_orders = await asyncio.gather(*tasks, return_exceptions=True)
                binance_orders = open_orders[0]
                kucoin_orders = open_orders[1]
                try:
                    binance_remaining = binance_orders[0]['remaining']
                except IndexError:
                    log.info('IndexError when checking remaining orders on Binance means remain 0')
                    binance_remaining = 0
                except (errors.RequestTimeout, errors.ExchangeNotAvailable, errors.ExchangeError):
                    log.info('Some exchange error found when parsing the remaining orders on Binance')
                    fail_at_parsing = True
                try:
                    kucoin_remaining = kucoin_orders[0]['remaining']
                except IndexError:
                    log.info('IndexError when checking remaining orders on Kucoin means remain 0')
                    kucoin_remaining = 0
                except (errors.RequestTimeout, errors.ExchangeNotAvailable, errors.ExchangeError):
                    log.info('Some exchange error found when parsing the remaining orders on Kucoin')
                    fail_at_parsing = True
                if fail_at_parsing:
                    continue
                else:
                    break
            except (errors.RequestTimeout, errors.ExchangeNotAvailable, errors.ExchangeError):
                log.info("Waiting due to exchange error")
                await asyncio.sleep(max(e.rateLimit for e in exchanges) / 1000)
                continue

        # TODO check what's the next best available bid/ask offer on the pending exchange and if profitale, execute
        remaining = [binance_remaining, kucoin_remaining]

        if (binance_orders or kucoin_orders):
            status = "Pending"
            log.info("Binance, Kucoin remaining (pending): %s %s", binance_remaining, kucoin_remaining)
        else:
            status = "Closed"
            log.info("Binance, Kucoin remaining (closed): %s %s", binance_remaining, kucoin_remaining)

        if counter >= 8 and (binance_orders or kucoin_orders):
            log.info("Timeout reached: %s", counter)
            counter = 0
            await asyncio.sleep(max(e.rateLimit for e in exchanges) / 1000)
            tasks = [kucoin.cancel_orders(symbol), binance.cancel_order(binance_orderid, symbol)]
            cancels = await asyncio.gather(*tasks, return_exceptions=True)
            log.info("cancelled orders: %s", cancels)
            status = "Cancelled"
            log.info("Binance, Kucoin remaining (cancelled): %s %s", binance_remaining, kucoin_remaining)
            await asyncio.sleep(max(e.rateLimit for e in exchanges) / 1000)
            # fix holding assymetry by trading remaining on opposite exchange
            log.info("fixing any assymetric holding on binance: %s %s", kucoin_side, kucoin_remaining)
            if kucoin_remaining > 0:
                try:
                    if kucoin_side == 'BUY':
                        assymetry_fix = await binance.create_market_buy_order(symbol, binance.amount_to_lots(symbol,
                                                                                                             kucoin_remaining))
                    if kucoin_side == 'SELL':
                        assymetry_fix = await binance.create_market_sell_order(symbol, binance.amount_to_lots(symbol,
                                                                                                              kucoin_remaining))
                except errors.ExchangeError:
                    log.info("error with assymetry_fix %s", assymetry_fix)
                log.info("fixing: %s", assymetry_fix)
                status = "Asym_Fixed"
                await asyncio.sleep(max(e.rateLimit for e in exchanges) / 1000)
                boo = await binance.fetch_open_orders(symbol)
                await asyncio.sleep(max(e.rateLimit for e in exchanges) / 1000)
                if boo:
                    assymetric_fix_cancel = await binance.cancel_order(boo[0]['id'], symbol)
                    log.info("assym fixing cancel: %s", assymetric_fix_cancel)
                    await asyncio.sleep(max(e.rateLimit for e in exchanges) / 1000)
            if binance_remaining > 0:
                try:
                    if binance_side == 'BUY':
                        assymetry_fix = await binance.create_market_buy_order(symbol, binance.amount_to_lots(symbol,
                                                                                                             binance_remaining))
                    if binance_side == 'SELL':
                        assymetry_fix = await binance.create_market_sell_order(symbol, binance.amount_to_lots(symbol,
                                                                                                              binance_remaining))
                except errors.ExchangeError:
                    log.info("error with assymetry_fix %s", assymetry_fix)
                log.info("fixing: %s", assymetry_fix)
                status = "Asym_Fixed"
                await asyncio.sleep(max(e.rateLimit for e in exchanges) / 1000)
                boo = await binance.fetch_open_orders(symbol)
                await asyncio.sleep(max(e.rateLimit for e in exchanges) / 1000)
                if boo:
                    assymetric_fix_cancel = await binance.cancel_order(boo[0]['id'], symbol)
                    log.info("assym fixing cancel: %s", assymetric_fix_cancel)
                    await asyncio.sleep(max(e.rateLimit for e in exchanges) / 1000)

            tasks = [binance.fetch_open_orders(symbol), kucoin.fetch_open_orders(symbol)]
            open_orders = await asyncio.gather(*tasks, return_exceptions=True)

            binance_orders = open_orders[0]
            kucoin_orders = open_orders[1]

            await asyncio.sleep(max(e.rateLimit for e in exchanges) / 1000)

    return status, remaining


async def fetch_balances():
    global balances
    balances = {}
    log.info("fetching_balances...")
    base = symbol.split("/")[0]
    quote = symbol.split("/")[1]
    try:
        for exchange in allexchanges[:]:
            result = await exchange.fetch_balance()
            balances[exchange] = [result[base]['free'], result[quote]['free']]
        return balances
    except (errors.RequestTimeout, errors.ExchangeNotAvailable, errors.ExchangeError):
        log.error('Exchange  is not available during balance fetch.')
        await asyncio.sleep(max(e.rateLimit for e in exchanges) / 1000)

        return balances


def calculate_best_bid_and_ask(orderbooks):
    bids = {}
    asks = {}
    for key in orderbooks:
        bids[key] = orderbooks[key]['bids'][:5]
        asks[key] = orderbooks[key]['asks'][:5]
    best_bid_exchange = [key for key, val in bids.items() if val == max(bids.values())][0]
    best_ask_exchange = [key for key, val in asks.items() if val == min(asks.values())][0]
    try:
        best_bid = bids[best_bid_exchange][0]
        best_ask = asks[best_ask_exchange][0]
        bid_exchange_spread = asks[best_bid_exchange][0][0] - bids[best_bid_exchange][0][0]
        ask_exchange_spread = asks[best_ask_exchange][0][0] - bids[best_ask_exchange][0][0]
    except IndexError:
        log.error("An IndexError occured, asks and bids: %s %s ", asks, bids)
        return None

    ba = asks[best_bid_exchange][0][0]
    bb = bids[best_bid_exchange][0][0]
    aa = asks[best_ask_exchange][0][0]
    ab = bids[best_ask_exchange][0][0]
    return best_bid_exchange, best_bid, best_ask_exchange, best_ask, bid_exchange_spread, ask_exchange_spread, ba, bb, aa, ab


def send_email(text):
    smtp = smtplib.SMTP()
    smtp.connect('localhost')

    msgRoot = MIMEMultipart("alternative")
    msgRoot['Subject'] = Header("CryptoArb Report", "utf-8")
    msgRoot['From'] = "catacomb"
    msgRoot['To'] = "blockliners@gmail.com"
    text = MIMEText(text)
    msgRoot.attach(text)
    smtp.sendmail("blockliners@gmail.com", "blockliners@gmail.com", msgRoot.as_string())


async def main():
    global datafile
    global balances
    global starttime
    global endtime
    global mid_price
    all_profit = 0

    while len(balances) < len(allexchanges):
        print('starting fetching balances...')
        await fetch_balances()
        for key, value in balances.items():
            log.info("Balances %s %s", key.name, value)

    async for orderbook in poll():
        status = ("No", [0, 0])
        if len(orderbook.keys()) == len(exchanges):
            result = (calculate_best_bid_and_ask(orderbook))
            if not result:
                continue
            starttime = time.time()
            lessvolume = min(result[1][1], result[3][1])
            best_bid_exchange = result[0]
            best_ask_exchange = result[2]
            best_bid_price = result[1][0]
            best_ask_price = result[3][0]
            best_bid_volume = result[1][1]
            best_ask_volume = result[3][1]
            bid_exchange_spread = result[4]
            ask_exchange_spread = result[5]
            mid_price = (best_bid_price + best_ask_price) / 2
            ba, bb, aa, ab = result[6], result[7], result[8], result[9]

            estim_buy_fee = \
            best_ask_exchange.calculate_fee(symbol, 'limit', 'buy', balances[best_ask_exchange][1] / best_ask_price,
                                            best_ask_price, 'taker')[
                'cost']
            estim_sell_fee = \
            best_bid_exchange.calculate_fee(symbol, 'limit', 'sell', balances[best_bid_exchange][0], best_bid_price,
                                            'taker')[
                'cost']

            estim_buy_fee += 5
            estim_sell_fee += 5

            amt_can_buy = (balances[best_ask_exchange][1] - estim_buy_fee) / best_ask_price
            amt_can_sell = balances[best_bid_exchange][0] - (estim_sell_fee / best_bid_price)
            amt_can_trade = min(amt_can_buy, amt_can_sell, lessvolume)

            buy_fee = best_ask_exchange.calculate_fee(symbol, 'limit', 'buy', amt_can_trade, best_ask_price, 'taker')[
                'cost']
            sell_fee = best_bid_exchange.calculate_fee(symbol, 'limit', 'sell', amt_can_trade, best_bid_price, 'taker')[
                'cost']

            max_buy_fee = best_ask_exchange.calculate_fee(symbol, 'limit', 'buy', lessvolume, best_ask_price, 'taker')[
                'cost']
            max_sell_fee = \
            best_bid_exchange.calculate_fee(symbol, 'limit', 'sell', lessvolume, best_bid_price, 'taker')[
                'cost']

            spread = best_bid_price - best_ask_price
            profit = (spread * amt_can_trade) - (buy_fee + sell_fee)
            max_profit = (spread * lessvolume) - (max_buy_fee + max_sell_fee)

            if (profit >= 0.9):
                # or (amt_can_trade < 0.35 and profit > 0.3):
                # check if we have enough usdt to pay the fee:
                if (balances[best_ask_exchange][1] >= (best_ask_price * amt_can_trade) + buy_fee) and (
                        balances[best_bid_exchange][1] >= sell_fee):
                    amt_lot = min(best_ask_exchange.amount_to_lots(symbol, amt_can_trade),
                                  best_bid_exchange.amount_to_lots(symbol, amt_can_trade))
                    status = await execute_orders(best_ask_exchange, best_bid_exchange, amt_lot, best_ask_price,
                                                  best_bid_price)

                    log.info("calling executions: %s %s %s %s %s", best_ask_exchange, best_bid_exchange, amt_lot,
                             best_ask_price, best_bid_price)
                    log.info("Status is: %s", status[0])
                    await asyncio.sleep(max(e.rateLimit for e in exchanges) / 1000)
                    await fetch_balances()
                    for key, value in balances.items():
                        log.info("Balances %s %s %s", key.name, value, best_ask_price)

                    if sum(status[1]) == 0:
                        if status[0] == 'Closed':
                            all_profit += profit

                        text = "Accum profit: " + str(all_profit) + " \n"
                        bnb_profit = await binance.fetch_balance()
                        bnb_profit = bnb_profit['BNB']['free']
                        for key, value in balances.items():
                            text = text + str(key.name) + " \n " + str(value) + " \n "

                        text = text + "  " + str(status[0]) + " \n "
                        text = text + "best_ask_price: " + str(best_ask_price) + " \n "
                        text = text + "BNB: " + str(bnb_profit) + " \n "

                        send_email(text)
                    # TODO: add rebalancing:
                    # rebalance_cost = await rebalance(all_profit)
                    # all_profit -= rebalance_cost


                else:
                    log.info("USDT balance is not enough to pay fee: %s %s ", balances[best_bid_exchange][0],
                             balances[best_ask_exchange][1])
                    status = ("Fee_High", [None, None])

            if spread > 1:
                log.info("spreads: %s %s %s %s %s %s %s %s %s", best_bid_exchange.name, best_ask_exchange.name,
                         best_bid_price, best_bid_volume, best_ask_price, best_ask_volume, format(spread, '.5f'),
                         amt_can_trade, format(profit, '.5f'))
            # TODO change logging date / time to 24h
            current_index = pd.date_range(datetime.now().strftime("%D %H:%M:%S.%f")[:-3], periods=1)
            newLine = pd.DataFrame([[best_bid_exchange.name, best_ask_exchange.name, best_bid_price, best_ask_price,
                                     best_bid_volume, best_ask_volume, format(amt_can_trade, '.6f'),
                                     format(spread, '.5f'),
                                     format(bid_exchange_spread, '.5f'),
                                     format(ask_exchange_spread, '.5f'),
                                     format(profit, '.5f'), status[0], status[1][0], status[1][1],
                                     format(all_profit, '.6f'), format(max_profit, '.6f'), balances[binance][0],
                                     balances[binance][1],
                                     balances[kucoin][0], balances[kucoin][1]]], columns=column_names,
                                   index=current_index)
            newLine.to_csv(filename, mode='a', index=True, header=False)

        else:
            log.error('sleeping because data from exchnage(s) is not available.')
            await asyncio.sleep(max(e.rateLimit for e in exchanges) / 1000)


loop.run_until_complete(main())