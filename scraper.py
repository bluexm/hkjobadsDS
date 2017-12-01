# coding=utf-8
import os 
import ccxt
import pandas as pd 
import time, datetime

import sqlite3
from sqlite3 import Error
DB_FILE = '.\\orderbooks.db'

""" create a database connection to a SQLite database """
try:
	conn = sqlite3.connect(DB_FILE)
	print(sqlite3.version)
except Error as e:
	print(e)

#bitmex = ccxt.bitmex()

## ANX PRO SETTINGS 
anx = ccxt.anxpro()
##Private key 
api_key = os.environ.get('ANX_API_KEY') 
api_secret = os.environ.get('ANX_SECRET')
exmo = ccxt.exmo({
    'apiKey': api_key,
    'secret': api_secret
})

##Poloniex settings
polo = ccxt.poloniex()
##Private key 
api_key = os.environ.get('POLONIEX_API_KEY') 
api_secret = os.environ.get('POLONIEX_SECRET') 
exmo_polo   = ccxt.exmo({
    'apiKey': api_key,
    'secret': api_secret
})

##BitMex settings
bmex = ccxt.bitmex()
##Private key 
api_key = os.environ.get('BITMEX_API_KEY') 
api_secret = os.environ.get('BITMEX_SECRET') 
exmo_bmex   = ccxt.exmo({
    'apiKey': api_key,
    'secret': api_secret
})

##Bitfinex  settings
bfix = ccxt.bitfinex()
##Private key 
api_key = os.environ.get('BITFINEX_API_KEY') 
api_secret = os.environ.get('BITFINEX_SECRET') 
exmo_bfix   = ccxt.exmo({
    'apiKey': api_key,
    'secret': api_secret
})

##Gatecoin  settings
bgtcn = ccxt.gatecoin()
##Private key 
exmo_bgtcn   = ccxt.exmo({
    'apiKey': '',
    'secret': ''
})



def get_orderbook(ccxtobj,symbol,sourcename,connexion):
	dictbook = ccxtobj.fetch_order_book(symbol)
	print(dictbook['asks'][0])
	a = pd.DataFrame(dictbook['asks'],columns=['askprice','askqty'])
	a['rank']=a.index.values
	b = pd.DataFrame(dictbook['bids'],columns=['bidprice','bidqty'])
	b['rank']=b.index.values
	df = pd.merge(a,b,on='rank')
	df['source']=sourcename
	df.index = [str(dictbook['timestamp']) for  r in df['rank']] #+"_"+str(r)
	# record in DB
	df.to_sql('orderbooks',connexion,if_exists='append')

i=0
while (i<5):
	get_orderbook(bgtcn, "BTC/USD", 'gatecoin', conn)
	#get_orderbook(anx, anx.symbols[0], 'anx', conn)
	#get_orderbook(polo, "BTC/USDT", 'polo', conn)
	#get_orderbook(bmex, "BTC/USD", 'bitmex', conn)
	#get_orderbook(bfix, "BTC/USD", 'bitfinex', conn)
	
	time.sleep(2)
	i+=1
	
	"""
	## ANX
	#anx_markets = anx.load_markets()
	#print(anx.id, anx_markets)
	#print(anx.fetch_order_book(anx.symbols[0]))	
	
	dictbook = anx.fetch_order_book(anx.symbols[0])
	pd.DataFrame(dictbook['bids'],columns=['price','qty'])
	a = pd.DataFrame(dictbook['asks'],columns=['askprice','askqty'])
	a['rank']=a.index.values
	b = pd.DataFrame(dictbook['bids'],columns=['bidprice','bidqty'])
	b['rank']=b.index.values
	df_anx = pd.merge(a,b,on='rank')
	df_anx['source']='anx'
	df_anx.index = [str(dictbook['timestamp']) for  r in df_anx['rank']] #+"_"+str(r)
	# record in DB
	df_anx.to_sql('orderbooks',conn,if_exists='append')

	
	##POLONIEX 
	#polo_markets = polo.load_markets()
	#print(polo.id, polo_markets)
	#print(polo.fetch_order_book("BTC/USDT"))
	dictbook = polo.fetch_order_book("BTC/USDT")
	a = pd.DataFrame(dictbook['asks'],columns=['askprice','askqty'])
	a['rank']=a.index.values
	b = pd.DataFrame(dictbook['bids'],columns=['bidprice','bidqty'])
	b['rank']=b.index.values
	df_polo = pd.merge(a,b,on='rank')
	df_polo['source']='poloniex'
	df_polo.index = [str(dictbook['timestamp']) for  r in df_polo['rank']] #+"_"+str(r)
	# record in DB
	df_polo.to_sql('orderbooks',conn,if_exists='append')
	
	##BitMex
	#bmex_markets = bmex.load_markets()
	#print(bmex.id, bmex_markets)
	#print(bmex.fetch_order_book("BTC/USD"))
	dictbook = bmex.fetch_order_book("BTC/USD")
	a = pd.DataFrame(dictbook['asks'],columns=['askprice','askqty'])
	a['rank']=a.index.values
	b = pd.DataFrame(dictbook['bids'],columns=['bidprice','bidqty'])
	b['rank']=b.index.values
	df_bmex = pd.merge(a,b,on='rank')
	df_bmex['source']='bitmex'
	df_bmex.index = [str(dictbook['timestamp']) for  r in df_bmex['rank']] #+"_"+str(r)
	# record in DB
	df_bmex.to_sql('orderbooks',conn,if_exists='append')
	
	##BitFinex
	#bfix_markets = bfix.load_markets()
	#print(bfix.id, bfix_markets)
	#print(bfix.fetch_order_book("BTC/USD"))
	dictbook = bfix.fetch_order_book("BTC/USD")
	a = pd.DataFrame(dictbook['asks'],columns=['askprice','askqty'])
	a['rank']=a.index.values
	b = pd.DataFrame(dictbook['bids'],columns=['bidprice','bidqty'])
	b['rank']=b.index.values
	df_bfix = pd.merge(a,b,on='rank')
	df_bfix['source']='bitfinex'
	df_bfix.index = [str(dictbook['timestamp']) for  r in df_bfix['rank']] #+"_"+str(r)
	# record in DB
	df_bfix.to_sql('orderbooks',conn,if_exists='append')
	
	"""
	
conn.close()

'''
#print(polo.fetch_ticker('BTC/USD'))
#dfticker = pd.read_json(polo.fetch_ticker('BTC/USD'))

#print(polo.fetch_trades('BTC/USD'))
#print(exmo.fetch_balance())

# sell one ฿ for market price and receive $ right now
print(exmo.id, exmo.create_market_sell_order('BTC/USD', 1))

# limit buy BTC/EUR, you pay €2500 and receive ฿1  when the order is closed
print(exmo.id, exmo.create_limit_buy_order('BTC/EUR', 1, 2500.00))

# pass/redefine custom exchange-specific order params: type, amount, price, flags, etc...
kraken.create_market_buy_order('BTC/USD', 1, {'trading_agreement': 'agree'})

'''
