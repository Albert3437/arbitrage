import asyncio
from concurrent.futures import ProcessPoolExecutor
from loguru import logger
import ccxt.async_support as async_ccxt

logger.add('debug.log')

async def fetch_order_book(exchange, token:str):
    """Запрос ордербуков из биржи, принимает объект биржи и токен в 
    формате 'BTC/USDT', возвращает словарь:
    {'asks': data, 'bids': data, 'exchange': data, 'token': data} или None"""
    for _ in range(3):
        try:
            res = await exchange.fetch_order_book(symbol=token)
            filtered_res = {'asks': res['asks'], 'bids': res['bids'], 'exchange': str(exchange).lower(), 'token': token}
            return filtered_res
        except Exception as e:
            logger.warning((e, token))
            await asyncio.sleep(1)
    logger.warning((exchange, token))
    return

async def init_exchange(ex_name:str, tokens:list):
    """Асинхронная функция которая принимает имя биржи и список токенов: 
    ['BTC/USDT', ...] и запускает получение ордербуков асинхронно возвращая 
    список со словарями:
    [{'asks': data, 'bids': data, 'exchange': data, 'token': data}, ...]"""
    if ex_name == 'gate.io':
        ex_name = 'gateio'
    if ex_name == 'mexc global':
        ex_name = 'mexc'
    exchange = getattr(async_ccxt, ex_name)()
    tasks = [fetch_order_book(exchange, token) for token in tokens]
    results = await asyncio.gather(*tasks)
    await exchange.close()
    return results

def sync_init_exchange(ex_name, tokens):
    """Обертка для асинхронного запуска получения ордербуков
    принимает имя биржи и список токенов: ['BTC/USDT', ...]
    возвращая список со словарями:
    [{'asks': data, 'bids': data, 'exchange': data, 'token': data}, ...]"""
    result = asyncio.run(init_exchange(ex_name, tokens))
    return result

def get_orderbooks(data):
    """Функция входа, запрашивает список токенов, многопоточно запускает 
    получение ордербуков и преобразует полученые ордербуки словарь по биржам
    со словарем по токенам, возвращает: {Биржа:{Токен:{Тип:Данные}}}"""
    results = []

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(sync_init_exchange, ex, tokens) for ex, tokens in data.items()]
        for future in futures:
            results.append(future.result())

    ready_data = {}

    for result in results:
        for item in result:
            if item != None and item['asks'] and item['bids']:
                exchange = item['exchange']
                token = item['token']
                
                if exchange not in ready_data:
                    ready_data[exchange] = {}
                
                ready_data[exchange][token] = item

    return ready_data

if __name__ == '__main__':
    get_orderbooks()
