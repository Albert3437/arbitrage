import ccxt
import queue
import threading
import configparser

from loguru import logger

logger.add('debug.log')

class GeneralData:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')

        # Инициализация подключений к различным биржам с использованием API ключей
        self.exchanges = [
            ccxt.binance({'apiKey': config['Binance']['apikey'], 'secret': config['Binance']['secretkey']}),
            ccxt.bitfinex2({'apiKey': config['Bitfinex2']['apikey'], 'secret': config['Bitfinex2']['secretkey']}),
            ccxt.htx({'apiKey': config['HTX']['apikey'], 'secret': config['HTX']['secretkey']}),
            ccxt.kucoin({'apiKey': config['KuCoin']['apikey'], 'secret': config['KuCoin']['secretkey'], 'password': config['KuCoin']['password']}),
            ccxt.okx({'apiKey': config['OKX']['apikey'], 'secret': config['OKX']['secretkey'], 'password': config['OKX']['password']}),
            ccxt.bitget({'apiKey': config['Bitget']['apikey'], 'secret': config['Bitget']['secretkey'], 'password': config['Bitget']['password']}),
            ccxt.bybit({'apiKey': config['Bybit']['apikey'], 'secret': config['Bybit']['secretkey']}),
            ccxt.gate({'apiKey': config['Gate']['apikey'], 'secret': config['Gate']['secretkey']}),
            #ccxt.mexc({'apiKey': config['MEXC']['apikey'], 'secret': config['MEXC']['secretkey']}),
            ccxt.bitmex({'apiKey': config['BitMEX']['apikey'], 'secret': config['BitMEX']['secretkey']}),
            ccxt.poloniex({'apiKey': config['Poloniex']['apikey'], 'secret': config['Poloniex']['secretkey']}),
            ccxt.digifinex({'apiKey': config['Digifinex']['apikey'], 'secret': config['Digifinex']['secretkey']}),
            ccxt.hitbtc({'apiKey': config['HitBTC']['apikey'], 'secret': config['HitBTC']['secretkey']}),
            ccxt.coinex({'apiKey': config['Coinex']['apikey'], 'secret': config['Coinex']['secretkey']})
        ]


    @logger.catch
    def get_markets(self):
        # Получение и фильтрация рынков (торговых пар) с разных бирж, исключая фиатные пары.
        # Возвращает словарь, где ключи - это объекты бирж, а значения - списки торговых пар.
        def put_to_query(query, exchange):
            for token, data in exchange.load_markets().items():
                if data['active'] in [True, None]: # Проверка на то активен ли токен, на определленных биржах
                    if data['type'] == 'spot' and data['quote'] not in ['USD', 'RON', 'ARS', 'ZAR', 'EUR', 'GBP', 'TRY', 'PLN', 'UAH', 'JPY', 'BRL', 'RUB', 'NGN', 'AUD']:
                        query.put((exchange, token))

        query = queue.Queue()
        markets = {}
        threads = []

        # Создание и запуск потоков для асинхронной загрузки рынков с каждой биржи
        for exchange in self.exchanges:
            thread = threading.Thread(target=put_to_query, args=[query, exchange])
            thread.start()
            threads.append(thread)
        
        # Ожидание завершения всех потоков
        for thread in threads:
            thread.join()

        # Сбор данных из очереди в словарь markets
        while not query.empty():
            data = query.get()
            exchange = str(data[0]).lower()
            if exchange not in list(markets.keys()):
                markets[exchange] = []
            markets[exchange].append(data[1])
        
        return markets
        
    @logger.catch
    def convert_tokens(self, markets = None):
        """Преобразование словаря рынков из формата {биржа: [токены]} в {токен: [биржи]}.
        Если markets не указан, используется filter_networks для получения актуальных рынков.
        Возвращает словарь токенов с указанием бирж, на которых они торгуются."""
        if markets == None:
            markets = self.filter_networks()
        
        tokens = {}

        for exchange, markets in markets.items():
            for token in markets:
                if token not in list(tokens.keys()):
                    tokens[token] = []
                tokens[token].append(str(exchange))
        
        return tokens

    @logger.catch
    def _filter_tokens(self, tokens = None):
        """Фильтрация токенов, оставляя только те, которые торгуются минимум на двух биржах.
        Если tokens не указан, используется get_tokens для их получения.
        Возвращает словарь отфильтрованных токенов."""
        if tokens == None:
            tokens = self.get_tokens()

        tokens_to_remove = []

        for token, exchanges in tokens.items():
            if len(exchanges) <= 1:
                tokens_to_remove.append(token)

        for token in tokens_to_remove:
            del tokens[token]
        
        return tokens

    @logger.catch
    def get_tokens(self, tokens = None):
        """Удаление токенов, которые доступны только на одной бирже.
        Если tokens не переданы, используется get_markets для их получения."""
        if tokens == None:
            tokens = self.get_markets()
#----------------------------------------------------------------------------------------------------------
# Добавить функционал black list токенов в эту функцию и вывести его в конфиг
#----------------------------------------------------------------------------------------------------------
        for exchange, _tokens in tokens.items():
            tokens_to_remove = []
            for token in _tokens:
                i = 0
                for tokens_copy in tokens.values():
                    if token in tokens_copy:
                        i+=1
                if i<=1:
                    tokens_to_remove.append(token)
            for token in tokens_to_remove:
                tokens[exchange].remove(token)

        # Возвращает словарь, где ключи - биржи, а значения - токены, оставшиеся после фильтрации.
        return tokens

    class GetNetworks:
        @logger.catch
        def binance(token, currencies):
            # Получение сетей для токена на Binance.
            # Возвращает список сетей и информацию о возможности депозита/вывода и комиссиях.
            networks = []
            try:
                for currencie in currencies[token]['info']['networkList']:
                    networks.append({'network':currencie['network'],
                                    'deposit':currencie['depositEnable'] or False,
                                    'withdraw':currencie['withdrawEnable'] or False,
                                    'fee':currencie['withdrawFee'] or 0})
            except:
                pass
            return networks
        
        @logger.catch
        def okx(token, currencies):
            networks = []
            try:
                for currencie in currencies[token]['networks'].values():
                    networks.append({'network':currencie['network'],
                                    'deposit':currencie['deposit'] or False,
                                    'withdraw':currencie['withdraw'] or False,
                                    'fee':currencie['fee'] or 0})
            except:
                pass
            return networks

        @logger.catch
        def bitget(token, currencies):
            networks = []
            try:
                for currencie in currencies[token]['info']['chains']:
                    networks.append({'network':currencie['chain'],
                                    'deposit':bool(currencie['depositConfirm'] or False),
                                    'withdraw':bool(currencie['withdrawConfirm'] or False),
                                    'fee':currencie['withdrawFee'] or 0})
            except:
                pass
            return networks

        @logger.catch
        def coinex(token, currencies):
            networks = []
            try:
                for currencie in currencies[token]['info']:
                    networks.append({'network':currencie['chain'],
                                    'deposit':currencie['can_deposit'] or False,
                                    'withdraw':currencie['can_withdraw'] or False,
                                    'fee':currencie['withdraw_tx_fee'] or 0})
            except:
                pass
            return networks

        @logger.catch
        def htx(token, currencies):
            networks = []
            try:
                for currencie in currencies[token]['info']['chains']:
                    status_dict = {'allowed':True,'prohibited':False}
                    networks.append({'network':currencie['displayName'],
                                    'deposit':status_dict[currencie['depositStatus']] or False,
                                    'withdraw':status_dict[currencie['withdrawStatus']] or False,
                                    'fee':currencie['transactFeeWithdraw'] or 0})
            except:
                pass
            return networks
        
        @logger.catch
        def bitfinex(token, currencies):
            networks = []
            try:
                for currencie in currencies[token]['networks'].values():
                    networks.append({'network':currencie['network'],
                                    'deposit':currencie['deposit'] or False,
                                    'withdraw':currencie['withdraw'] or False,
                                    'fee':currencie['fee'] or 0})
            except:
                pass
            return networks
        
        @logger.catch
        def kucoin(token, currencies):
            networks = []
            try:
                for currencie in currencies[token]['networks'].values():
                    networks.append({'network':currencie['info']['chainName'],
                                    'deposit':currencie['info']['isDepositEnabled'] or False,
                                    'withdraw':currencie['info']['isWithdrawEnabled'] or False,
                                    'fee':currencie['fee'] or 0})
            except:
                pass
            return networks
        
        @logger.catch
        def bybit(token, currencies):
            networks = []
            try:
                for currencie in currencies[token]['networks'].values():
                    networks.append({'network':currencie['network'],
                                    'deposit':currencie['deposit'] or False,
                                    'withdraw':currencie['withdraw'] or False,
                                    'fee':currencie['fee'] or 0})
            except:
                pass
            return networks
        
        @logger.catch
        def gate(token, currencies):
            networks = []
            try:
                for currencie in currencies[token]['networks'].values():
                    networks.append({'network':currencie['network'],
                                    'deposit':currencie['deposit'] or False,
                                    'withdraw':currencie['withdraw'] or False,
                                    'fee':currencie['fee'] or 0})
            except:
                pass
            return networks
        
        @logger.catch
        def mexc(token, currencies):
            networks = []
            try:
                for currencie in currencies[token]['networks'].values():
                    networks.append({'network':currencie['network'],
                                    'deposit':currencie['deposit'] or False,
                                    'withdraw':currencie['withdraw'] or False,
                                    'fee':currencie['fee'] or 0})
            except:
                pass
            return networks
        
        @logger.catch
        def bitmex(token, currencies):
            networks = []
            try:
                for currencie in currencies[token]['networks'].values():
                    networks.append({'network':currencie['network'],
                                    'deposit':currencie['deposit'] or False,
                                    'withdraw':currencie['withdraw'] or False,
                                    'fee':currencie['fee'] or 0})
            except:
                pass
            return networks
        
        @logger.catch
        def whitebit(token, currencies):
            networks = []
            try:
                for currencie in currencies[token]['networks'].values():
                    networks.append({'network':currencie['network'],
                                    'deposit':currencie['deposit'] or False,
                                    'withdraw':currencie['withdraw'] or False,
                                    'fee':currencie['fee'] or 0})
            except:
                pass
            return networks
        
        @logger.catch
        def poloniex(token, currencies):
            networks = []
            try:
                for currencie in currencies[token]['networks'].values():
                    networks.append({'network':currencie['network'],
                                    'deposit':currencie['deposit'] or False,
                                    'withdraw':currencie['withdraw'] or False,
                                    'fee':currencie['fee'] or 0})
            except:
                pass
            return networks
        
        @logger.catch
        def digifinex(token, currencies):
            networks = []
            try:
                for currencie in currencies[token]['networks'].values():
                    networks.append({'network':currencie['network'],
                                    'deposit':currencie['deposit'] or False,
                                    'withdraw':currencie['withdraw'] or False,
                                    'fee':currencie['fee'] or 0})
            except:
                pass
            return networks
        
        @logger.catch
        def hitbtc(token, currencies):
            networks = []
            try:
                for currencie in currencies[token]['networks'].values():
                    networks.append({'network':currencie['network'],
                                    'deposit':currencie['deposit'] or False,
                                    'withdraw':currencie['withdraw'] or False,
                                    'fee':currencie['fee'] or 0})
            except:
                pass
            return networks

    
    @logger.catch
    def get_exchanges_networks(self):
        """Сбор информации о сетях для каждого токена на всех биржах.
        Возвращает словарь, где ключи - биржи, а значения - словари токенов с информацией о сетях."""
        exchanges_networks = {}
        networks_dict = {} 
        for name, method in self.GetNetworks.__dict__.items():
            if callable(method) and not name.startswith("__"):
                if name == 'gate':
                    name = 'gate.io'
                if name == 'mexc':
                    name = 'mexc global'
                networks_dict[name] = method
        for exchange in self.exchanges:
            exchange_name = str(exchange).lower()
            exchanges_networks[exchange_name] = {}
            currencies = exchange.fetch_currencies()
            for token in self.get_tokens()[exchange_name]:
                token = token.split('/')[0] # Получение имени токена без указания пары
                exchanges_networks[exchange_name][token] = networks_dict[exchange_name](token, currencies)

        return exchanges_networks

    @logger.catch
    def filter_networks(self, tokens=None, networks=None):
        """Принимает токены и сети и проверяет их на доступность ввода и вывода"""
        if tokens == None:
        # Если tokens не переданы, получаем их используя get_tokens().
            tokens = self.get_tokens()
        if networks == None:
        # Если networks не переданы, получаем их используя get_exchanges_networks().
            networks = self.get_exchanges_networks()
        tokens_to_remove = []
        # Перебор всех бирж и токенов, доступных на этих биржах.
        for _exchange, _tokens in tokens.items():
            for token in _tokens:
                bool_list = []
                for network in networks[str(_exchange)][token.split('/')[0]]:
                    bool_list.append(network['withdraw'])
                    bool_list.append(network['deposit'])
                if not all(bool_list):
                    tokens_to_remove.append({'exchange':_exchange, 'token':token})
        for token in tokens_to_remove:
            tokens[token['exchange']].remove(token['token'])
        # Возвращает отфильтрованный список токенов, для которых доступны депозит и вывод на всех рассматриваемых биржах.
        return self.get_tokens(tokens)
    
    def main(self):
        networks = self.get_exchanges_networks()
        tokens = self.filter_networks(networks=networks)
        return networks, tokens

if __name__ == '__main__':
    data = GeneralData()
    data = data.main()
    print(data)
