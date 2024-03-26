from loguru import logger
from data import GeneralData
from orderbooks import get_orderbooks

logger.add('debug.log')

#exchanges, tokens, networks, orderbooks

class Analize:
    def __init__(self, networks):
        self.networks = networks

    @logger.catch
    def _convert_token(self, token:str):
        """Функция принимает токен в формате BTCUSDT а возвращает только символ в формате BTC"""
        for _tokens in self.tokens.values():
            for _token in _tokens:
                if _token.replace('/', '') == token:
                    return _token.split('/')[0]

    @logger.catch
    def allocation(self, orderbooks:dict):
        """Функция принимает книгу ордеров в формате {Биржа:{Токен:{Тип:Данные}}}, 
        добавляет к ним данные о сетях и возвращает в формате {Токен:{Биржа:{Тип:Данные}}}"""
        full_data = {}
        for exchange, orderbook in orderbooks.items():
            for token, order in orderbook.items():
                order['networks'] = self.networks[exchange][token.split('/')[0]]
                if token not in list(full_data.keys()) or not isinstance(full_data[token], dict):
                    full_data[token] = {}
                full_data[token][exchange] = order
        return full_data
    
    @logger.catch
    def convert_orderbooks(self, orderbooks:dict):
        """Функция принимает данные в формате {Токен:{Биржа:{Тип:Данные}}}, а именно некоторые 
        из данных представляют собой {'asks' or 'bids':[[price, count], [price, count], ...]}
        и эти данные функция преобразовывает в  
        {'asks' or 'bids':{min:[price, count], max:[price, count], average:[price, count]}}
        и возвращает данные в аналогичном принятому формате"""
        for _orderbooks in orderbooks.values():
            for _orderbook in _orderbooks.values():
                for _type in ['asks','bids']:
                    orders = _orderbook[_type]
                    total_amount = sum([float(order[1]) for order in orders])
                    for ask in orders:
                        for i,_ in enumerate(ask): ask[i] = float(ask[i])
                        ask.append(ask[0]*ask[1])
                        ask.pop(1)
                    average_price=sum([float(order[1]) for order in orders])/total_amount
                    average = [average_price, total_amount*average_price]
                    minn = min(orders, key=lambda x:x[0])
                    maxx = max(orders, key=lambda x:x[0])

                    _orderbook[_type] = {'min':minn, 'max':maxx, 'average':average}
        return orderbooks

    @logger.catch
    def smart_sort(self, orderbooks):
        """Принимает данные в формате {Токен:{Биржа:{Тип:Данные}}}, вычисляет самые прибыльные 
        комбинации путем сортировок, проверяет на доступность ввода и вывода, и возвращает
        [{'token':token, 'buy':{Тип:Данные}, 'sell':{Тип:Данные}, 'profit':%}, ...]"""
        strats = []
        for token, exchanges in orderbooks.items():
            strat = {'token':token, 'buy':None, 'sell':None, 'profit':None}
            asks_sort = sorted(list(exchanges.values()), key=lambda x:x['asks']['average'][0])
            bids_sort = sorted(list(exchanges.values()), key=lambda x:x['bids']['average'][0], reverse=True)
            test_li = [(a,b) for a in range(len(asks_sort)) for b in range(len(bids_sort))]
            test_li.sort(key=lambda x: x[0] or x[1])
            for a, b in test_li:
                if strat['buy'] != None and strat['sell'] != None:
                    break
                asks = asks_sort[a]
                bids = bids_sort[b]
                asks_nets = {value['network'] for value in asks['networks']}
                bids_nets = {value['network'] for value in bids['networks']}
                common_values = asks_nets.intersection(bids_nets)
                asks['networks'] = [network for network in asks['networks'] if network['network'] in common_values]
                bids['networks'] = [network for network in bids['networks'] if network['network'] in common_values]
                for i,_ in enumerate(common_values):
                    if asks['networks'][i]['withdraw'] and bids['networks'][i]['deposit']:
                        strat['buy'] = asks
                        strat['sell'] = bids
                        break
            if all(item is not None for item in list(strat.values())[:2]):
                try:
                    strat['profit'] = (100/strat['buy']['asks']['average'][0])*strat['sell']['bids']['average'][0] - 100
                except ZeroDivisionError:
                    strat['profit'] = 0
                if strat['profit'] > 0:
                    strats.append(strat)
        return strats
    
    @logger.catch
    def networks_sorter(self, strats):
        """Функция принимает данные в формате 
        [{'token':token, 'buy':{Тип:Данные}, 'sell':{Тип:Данные}, 'profit':%}, ...]
        фильтрует и сортирует сети и возвращает данные в том же фоормате"""
        for strat in strats:
            networks = strat['buy']['networks']
            networks.sort(key=lambda x:float(x['fee']))
            network_names = [name['network'] for name in networks]
            for name in network_names:
                buy = next((data for data in strat['buy']['networks'] if name in data.values()), None)
                sell = next((data for data in strat['sell']['networks'] if name in data.values()), None)
                if buy['withdraw'] and sell['deposit']:
                    strat['buy']['networks'] = [buy]
                    strat['sell']['networks'] = [buy]
                    break
        return strats
                
    @logger.catch
    def main(self, orderbooks):
        aloc_data = self.allocation(orderbooks)
        converted_data = self.convert_orderbooks(aloc_data)
        smart_sort_data = self.smart_sort(converted_data)
        sorted_data = self.networks_sorter(smart_sort_data)
        return sorted_data



#orderbooks = {'binance': {'BTCUSDT': {'asks': [['42426.46000000', '5.41978000'], ['42426.71000000', '0.00168000'], ['42426.72000000', '0.04691000'], ['42426.84000000', '0.28000000'], ['42426.95000000', '0.00021000'], ['42426.97000000', '0.00500000'], ['42427.12000000', '0.47139000'], ['42427.19000000', '0.00021000'], ['42427.37000000', '0.58585000'], ['42427.78000000', '0.00018000']], 'bids': [['42426.45000000', '2.85417000'], ['42426.40000000', '0.00018000'], ['42426.36000000', '0.07151000'], ['42426.23000000', '0.00021000'], ['42426.09000000', '0.44129000'], ['42426.00000000', '0.24140000'], ['42425.99000000', '0.00021000'], ['42425.77000000', '0.02400000'], ['42425.75000000', '0.00021000'], ['42425.51000000', '0.00021000']], 'exchange': 'binance', 'token': 'BTCUSDT'}, 'ETHUSDT': {'asks': [['2274.49000000', '27.90410000'], ['2274.50000000', '7.61440000'], ['2274.51000000', '0.00680000'], ['2274.53000000', '2.36700000'], ['2274.54000000', '0.80680000'], ['2274.56000000', '0.00320000'], ['2274.57000000', '1.60680000'], ['2274.58000000', '8.82390000'], ['2274.60000000', '1.95080000'], ['2274.63000000', '6.38470000']], 'bids': [['2274.48000000', '56.26950000'], ['2274.47000000', '11.64420000'], ['2274.46000000', '1.31960000'], ['2274.45000000', '0.01150000'], ['2274.44000000', '2.37660000'], ['2274.42000000', '0.00920000'], ['2274.41000000', '0.00230000'], ['2274.40000000', '2.74050000'], ['2274.39000000', '1.32640000'], ['2274.36000000', '0.00680000']], 'exchange': 'binance', 'token': 'ETHUSDT'}}, 'bitget': {'BTCUSDT': {'asks': [['42424.30', '0.0019'], ['42424.35', '0.3099'], ['42424.36', '0.5497'], ['42424.40', '0.3536'], ['42425.39', '0.0109'], ['42425.90', '0.2325'], ['42426.00', '0.3778'], ['42426.40', '0.4319'], ['42426.45', '0.0003'], ['42426.46', '10.6884'], ['42426.60', '0.1391'], ['42426.71', '0.3318'], ['42426.72', '0.0571'], ['42426.73', '2.4882'], ['42426.75', '1.1440']], 'bids': [['42424.29', '0.6525'], ['42424.12', '0.0789'], ['42424.10', '0.0126'], ['42424.08', '0.1381'], ['42424.07', '0.0129'], ['42424.00', '0.5311'], ['42423.93', '0.1037'], ['42423.90', '1.1972'], ['42423.88', '0.1037'], ['42423.85', '0.1037'], ['42423.27', '0.0418'], ['42423.03', '1.2019'], ['42423.02', '0.1037'], ['42423.00', '0.0305'], ['42422.93', '0.0224']], 'exchange': 'bitget', 'token': 'BTCUSDT'}, 'ETHUSDT': {'asks': [['2274.56', '0.0095'], ['2274.57', '1.7535'], ['2274.58', '10.4411'], ['2274.60', '3.7065'], ['2274.61', '3.0400'], ['2274.63', '12.1309'], ['2274.64', '1.0041'], ['2274.65', '4.0331'], ['2274.66', '6.7462'], ['2274.67', '3.0607'], ['2274.68', '5.7610'], ['2274.69', '1.2415'], ['2274.70', '2.7387'], ['2274.71', '3.7387'], ['2274.72', '2.9993']], 'bids': [['2274.54', '1.0026'], ['2274.50', '1.0046'], ['2274.48', '106.9001'], ['2274.47', '11.0887'], ['2274.46', '2.5072'], ['2274.45', '1.0019'], ['2274.44', '4.5091'], ['2274.42', '1.0003'], ['2274.41', '1.0017'], ['2274.40', '1.8167'], ['2274.39', '1.6401'], ['2274.34', '2.9241'], ['2274.33', '2.6764'], ['2274.32', '3.4390'], ['2274.31', '1.2527']], 'exchange': 'bitget', 'token': 'ETHUSDT'}}, 'coinex': {'BTCUSDT': {'asks': [['42422.27', '0.98599831'], ['42427.59', '0.07505916'], ['42428.80', '0.53136893'], ['42428.81', '0.07036905'], ['42428.82', '0.00521187'], ['42428.88', '0.14071409'], ['42431.68', '0.41718885'], ['42431.88', '0.00221986'], ['42432.85', '0.02877179'], ['42433.68', '0.11076107']], 'bids': [['42419.23', '0.11079880'], ['42418.65', '0.00471190'], ['42410.72', '0.08192614'], ['42410.71', '0.07735078'], ['42407.68', '0.00070711'], ['42407.54', '0.10800000'], ['42407.06', '0.11083060'], ['42406.21', '0.01222364'], ['42406.10', '0.53136893'], ['42405.50', '0.00071769']], 'exchange': 'coinex', 'token': 'BTCUSDT'}, 'ETHUSDT': {'asks': [['2275.04', '3.14858725'], ['2275.21', '0.87931607'], ['2275.25', '1.22400953'], ['2275.26', '0.82627919'], ['2275.31', '0.41186702'], ['2275.33', '2.65666471'], ['2275.34', '0.41302523'], ['2275.40', '0.41311417'], ['2275.44', '3.51726427'], ['2275.45', '1.08875356']], 'bids': [['2275.03', '0.15607860'], ['2274.43', '2.22055312'], ['2274.38', '1.65319778'], ['2274.19', '0.08781146'], ['2273.95', '0.41337760'], ['2273.90', '0.41338669'], ['2273.82', '1.02966756'], ['2273.80', '0.41302523'], ['2273.76', '0.43905733'], ['2273.71', '0.82684247']], 'exchange': 'coinex', 'token': 'ETHUSDT'}}}

if __name__ == '__main__':
    gd = GeneralData()
    networks = gd.get_exchanges_networks()
    tokens = gd.filter_networks()
    anal = Analize(networks)
    strats = anal.main(get_orderbooks(tokens))
    strats = sorted(strats, key=lambda x:x['profit'])
    for strat in strats:
        print(strat['token'],'buy: ', strat['buy']['exchange'], strat['buy']['asks']['average'],'sell: ', strat['sell']['exchange'], strat['sell']['bids']['average'],'fee: ', strat['sell']['networks'][0]['fee'],'profit: ', strat['profit'])
    #print(anal.networks_sorter(smart_sort_data))
    #print(anal.smart_sort(converted_data))
    #print(anal.convert_orderbooks(aloc_data))
    #print(anal.allocation(orderbooks))
    #print(anal.convert_token('BITCOIN/USDT'))
