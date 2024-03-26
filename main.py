import time
import telebot
import threading
import configparser

from tele import *
from orderbooks import *

from queue import Queue
from analyze import Analize
from data import GeneralData

tokens = None
networks = None
anal = None

config = configparser.ConfigParser()
config.read('config.ini')
bot = telebot.TeleBot(config['Telegram']['apikey'])
data_queue = Queue()

def update_tokens():
    """Рекурсивная функция которая обновляет данные по сетям и 
    данные по токенам раз в 24 часа, а так же переинициирует класс
    Анализа с обновленными данными"""
    global tokens
    global networks
    global anal
    gd = GeneralData() 
    networks, tokens = gd.main()
    anal = Analize(networks)
    threading.Timer(86400, update_tokens).start()

def main():
    """Точка запуска всего приложения, запрашивает ордербуки и 
    отправляет их в функцию анализа, потом сортирует и проверяет
    на новые стратегии о которых оповещает в телеграмм"""
    old_strats= set()
    while True:
        orderbooks = get_orderbooks(tokens)
        strats = anal.main(orderbooks)
        strats = sorted(strats, key=lambda x:x['profit'])
        set_strats = set([strat['token'] for strat in strats])
        unique_strats = set_strats - old_strats
        old_strats = set_strats
        data_queue.put(strats)
        for strat in strats:
            if strat['token'] in unique_strats:
                mess = convert_message(strat)
                send_message_to_user(bot, mess)
        time.sleep(60)

if __name__ == '__main__':
    update_tokens()
    main_threrad = threading.Thread(target=main) # Основной технический поток
    tele_threrad = threading.Thread(target=start_bot, args=(bot, data_queue)) # Теллеграмм поток
    main_threrad.start()
    tele_threrad.start()
    tele_threrad.join()
    tele_threrad.join()
