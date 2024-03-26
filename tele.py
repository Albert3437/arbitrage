import time
import telebot
from telebot import types
from loguru import logger

logger.add('debug.log')


@logger.catch
def convert_message(strat):
    message=(f'''Токен: {strat['token']}, 
                покупка на бирже {strat['buy']['exchange']},
                по цене {strat['buy']['asks']['average'][0]}, 
                объемом  {round(float(strat['buy']['asks']['average'][1]) * float(strat['buy']['asks']['average'][0]), 2)}$.
                        
                Продажа на бирже {strat['sell']['exchange']}
                по цене {strat['sell']['bids']['average'][0]}, 
                объемом  {round(float(strat['sell']['bids']['average'][1]) * float(strat['sell']['bids']['average'][0]), 2)}$.
                        
                Доходность сделки: {strat['profit']} %
                Коммисия: {round(float(strat['sell']['networks'][0]['fee']) * float(strat['buy']['asks']['average'][0]), 2)} $
                ''')
    return message

# Функция для отправки сообщения (можно вызвать в любой момент)
@logger.catch
def send_message_to_user(bot, message):
    chat_id = '476600066'  # Замените на ваш Telegram ID
    bot.send_message(chat_id, message)

def start_bot(bot, data_queue):
    # Обработчик команды '/start'
    strats = []
    @bot.message_handler(commands=['start'])
    def welcome(message):
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        markup.add(types.KeyboardButton("Получить стратегии"))
        bot.send_message(message.chat.id, "Дорова, епта!", reply_markup=markup)

    # Обработчик нажатия на кнопку
    @bot.message_handler(func=lambda call: True)
    def message_handler(message):
        if message.text == "Получить стратегии":
            global strats
            if not data_queue.empty():
                strats = data_queue.get()
            for strat in strats:
                if len(strats) > 0:
                    mess = convert_message(strat)
                    bot.send_message(message.chat.id, mess)
                else:
                    bot.send_message(message.chat.id, "Нету")

    while True:
        try:    
            bot.polling(non_stop=True)
        except Exception as e:
            logger.error(e)
            time.sleep(5)


if __name__ == '__main__':
    bot = telebot.TeleBot('6317419292:AAEKY0HHw-woqfGo7vUQdyOeBIuOwsO__Ow')
    start_bot(bot, None)