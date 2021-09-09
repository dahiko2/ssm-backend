from flask import Flask, request, Blueprint
import requests
import telebot
from config import token
from telebot import types
from mysql.connector import connect, Error
import Strings

salom_bot = Blueprint("salom_bot", __name__)
bot = telebot.TeleBot(token, threaded=False)

bot.remove_webhook()
bot.set_webhook(url="https://maksimsalnikov.pythonanywhere.com/salob/1994938654:AAHFLtVLwkog_4HK75-xTo8_-PA4vi4reuU/")

def read_creds():
    """
    Считывает данные для входа в бд из файла
    """
    global fhost, fuser, fpass, fdbname
    with open("credentials.txt") as f:
        fhost = f.readline()
        fuser = f.readline()
        fpass = f.readline().strip()
        f.readline()
        f.readline()# Просто пропускаем имя базы для инсты
        fdbname = f.readline().strip()

@salom_bot.route("/" + token + "/", methods=["POST"])
def receive_update():
    bot.process_new_updates([telebot.types.Update.de_json(request.stream.read().decode("utf-8"))])
    print("Message")
    return "ok", 200
    #chat_id = request.json["message"]["chat"]["id"]
    #send_message(chat_id, "Hello!")
    #return "ok"

@bot.message_handler(commands=['start'])
def start_message(message):

    read_creds()
    mydb = connect(
        host=fhost,
        user=fuser,
        password=fpass,
        database=fdbname
    )
    mycursor = mydb.cursor()

    roles = ['creator', 'administrator', 'member']
    mycursor.execute(
        'SELECT chat_id from users')
    Ids =mycursor.fetchall()
    chat_id = message.chat.id

    for id in Ids:
        if chat_id != id[0]:
            sql = 'INSERT INTO users (chat_id) VALUES (%s)'
            val = (chat_id,)
            mycursor.execute(sql, val)
            mydb.commit()
    else:
        bot.send_message(message.chat.id, 'Qaytganing bilan ' + message.chat.username + '!')
    #if bot.get_chat_member(chat_id=my_channel_id, user_id=message.from_user.id).status in roles:
    #    pass
    #else:
    #    bot.send_message(message.chat.id, '@SalomSerialBot kanaliga obuna bo'ling')

    markup = telebot.types.InlineKeyboardMarkup()
    markup.add(telebot.types.InlineKeyboardButton(text='Maktab', callback_data="maktab"))
    markup.add(telebot.types.InlineKeyboardButton(text='Qichchu Qudrat', callback_data=2))
    markup.add(telebot.types.InlineKeyboardButton(text='Shaharlik Qichloqi', callback_data=3))
    keyboard = telebot.types.ReplyKeyboardMarkup(True)
    keyboard.row('Serialar', 'Ortga')
    bot.send_message(message.chat.id,
                     Strings.start,
                     reply_markup=keyboard)
    bot.send_message(message.chat.id, 'Serialar',
                     reply_markup=markup)

@bot.callback_query_handler(func=lambda call: True)
def query_handler(call):

    #bot.answer_callback_query(callback_query_id=call.id, text='Спасибо за честный ответ!')

    keyboard = telebot.types.ReplyKeyboardMarkup(True)
    keyboard.row('Sevimli', 'Ortga')
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id)
    answer = ''
    if call.data == "maktab":
        answer = Strings.maktab_desc
        markup = telebot.types.InlineKeyboardMarkup()
        markup.add(telebot.types.InlineKeyboardButton(text='1 qism', callback_data=1))
        markup.add(telebot.types.InlineKeyboardButton(text='2 qism', callback_data=2))
        markup.add(telebot.types.InlineKeyboardButton(text='3 qism', callback_data=3))
        markup.add(telebot.types.InlineKeyboardButton(text='4 qism', callback_data=1))
        markup.add(telebot.types.InlineKeyboardButton(text='5 qism', callback_data=2))
        markup.add(telebot.types.InlineKeyboardButton(text='6 qism', callback_data=3))
        markup.add(telebot.types.InlineKeyboardButton(text='7 qism', callback_data=1))
        markup.add(telebot.types.InlineKeyboardButton(text='8 qism', callback_data=2))
        markup.add(telebot.types.InlineKeyboardButton(text='9 qism', callback_data=3))
        markup.add(telebot.types.InlineKeyboardButton(text='10 qism', callback_data=1))

    elif call.data == '2':
        answer = 'Вы хорошист!'
    elif call.data == '3':
        answer = 'Вы отличник!'

    bot.send_message(call.message.chat.id, answer, reply_markup=markup)

