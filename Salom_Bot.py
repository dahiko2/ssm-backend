from flask import Flask, request, Blueprint
import requests
import telebot
from telebot.apihelper import ApiTelegramException

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
        f.readline()
        fdbname = f.readline().strip()

def serial_menu(message, start=False):
    markup = telebot.types.InlineKeyboardMarkup()
    markup.add(telebot.types.InlineKeyboardButton(text='Maktab', callback_data="maktab"))
    markup.add(telebot.types.InlineKeyboardButton(text='Qichchu Qudrat', callback_data="qichchu_qudrat"))
    markup.add(telebot.types.InlineKeyboardButton(text='Shaharlik Qichloqi', callback_data="shaharlik_qichilogi"))
    if start == False:
        #bot.edit_message_reply_markup(message.chat.id, message_id=message.message_id - 1, reply_markup='')
        #bot.edit_message_text("test", chat_id=message.chat.id, message_id=to_delete.message_id)
        bot.delete_messages(message.chat.id, message.message_id)
    bot.send_message(message.chat.id, 'Serialar',
                    reply_markup=markup)



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
    Ids = mycursor.fetchall()
    chat_id = message.chat.id
    count = 0

    '''if not bot.get_chat_member(chat_id=-1001584831368, user_id=message.from_user.id).status in roles:
        chat_id = message.chat.id
        bot.send_message(chat_id, "@salomserial kanaliga obuna bo'ling")''' #Проверка подписки, требуется админка

    for id in Ids:
        if chat_id == id[0]:
            count += 1

    if count == 0:
        sql = 'INSERT INTO users (chat_id) VALUES (%s)'
        val = (chat_id,)
        mycursor.execute(sql, val)
        mydb.commit()
        keyboard = telebot.types.ReplyKeyboardMarkup(True)
        keyboard.row('Sevimli')
        bot.send_message(message.chat.id,
                         Strings.start,
                         reply_markup=keyboard)
    else:
        keyboard = telebot.types.ReplyKeyboardMarkup(True)
        keyboard.row('Sevimli')
        bot.send_message(message.chat.id, 'Qaytganing bilan ' + message.chat.username + '!', reply_markup=keyboard)
    serial_menu(message, True)


@bot.message_handler(content_types=['text'])
def send_text(message):
    if message.text == 'Ortga':
        serial_menu(message)

@bot.callback_query_handler(func=lambda call: True)
def query_handler(call):

    global to_delete, to_delete_ser

    #bot.answer_callback_query(callback_query_id=call.id, text='Спасибо за честный ответ!')

    keyboard = telebot.types.ReplyKeyboardMarkup(True)
    keyboard.row('Sevimli', 'Ortga')
    answer = ''
    start_markup = telebot.types.InlineKeyboardMarkup()

    if call.data == "maktab":
        answer = Strings.maktab_desc
        btn1 = telebot.types.InlineKeyboardButton('1 qism', callback_data="mak1")
        btn2 = telebot.types.InlineKeyboardButton('2 qism', callback_data="mak2")
        start_markup.row(btn1, btn2)

        btn3 = telebot.types.InlineKeyboardButton('3 qism', callback_data="mak3")
        btn4 = telebot.types.InlineKeyboardButton('4 qism', callback_data="mak4")
        start_markup.row(btn3, btn4)

        btn5 = telebot.types.InlineKeyboardButton('5 qism', callback_data="mak5")
        btn6 = telebot.types.InlineKeyboardButton('6 qism', callback_data="mak6")
        start_markup.row(btn5, btn6)

        btn7 = telebot.types.InlineKeyboardButton('7 qism', callback_data="mak7")
        btn8 = telebot.types.InlineKeyboardButton('8 qism', callback_data="mak8")
        start_markup.row(btn7, btn8)

        btn9 = telebot.types.InlineKeyboardButton('9 qism', callback_data="mak9")
        btn10 = telebot.types.InlineKeyboardButton('10 qism', callback_data="mak10")
        start_markup.row(btn9, btn10)

    elif call.data == "qichchu_qudrat":
        answer = Strings.qichchu_qudrat
        btn1 = telebot.types.InlineKeyboardButton('1 qism', callback_data="qich_qud1")
        btn2 = telebot.types.InlineKeyboardButton('2 qism', callback_data="qich_qud3")
        start_markup.row(btn1, btn2)

        btn3 = telebot.types.InlineKeyboardButton('3 qism', callback_data="qich_qud4")
        btn4 = telebot.types.InlineKeyboardButton('4 qism', callback_data="qich_qud4")
        start_markup.row(btn3, btn4)

        btn5 = telebot.types.InlineKeyboardButton('5 qism', callback_data="qich_qud5")
        btn6 = telebot.types.InlineKeyboardButton('6 qism', callback_data="qich_qud6")
        start_markup.row(btn5, btn6)

        btn7 = telebot.types.InlineKeyboardButton('7 qism', callback_data="qich_qud7")
        btn8 = telebot.types.InlineKeyboardButton('8 qism', callback_data="qich_qud8")
        start_markup.row(btn7, btn8)

        btn9 = telebot.types.InlineKeyboardButton('9 qism', callback_data="qich_qud9")
        btn10 = telebot.types.InlineKeyboardButton('10 qism', callback_data="qich_qud10")
        start_markup.row(btn9, btn10)

    elif call.data == "shaharlik_qichilogi":
        answer = Strings.shaharlik_qichloqi
        btn1 = telebot.types.InlineKeyboardButton('1 qism', callback_data="shah_qich1")
        btn2 = telebot.types.InlineKeyboardButton('2 qism', callback_data="shah_qich2")
        start_markup.row(btn1, btn2)

        btn3 = telebot.types.InlineKeyboardButton('3 qism', callback_data="shah_qich3")
        btn4 = telebot.types.InlineKeyboardButton('4 qism', callback_data="shah_qich4")
        start_markup.row(btn3, btn4)

        btn5 = telebot.types.InlineKeyboardButton('5 qism', callback_data="shah_qich5")
        btn6 = telebot.types.InlineKeyboardButton('6 qism', callback_data="shah_qich6")
        start_markup.row(btn5, btn6)

        btn7 = telebot.types.InlineKeyboardButton('7 qism', callback_data="shah_qich7")
        btn8 = telebot.types.InlineKeyboardButton('8 qism', callback_data="shah_qich8")
        start_markup.row(btn7, btn8)

        btn9 = telebot.types.InlineKeyboardButton('9 qism', callback_data="shah_qich9")
        btn10 = telebot.types.InlineKeyboardButton('10 qism', callback_data="shah_qich10")
        start_markup.row(btn9, btn10)

    to_delete = bot.send_message(call.message.chat.id, answer, reply_markup=keyboard)
    to_delete_ser = bot.send_message(call.message.chat.id, Strings.series_chose, reply_markup=start_markup)
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id)

