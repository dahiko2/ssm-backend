from flask import Flask, request, Blueprint
import requests
import telebot
from config import token
from telebot import types

salom_bot = Blueprint("salom_bot", __name__)
bot = telebot.TeleBot(token, threaded=False)

bot.remove_webhook()
bot.set_webhook(url="https://maksimsalnikov.pythonanywhere.com/salob/1994938654:AAHFLtVLwkog_4HK75-xTo8_-PA4vi4reuU/")
'''
def send_message(chat_id, text):
    method = "sendMessage"
    url = f"https://api.telegram.org/bot{token}/{method}"
    data = {"chat_id": chat_id, "text": text}
    requests.post(url, data=data)'''

@salom_bot.route("/" + token + "/", methods=["POST"])
def receive_update():
    bot.process_new_updates([telebot.types.Update.de_json(request.stream.read().decode("utf-8"))])
    print("Message")
    return "ok", 200
    #chat_id = request.json["message"]["chat"]["id"]
    #send_message(chat_id, "Hello!")
    #return "ok"

'''@bot.message_handler(commands=['start', 'help'])
def start_command(message):
    bot.send_message(message.chat.id, 'Hi *' + message.chat.first_name + '*!' , parse_mode='Markdown', reply_markup=types.ReplyKeyboardRemove())
'''

@bot.message_handler(commands=['/start'])
def start_message(message):
    keyboard = telebot.types.ReplyKeyboardMarkup(True)
    keyboard.row('START')
    bot.send_message(message.chat.id, 'Assalomu alaykum!\nSerialni tanlang, qaysi birini tomosha qilishni istaysiz? Yoki botga serialni nomini yozing.',
                     reply_markup=keyboard)
