from flask import Flask, request, Blueprint
import requests
import telebot
from config import token

salom_bot = Blueprint("salom_bot", __name__)

bot = telebot.TeleBot(token)

def send_message(chat_id, text):
    method = "sendMessage"
    url = f"https://api.telegram.org/bot{token}/{method}"
    data = {"chat_id": chat_id, "text": text}
    requests.post(url, data=data)

@bot.message_handler(commands=['start'])
def handle_start(message):
    user_markup = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True)
    user_markup.row('START')
    bot.send_message(message.from_user.id, 'Assalomu alaykum!\n'
                                           'Serialni tanlang, qaysi birini tomosha qilishni istaysiz? Yoki botga serialni nomini yozing.',
                     reply_markup=user_markup)

@salom_bot.route("/" + token + "/", methods=["POST"])
def receive_update():
    chat_id = request.json["message"]["chat"]["id"]
    send_message(chat_id, "Hello!")
    return "ok"
