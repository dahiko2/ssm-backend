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

@salom_bot.route("/" + token + "/", methods=["POST"])
def receive_update():
    bot.process_new_updates(
        [telebot.types.Update.de_json(request.stream.read().decode("utf-8"))]
    )
    return "ok"
    #chat_id = request.json["message"]["chat"]["id"]
    #send_message(chat_id, "Hello!")
    #return "ok"

@bot.message_handler(commands=['start'])
def start_message(message):
    keyboard = telebot.types.ReplyKeyboardMarkup(True)
    keyboard.row('START')
    bot.send_message(message.chat.id, 'Assalomu alaykum!\nSerialni tanlang, qaysi birini tomosha qilishni istaysiz? Yoki botga serialni nomini yozing.',
                     reply_markup=keyboard)
    print("Start Button")