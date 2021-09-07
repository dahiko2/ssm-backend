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
    chat_id = request.json["message"]["chat"]["id"]
    send_message(chat_id, "Hello!")
    return "ok"
