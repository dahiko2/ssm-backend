from flask import Flask, request, Blueprint
import requests

salom_bot = Blueprint("salom_bot", __name__)

def send_message(chat_id, text):
    method = "sendMessage"
    token = "1994938654:AAHFLtVLwkog_4HK75-xTo8_-PA4vi4reuU"
    url = f"https://api.telegram.org/bot{token}/{method}"
    data = {"chat_id": chat_id, "text": text}
    requests.post(url, data=data)


@salom_bot.route("/", methods=["POST"])
def receive_update():
    chat_id = request.json["message"]["chat"]["id"]
    send_message(chat_id, "Hello!")
    return "ok"
