import telebot
import json
from urllib.parse import urlparse
from urllib.parse import parse_qs
from flask import Flask, request, Blueprint

fssm_token = ""


def read_creds():
    """
    Построчно считывает данные для подключения к бд из файла credentials.txt.
    """
    global fssm_token
    with open("credentials.json") as f:
        credentials = json.load(f)
        """fhost = credentials['db_hostname']
        fuser = credentials['db_user']
        fpass = credentials['db_password']"""
        fssm_token = credentials['ssm_token']


read_creds()

ssm_bot = Blueprint("ssm_bot", __name__)
bot = telebot.TeleBot(fssm_token)
bot.set_webhook("https://YOUR_PYTHONANYWHERE_USERNAME.pythonanywhere.com/{}".format(fssm_token))


@bot.message_handler(commands=['utmcheck'])
def check_utm(message):
    bot.reply_to(message, message.text)
    """parsed_url = urlparse(url)
    captured_value = parse_qs(parsed_url.query)
    if 'utm_campaign' not in captured_value:
        return False
    return True"""