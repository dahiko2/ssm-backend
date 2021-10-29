"""
Пакеты.
json, datetime, os, hmac, hashlib - стандартная библиотека питона.
os - получение переменных из среды. hmac и hashlib для сравнения secret_key от гитхаба с переменной в ОС.
mysql-connector-python 8.0.17 - подключение к базе данных.
flask 2.0.1 - серверная часть API.
flask-cors 3.0.10 - CORS, для выполнения запросов со сторонних сайтов.
urllib3 1.25.11 - работа с URL.
GitPython 3.1.18 - обновление сервера на PythonAnywhere при push в git-репозиторий.
"""
import mysql.connector
import flask
import git
import os
import hmac
import hashlib
from flask import request
from flask_cors import CORS
from instagram import insta_bp
from ssm import ssm
from Salom_Bot import salom_bot

"""
Глобальные переменные. mydb - создание пустого подключения к бд. f% - реквизиты для аутентификации и подключения к бд.
"""
AUTH = False
mydb = mysql.connector.connect()
fhost = ""
fuser = ""
fpass = ""
fdbname_insta = ""
fdbname_ssm = ""
"""
Создание базового объекта Flask и обертка его в CORS.
Подключение blueprint'ов.
"""
app = flask.Flask(__name__)
app.register_blueprint(insta_bp, url_prefix='/instagram')
app.register_blueprint(ssm, url_prefix='/ssm')
app.register_blueprint(salom_bot, url_prefix='/salob')
cors = CORS(app, resources={
    r"/*": {
        "origins": "*"  # origins - список сайтов с которых можно делать запрос игнорируя CORS, поставить * для любых сайтов
    }
})


def read_creds():
    """
    Построчно считывает данные для подключения к бд из файла credentials.txt.
    """
    global fhost, fuser, fpass, fdbname_insta, fdbname_ssm
    with open("credentials.txt") as f:
        fhost = f.readline().strip()
        fuser = f.readline().strip()
        fpass = f.readline().strip()
        fdbname_insta = f.readline().strip()
        fdbname_ssm = f.readline().strip()


def is_valid_signature(x_hub_signature, data, private_key):
    """
    Проверка Secret Key на валидность (честно спизжено с гитхаба)
    """
    hash_algorithm, github_signature = x_hub_signature.split('=', 1)
    algorithm = hashlib.__dict__.get(hash_algorithm)
    encoded_key = bytes(private_key, 'latin-1')
    mac = hmac.new(encoded_key, msg=data, digestmod=algorithm)
    return hmac.compare_digest(mac.hexdigest(), github_signature)


@app.route("/update_server", methods=['GET', 'POST'])
def git_webhook():
    """
    Webhook, который вызывается когда происходит push в мастер ветку в github'e
    Он защищен проверкой Secret поля и отсеивает нежелательные запросы
    плз начни работать заебал
    :return: flask.Response
    """
    if request.method != 'POST':
        return "OK"
    else:
        abort_code = 418
        if 'X-Github-Event' not in request.headers:
            flask.abort(abort_code)
        if 'X-Github-Delivery' not in request.headers:
            flask.abort(abort_code)
        if 'X-Hub-Signature' not in request.headers:
            flask.abort(abort_code)
        if not request.is_json:
            flask.abort(abort_code)
        if 'User-Agent' not in request.headers:
            flask.abort(abort_code)
        ua = request.headers.get('User-Agent')
        if not ua.startswith('GitHub-Hookshot/'):
            flask.abort(abort_code)
        x_hub_signature = request.headers.get('X-Hub-Signature')
        w_secret = os.getenv('SECRET_KEY')
        if not is_valid_signature(x_hub_signature, request.data, w_secret):
            print('Deploy signature failed: {sig}'.format(sig=x_hub_signature))
            flask.abort(abort_code)
        repo = git.Repo('/home/maksimsalnikov/ssm-backend/')
        origin = repo.remotes.origin
        origin.pull()
        return 'Updated PythonAnywhere successfully', 200


@app.before_request
def validate_auth():
    """
    Если аутентификация включена, сравнивает enviroment variable AUTH с полем auth из тела запроса, присланного на сервер.
    Если они не совпадают, то сервер отвечает '401' клиенту.
    :return:
    """
    if AUTH:
        if flask.request.endpoint != 'git_webhook':
            body = flask.request.headers.get('auth')
            if body is not None:
                if body != os.getenv('AUTH'):
                    flask.abort(401)
            else:
                flask.abort(401)


@app.route("/google01dd5a969f463cdb.html")
def google_webhook_verify():
    return flask.render_template("google16b6d70bf4da1503.html")

