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
import json
import mysql.connector
import flask
import git
import os
import hmac
import hashlib
from flask import request
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from flask_cors import CORS


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
"""
app = flask.Flask(__name__)
CORS(app)
cors = CORS(app, resources={
    r"/*": {
        "origins": "https://salemsocial.kz/"  # origins - список сайтов с которых можно делать запрос игнорируя CORS, поставить * для любых сайтов
    }
})


def read_creds():
    """Построчно считывает данные для подключения к бд из файла credentials.txt."""
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


def instagram_connection():
    """
    Подключается к базе данных - к схеме insta и выставляет для курсора тайм-зону Алматы (UTC+6).
    :return: mysql.connection.cursor
    """
    global mydb
    mydb = mysql.connector.connect(
        host=fhost,
        user=fuser,
        password=fpass,
        database=fdbname_insta
    )
    mydb.time_zone = "+06:00"
    return mydb.cursor()


def ssm_connection():
    """
    Подключается к базе данных - к схеме ssm и выставляет для курсора тайм-зону Алматы (UTC+6).
    :return: mysql.connection.cursor
    """
    global mydb
    mydb = mysql.connector.connect(
        host=fhost,
        user=fuser,
        password=fpass,
        database=fdbname_ssm
    )
    mydb.time_zone = "+06:00"
    return mydb.cursor()


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


def calculate_cp(price, metric):
    """
    Считает стоимость за метрику, если параметры пустые (None) то возвращает 0.
    :param price: int
    :param metric: int
    :return: float
    """
    if price is None or metric is None or metric == 0:
        return 0
    return price / metric


def get_yt_id(url):
    """
    Принимает ссылку на видео ютуба или его id, очищает его от всего лишнего и возвращает только youtube id.
    :param url: str
    :return: str
    """
    res = ""
    doNext = True
    u_pars = urlparse(url)
    quer_v = parse_qs(u_pars.query).get('v')
    if quer_v:
        res = quer_v[0]
        doNext = False
    if doNext:
        pth = u_pars.path.split('/')
        if pth:
            res = pth[-1]
    temp = res
    if temp[:2] == 'v=':
        temp = res[2:]
    elif res[0] == '=':
        temp = res[1:]
    n = temp.find("&")
    if n != -1:
        return temp[:n]
    return temp


def get_youtube_channels(mycursor):
    """
    Возвращает список всех ютуб каналов из бд.
    :param mycursor: mysql.connector.cursor
    :return: list
    """
    youtube_channels = []
    query = "SELECT name FROM channels;"
    mycursor.execute(query)
    query_result = mycursor.fetchall()
    for row in query_result:
        youtube_channels.append(row[0])
    return youtube_channels


def get_today_trends_videos(mycursor):
    """
    Возвращает список названий всех видео в трендах за сегодняшний день.
    :param mycursor: mysql.connector.cursor
    :return: list
    """
    today_videos = []
    query = "SELECT video_name FROM youtube_trends WHERE DATE(date) = CURDATE();"
    mycursor.execute(query)
    query_result = mycursor.fetchall()
    for row in query_result:
        today_videos.append(row[0])
    return today_videos


def form_insta_post_dict(row):
    """
    Принимает row - результат запроса в бд.
    Возвращает словарь с данными о посте.
    :param row: list
    :return: dict
    """
    temp = dict()
    temp["id"] = row[0]
    temp["shortlink"] = row[1]
    temp["isvideo"] = row[2]
    temp["comments"] = row[3]
    temp["likes"] = row[4]
    temp["video_views"] = row[6]
    temp["upload_date"] = str(row[5])
    return temp


@app.route("/instagram/<account>", methods=['GET'])
def get_instagram_profile(account):
    """
    Принимает str - название существующего аккаунта в бд.
    Если постов нет, то сумма лайков и комметариев равняется 0.
    Если аккаунта нет, возвращает пустой json-объект.
    Если есть возвращает всю информацию об аккаунте из бд и считает сумму лайков, комментариев по постам, которые хранятся в табличке.
    :param account: str
    :return: json
    """
    account = str(account)
    mycursor = instagram_connection()
    query = "SELECT * FROM profile WHERE profilename = %s;"
    val = (account,)
    mycursor.execute(query, val)
    query_result_info = mycursor.fetchall()
    if query_result_info is None:
        return "{}"
    query = "SELECT SUM(likes) from posts where profileID in (select idprofile from profile where profilename = %s);"
    val = (account,)
    mycursor.execute(query, val)
    query_result_likes = mycursor.fetchone()
    if query_result_likes is None:
        query_result_likes = [0]
        query_result_comments = [0]
    else:
        query = "SELECT SUM(comments) from posts where profileID in (select idprofile from profile where profilename = %s);"
        val = (account,)
        mycursor.execute(query, val)
        query_result_comments = mycursor.fetchone()
    temp = dict()
    for row in query_result_info:
        temp["id"] = row[0]
        temp["name"] = row[1]
        temp["posts"] = row[2]
        temp["followers"] = row[3]
        temp["likes"] = str(query_result_likes[0])
        temp["comments"] = str(query_result_comments[0])
        temp["engagement"] = ((float(query_result_likes[0]) + float(query_result_comments[0])) / float(row[2])) / float(row[3]) * 100
    result = json.dumps(temp, indent=4)
    return result


@app.route("/instagram/<account>/posts", methods=['GET'])
def get_instagram_posts(account):
    """
    Принимает str - название существующего аккаунта в бд.
    Выводит все посты аккаунта с названием <account>.
    Если постов нет, возвращает пустой json объект.
    Если есть возвращает json объект - список словарей с информацией о постах.
    :param account: str
    :return: json
    """
    mycursor = instagram_connection()
    query = "select * from posts where profileID in (select idprofile from profile where profilename = %s);"
    val = (str(account),)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    if query_result is None:
        return "{}"
    itemlist = []
    for row in query_result:
        itemlist.append(form_insta_post_dict(row))
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/instagram/<account>/posts/top_likes<int:n>", methods=['GET'])
def get_instagram_posts_top_by_likes(account, n):
    """
    Принимает str - название существующего аккаунта в бд и int - кол-во постов для вывода.
    Выводит топ <n> постов по лайкам из бд (отсортированные по убыванию).
    Если постов нет возвращает пустой json-объект.
    Если есть возвращает json объект - список словарей с информацией о постах.
    :param account: str
    :param n: int
    :return: json
    """
    mycursor = instagram_connection()
    query = "select * from posts where profileID in (select idprofile from profile where profilename = %s) ORDER BY likes DESC LIMIT %s;"
    val = (str(account), n)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    if query_result is None:
        return "{}"
    itemlist = []
    for row in query_result:
        itemlist.append(form_insta_post_dict(row))
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/instagram/<account>/posts/top_comments<int:n>", methods=['GET'])
def get_instagram_posts_top_by_comments(account, n):
    """
    Принимает str - название существующего аккаунта в бд и int - кол-во постов для вывода.
    Выводит топ <n> постов по комментариям из бд (отсортированные по убыванию).
    Если постов нет возвращает пустой json-объект.
    Если есть возвращает json объект - список словарей с информацией о постах.
    :param account: str
    :param n: int
    :return: json
    """
    mycursor = instagram_connection()
    query = "select * from posts where profileID in (select idprofile from profile where profilename = %s) ORDER BY comments DESC LIMIT %s;"
    val = (str(account), n)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    if query_result is None:
        return "{}"
    itemlist = []
    for row in query_result:
        itemlist.append(form_insta_post_dict(row))
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/instagram/<account>/posts/top_videos<int:n>", methods=['GET'])
def get_instagram_posts_top_by_videos(account, n):
    """
    Принимает str - название существующего аккаунта в бд и int - кол-во постов для вывода.
    Выводит топ <n> видео-постов по просмотрам из бд (отсортированные по убыванию).
    Если постов нет возвращает пустой json-объект.
    Если есть возвращает json объект - список словарей с информацией о постах.
    :param account: str
    :param n: int
    :return: json
    """
    mycursor = instagram_connection()
    query = "select * from posts where profileID in (select idprofile from profile where profilename = %s) ORDER BY video_views DESC LIMIT %s;"
    val = (str(account), n)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    if query_result is None:
        return "{}"
    itemlist = []
    for row in query_result:
        itemlist.append(form_insta_post_dict(row))
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/instagram/top_followers", methods=['GET'])
def get_top_by_followers():
    """
    Собирает список инстаграмм аккаунтов, отсортированных по убыванию, по кол-ву подписчиков.
    Возвращает json объект - список словарей с информацией об аккаунтах.
    :return: json
    """
    mycursor = instagram_connection()
    query = "SELECT idprofile, profilename, followers FROM profile ORDER BY followers DESC"
    mycursor.execute(query)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        temp = dict()
        temp["id"] = row[0]
        temp["profilename"] = row[1]
        temp["followers"] = row[2]
        itemlist.append(temp)
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/instagram/top_video<int:n>", methods=['GET'])
def get_top_by_video_views(n):
    """
    Собирает список топ <n> постов, отсортированных по убыванию, по просмотрам видео.
    Возвращает json объект - список словарей с информацией о постах.
    :param n: int
    :return: json
    """
    mycursor = instagram_connection()
    query = "select idposts, shortlink, comments, likes, video_views,  profilename from posts, profile where profileID = idprofile ORDER BY video_views DESC LIMIT %s;"
    val = (n,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        temp = dict()
        temp["id"] = row[0]
        temp["shortlink"] = row[1]
        temp["comments"] = row[2]
        temp["likes"] = row[3]
        temp["video_views"] = row[4]
        temp["profilename"] = row[5]
        itemlist.append(temp)
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/instagram/top_likes<int:n>", methods=['GET'])
def get_top_by_likes(n):
    """
    Собирает список топ <n> постов, отсортированных по убыванию, по лайкам.
    Возвращает json объект - список словарей с информацией о постах.
    :param n: int
    :return: json
    """
    mycursor = instagram_connection()
    query = "select idposts, shortlink, comments, likes, video_views,  profilename from posts, profile where profileID = idprofile ORDER BY likes DESC LIMIT %s;"
    val = (n,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        temp = dict()
        temp["id"] = row[0]
        temp["shortlink"] = row[1]
        temp["comments"] = row[2]
        temp["likes"] = row[3]
        temp["video_views"] = row[4]
        temp["profilename"] = row[5]
        itemlist.append(temp)
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/instagram/top_comments<int:n>", methods=['GET'])
def get_top_by_comments(n):
    """
    Собирает список топ <n> постов, отсортированных по убыванию, по кол-ву комментариев.
    Возвращает json объект - список словарей с информацией о постах.
    :param n: int
    :return: json
    """
    mycursor = instagram_connection()
    query = "select idposts, shortlink, comments, likes, video_views,  profilename from posts, profile where profileID = idprofile ORDER BY comments DESC LIMIT %s;"
    val = (n,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        temp = dict()
        temp["id"] = row[0]
        temp["shortlink"] = row[1]
        temp["comments"] = row[2]
        temp["likes"] = row[3]
        temp["video_views"] = row[4]
        temp["profilename"] = row[5]
        itemlist.append(temp)
    result = json.dumps(itemlist, indent=4)
    return result


def form_proj_info_dict(row):
    """
    Принимает row - результат запроса в бд.
    Возвращает словарь с данными о релизе.
    Используется чтобы обрабатывать результат запроса 'SELECT * FROM releases'.
    :param row: list
    :return: dict
    """
    item = dict()
    item["id"] = row[16]
    item["yt_id"] = row[0]
    item["episode_name"] = row[1]
    item["uniq_year"] = row[2]
    item["traffic"] = row[3]
    item["release_date"] = str(row[4])
    item["tail"] = row[5]
    item["traffic_per_day"] = row[6]
    item["traffic_per_tail"] = row[7]
    item["youtube_views"] = row[8]
    item["youtube_likes"] = row[22]
    item["youtube_comments"] = row[23]
    item["avg_view_by_user"] = row[9]
    item["shows"] = row[10]
    item["ctr"] = row[11]
    item["uniq_users_youtube"] = row[12]
    item["subscribers"] = row[13]
    item["project_id"] = row[14]
    item["price"] = row[15]
    item["uniq_release_month"] = row[17]
    item["uniq_second_month"] = row[18]
    item["cpv"] = calculate_cp(row[15], row[8])
    item["cpu"] = calculate_cp(row[15], row[2])
    item["cpc"] = calculate_cp(row[15], row[3])
    item["male"] = row[19]
    item["female"] = row[20]
    item["retention"] = row[21]
    item["avg_tail"], item["avg_retention"], item["avg_cpc"] = get_project_averages(row[14])
    gender = "M-F"
    if row[19] is not None and row[19] != '0%':
        if float(row[19]) > 60.0:
            gender = "M"
        elif float(row[20]) > 60.0:
            gender = "F"
    item["gender"] = gender
    return item


@app.route("/ssm/get_projects", methods=['GET'])
def get_projects():
    """
    Собирает ID проекта в базе и его название в список.
    Возвращает json-объект, список проектов.
    :return: json
    """
    mycursor = ssm_connection()
    query = "SELECT ProjectID, ProjectName from project;"
    mycursor.execute(query)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        item = dict()
        item["id"] = row[0]
        item["name"] = row[1]
        itemlist.append(item)
    result = json.dumps(itemlist, indent=4)
    return result


def get_project_averages(project_id):
    """
    Выводит средние значения для всего проекта по Хвосту, Досматриваемости и CPC.
    :param project_id: int - id проекта
    :return: float, float, float
    """
    mycursor = ssm_connection()
    # Вытаскиваем среднюю длину хвоста, где значение хвоста задано, и значени хвоста у первой серии не учитывается (для этого нужна сортировка по дате и лимит 1)
    query = "select AVG(tail) from (select EpisodesName, tail, ReleaseDate from releases where ProjectID = %s and Tail is not null ORDER BY ReleaseDate ASC LIMIT 1, 1000) as T;"
    val = (project_id, )
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    avg_tail = 0
    for row in query_result:
        avg_tail = 0
        if row[0] is not None:
            avg_tail = float(row[0])
    # Вытаскиваем досматриваемость, цену и переходы.
    query = "select AudienceRetention, Price, Traffic, EpisodesName from releases where ProjectID = %s ORDER BY ReleaseDate ASC;"
    val = (project_id, )
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    summary_retention = 0
    count_retention = 0
    summary_cpc = 0
    count_cpc = 0
    avg_cpc = 0
    avg_retention = 0
    first_ep = ["1 серия", "1 часть", "серия 1", "#1", "еp.1", "еp. 1", "часть 1", "ep.1", "ep. 1", "1 бөлім", "бөлім 1"]
    # Считаем суммы для СРС и досматриваемости
    for row in query_result:
        # Если в названии серии нет строки из списка first_list то считаем среднее срс (Если не первая серия)
        if not any(substring in str(row[3]).lower() for substring in first_ep):
            count_cpc += 1
            summary_cpc += calculate_cp(row[1], row[2])
        # Пропускаем досматриваемость в 0% как незаполненную (условный костыль пока не пофиксим автозаполнение досматриваемости)
        if row[0] == "0%":
            pass
        count_retention += 1
        summary_retention += float(row[0][:-1].replace(",", "."))
    # Если досматриваемость незаполнена, ее нет, то средняя отдается как 0, в ином случае считается средняя арифметическая, тоже самое для СРС
    if count_retention != 0:
        avg_retention = summary_retention / count_retention
    if count_cpc != 0:
        avg_cpc = summary_cpc / count_cpc
    return avg_tail, avg_retention, avg_cpc


@app.route("/ssm/info_byprojectid=<int:project_id>", methods=['GET'])
def get_fullinfo_by_projectid(project_id):
    """
    Собирает список релизов определенного проекта, заданного через project_id
    Возвращает json-объект, список релизов.
    :param project_id: int
    :return: json
    """
    mycursor = ssm_connection()
    query = "SELECT * FROM releases where projectID = %s ORDER BY ReleaseDate ASC;"
    val = (project_id,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        itemlist.append(form_proj_info_dict(row))
    result = json.dumps(itemlist, indent=4)
    return result
        

@app.route("/ssm/info_byprojectname=<project>", methods=['GET'])
def get_fullinfo_by_projectname(project):
    """
    Собирает список релизов определенного проекта, заданного через название проекта (полное или частичное)
    Возвращает json-объект, список релизов.
    :param project: int
    :return: json
    """
    project = str(project) + "%"
    mycursor = ssm_connection()
    query = "SELECT * FROM releases where projectID in (select ProjectID from project where ProjectName like %s) ORDER BY ReleaseDate ASC;"
    val = (project,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        itemlist.append(form_proj_info_dict(row))
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/ssm/releases_for_<int:n>_<period>", methods=['GET'])
def get_releases_by_period(n, period):
    """
    Собирает список релизов за определенный период, указанный в параметрах, где
    n - кол-во выбранных периодов, period - период 1 из ниже перечисленных.
    Возвращает json-объект, список релизов.
    :param period: enum (DAY, WEEK, MONTH, YEAR)
    :param n: int
    :return: json
    """
    # CURDATE - INTERVAL N PERIOD
    periods = ["DAY", "WEEK", "MONTH", "YEAR"]
    period = period.upper()
    if period not in periods:
        flask.abort(403)
    mycursor = ssm_connection()
    query = "SELECT * FROM releases " \
            "WHERE ReleaseDate > DATE_SUB(CURDATE(), INTERVAL %s " + period + ") ORDER BY projectID DESC;"
    val = (n,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        itemlist.append(form_proj_info_dict(row))
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/ssm/releases_between_<date1>_and_<date2>", methods=['GET'])
def get_releases_between(date1, date2):
    """
    Собирает список релизов за определенные даты, указанные в параметрах.
    Возвращает json-объект, список релизов.
    :param date1: str (date format yyyy.mm.dd)
    :param date2: str (date format yyyy.mm.dd)
    :return: json
    """
    mycursor = ssm_connection()
    date1 = date1.replace(".", "/")
    date2 = date2.replace(".", "/")
    query = "SELECT * FROM releases where (ReleaseDate between %s and %s) ORDER BY projectID DESC;"
    val = (date1, date2)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        itemlist.append(form_proj_info_dict(row))
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/ssm/kpi_<year>_<country>", methods=['GET'])
def get_kpi_by_country_year(year, country):
    """
    Собирает данные по KPI с значениемя за указанный год и страну.
    Возвращает json-объект, список словарей.
    :param year: int
    :param country: str
    :return: json
    """
    country = country.upper()
    mycursor = ssm_connection()
    query = "SELECT idkpi, value, target, month FROM kpi_mao where year = %s and country = %s;"
    val = (year, country)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        item = dict()
        item["value"] = row[1]
        item["target"] = row[2]
        item["month"] = row[3]
        itemlist.append(item)
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/ssm/updatekpimau", methods=['POST'])
def update_kpi_mau():
    """
    POST метод. Обновляет таблицу с MAU KPI.
    Параметры считываются через тело запроса как json объект.
    Пример тела запроса:
    {"value": 123456, "country": "KAZAKHSTAN"}
    Возвращает код 200 и сообщение о успешном выполнении. В любом другом случае отдает код 403.
    :return: str
    """
    body = flask.request.get_json()
    if body is None:
        flask.abort(403)
    data = json.loads(str(body).replace("'", '"'))
    try:
        value = data['value']
        country = data['country']
    except KeyError:
        flask.abort(403)
    else:
        global mydb
        mycursor = ssm_connection()
        months = [0, "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь",
                  "Ноябрь", "Декабрь"]
        now = datetime.today()
        query = "INSERT INTO kpi_mao (value, country, month, year, month_year) VALUES (%s, %s, %s, %s, %s);"
        if now.month < 10:
            month = '0' + str(now.month)
        else:
            month = str(now.month)
        val = (value, country, months[now.month], now.year, month + str(now.year) + str(country))
        try:
            mycursor.execute(query, val)
        except mysql.connector.errors.IntegrityError:  # если данные по стране уже заполнены, то просто обновляет их
            query = "UPDATE kpi_mao SET value = %s where country = %s and month = %s and year = %s and month_year = %s;"
            mycursor.execute(query, val)
        mydb.commit()
        return "MAU KPI of" + country + " updated."


@app.route("/ssm/update_yt_trends", methods=['POST'])
def update_yt_trends():
    """
    POST метод. Обновляет таблицу с трендами ютуба.
    Добавляет видео в таблицу и записывает дату обновления.
    Если видео за сегоднящную дату уже есть в базе, то просто обновляет его место.
    Параметры считываются через тело запроса как json объект.
    Пример тела запроса:
    {"video_name": "asdfzxcv", "channel": "SALEM", "views": 12345678, "place": 1}
    Возвращает код 200 и сообщение о кол-во добавленных или обновленных видео. В любом другом случае отдает код 403.
    :return: str
    """
    body = flask.request.get_json()
    if body is None:
        flask.abort(403)
    global mydb
    mycursor = ssm_connection()
    count = 0
    video_list = json.loads(str(body).replace("'", '"'))
    youtube_channels = get_youtube_channels(mycursor)
    today_videos = get_today_trends_videos(mycursor)
    for video in video_list:
        if youtube_channels.count(video['channel']) > 0:
            count += 1
            if today_videos.count(video['video_name']) > 0:
                query = "UPDATE youtube_trends SET place = %s WHERE video_name = %s and DATE(date) = CURDATE();"
                values = (video['place'], video['video_name'])
            else:
                query = "INSERT INTO youtube_trends (video_name, channel, views, place, date) VALUES (%s, %s, %s, %s, NOW());"
                values = (video['video_name'], video['channel'], video['views'], video['place'])
            mycursor.execute(query, values)
    mydb.commit()
    return str(count)+" videos added."


@app.route("/ssm/get_yt_trends", methods=['GET'])
def get_yt_trends():
    """
    Получает ютуб тренды за текущую дату.
    Возвращает их в виде json-объекта, списка словарей.
    :return: json
    """
    mycursor = ssm_connection()
    query = "SELECT id, video_name, channel, views, place FROM youtube_trends WHERE DATE(date) = CURDATE() ORDER BY date DESC;"
    mycursor.execute(query)
    if mycursor.rowcount == 0:  # Костыль. Фиксит ошибку которая не показывает никакие видосы ночью нового дня.
        query = "SELECT id, video_name, channel, views, place FROM youtube_trends WHERE DATE(date) = DATE_SUB(CURDATE(), INTERVAL 1 DAY) ORDER BY date DESC;"
        mycursor.execute(query)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        item = dict()
        item["video_name"] = row[1]
        item["channel"] = row[2]
        item["views"] = row[3]
        item["place"] = row[4]
        itemlist.append(item)
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/ssm/add_yt_channel", methods=['POST'])
def add_yt_channel():
    """
    POST метод. Добавляет новую запись в таблицу с Ютуб Каналами.
    Параметры считываются через тело запроса как json объект.
    Пример тела запроса:
    {"channel": "SALEM"}
    Возвращает код 200 и сообщение о успешном выполнении. В любом другом случае отдает код 403.
    :return: str
    """
    body = flask.request.get_json()
    if body is None:
        flask.abort(403)
    body_input = json.loads(str(body).replace("'", '"'))
    try:
        channel = body_input['channel']
    except KeyError:
        flask.abort(403)
    else:
        mycursor = ssm_connection()
        global mydb
        query = "INSERT INTO channels (name) VALUES (%s);"
        values = (channel, )
        mycursor.execute(query, values)
        mydb.commit()
        return "Channel "+channel+" added."


@app.route("/ssm/get_kpi_aitu", methods=['GET'])
def get_kpi_aitu():
    """
    Получает значения KPI Aitu.
    Возвращает json-объект, список словарей.
    :return: json
    """
    mycursor = ssm_connection()
    sql = "SELECT target, `left`, top_50, top_100, quiz, releases, today, `quarter`, quarter_left from kpi_aitu;"   # Кавычки в запросе не убирать, иначе сломает запрос.
    mycursor.execute(sql)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        item = dict()
        item["target"] = row[0]
        item["left"] = row[1]
        item["top_50"] = row[2]
        item["top_100"] = row[3]
        item["quiz"] = row[4]
        item["releases"] = row[5]
        item["today"] = row[6]
        item["quarter"] = row[7]
        item["quarter_left"] = row[8]
        itemlist.append(item)
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/ssm/update_kpi_aitu", methods=['POST'])
def update_kpi_aitu():
    """
    POST метод. Обновляет значения в таблице KPI Aitu
    Параметры считываются через тело запроса как json объект.
    Пример тела запроса:
    { "target": 11111111, "left": 123456, "top_50": 1, "top_100": 3, "quiz": 0, "releases": 567890, "today": 1234, "quarter": 1337, "quarter_left": 90 }
    Возвращает код 200 и сообщение о успешном выполнении. В любом другом случае отдает код 403.
    :return: str
    """
    body = flask.request.get_json()
    if body is None:
        flask.abort(403)
    data = json.loads(str(body).replace("'", '"'))
    mycursor = ssm_connection()
    items = []
    try:
        items.append(data['target'])
        items.append(data['left'])
        items.append(data['top_50'])
        items.append(data['top_100'])
        items.append(data['quiz'])
        items.append(data['releases'])
        items.append(data['today'])
        items.append(data['quarter'])
        items.append(data['quarter_left'])
    except KeyError:
        flask.abort(403)
    query = 'UPDATE kpi_aitu set target = %s, `left` = %s, top_50 = %s, top_100 = %s, quiz = %s, releases = %s, today = %s, `quarter` = %s, quarter_left = %s WHERE id = 1'
    # Кавычки в запросе не убирать, иначе сломает запрос.
    val = (int(items[0]), int(items[1]), int(items[2]), int(items[3]), int(items[4]), int(items[5]), int(items[6]), int(items[7]), int(items[8]))
    mycursor.execute(query, val)
    mydb.commit()
    return "AITU KPI updated."


@app.route("/ssm/update_logs", methods=['POST'])
def update_logging():
    """
    POST метод. Обновляет логи (дата последнего успешного запуска скрипта).
    Параметры считываются через тело запроса как json объект.
    Пример тела запроса:
    {"type": "kpi_mao"}
    :return: str
    """
    body = flask.request.get_json()
    if body is None:
        flask.abort(403)
    data = json.loads(str(body).replace("'", '"'))
    types = [{"kpi_mao": 1}, {"kpi_aitu": 2}, {"aitube_utm": 3}, {"yt_trends": 4}]
    try:
        for item in types:
            if data["type"] == list(item.keys())[0]:
                query = "UPDATE log SET date = NOW() WHERE idlog = %s"
                value = (item[list(item.keys())[0]],)
                mycursor = ssm_connection()
                global mydb
                mycursor.execute(query, value)
                mydb.commit()
                return data["type"] + " log updated."
    except KeyError:
        flask.abort(403)


@app.route("/ssm/get_logs_<logtype>", methods=['GET'])
def get_logs(logtype):
    """
    Получает значение даты определенного лога.
    Возвращает json-объект, словарь.
    :param logtype: str
    :return: json
    """
    types = [{"kpi_mao": 1}, {"kpi_aitu": 2}, {"aitube_utm": 3}, {"yt_trends": 4}]
    mycursor = ssm_connection()
    try:
        for item in types:
            if logtype == list(item.keys())[0]:
                value = (item[list(item.keys())[0]],)
                query = "SELECT date FROM log WHERE idlog = %s"
                mycursor.execute(query, value)
                query_result = mycursor.fetchall()
                if query_result is None:
                    flask.abort(403)
                result = {"date": str(query_result[0][0])}
                return json.dumps(result, indent=4)
    except KeyError:
        flask.abort(403)


@app.route("/ssm/get_dashb_params", methods=['GET'])
def get_dashboard_params():
    """
    Получает параметры для дэшборда. Параметры заданы в коде.
    Возвращает json-объект, список словарей.
    :return: json
    """
    itemlist = []
    keys = ["color", "position", "cpv_youtube_m", "cpv_youtube_f", "cpv_youtube_mid", "cpu_aitube_m", "cpu_aitube_f", "cpu_aitube_mid", "youtube_ud"]
    items_1 = ["red", "bad", 12.5, 23.25, 17.88, 213.2, 396, 304.6, 20]
    items_2 = ["yellow", "middle", 5, 9.3, 7.15, 85.3, 158.4, 121.85, 40]
    items_3 = ["green", "great", 1, 1.86, 1.43, 17.1, 31.6, 24.35, 60]
    item_list = [items_1, items_2, items_3]
    for j in range(0, 3):
        item = dict()
        for i in range(0, 9):
            item[keys[i]] = item_list[j][i]
        itemlist.append(item)
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/ssm/get_utm_projects", methods=['GET'])
def get_utm_projects():
    """
    Получает названия проектов и соответсвующие названия UTM кампаний.
    Возвращает json-объект, список словарей.
    :return:
    """
    mycursor = ssm_connection()
    query = "select ProjectName, UtmName from project;"
    mycursor.execute(query)
    itemlist = []
    query_result = mycursor.fetchall()
    for row in query_result:
        item = dict()
        item["project_name"] = row[0]
        item["utm_name"] = row[1]
        itemlist.append(item)
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/ssm/", methods=['GET'])
def get_ssm_routes():
    """
    Выводит все доступные пути сервера (api endpoints).
    Возвращает json объект, список словарей
    todo: описание методов
    :return: json
    """
    itemlist = []
    for obj in app.url_map.iter_rules():
        if "ssm" in obj.__str__():
            item = dict()
            item["path"] = obj.__str__().replace('<', '"').replace('>', '"')
            itemlist.append(item)
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/ssm/get_milestone_releases", methods=['GET'])
def get_milestone_releases():
    """
    Выводит все релизы, которые приближаются к milestone
    Возвращает json-объект, список словарей
    :return: json
    """
    mycursor = ssm_connection()
    query = "select * from releases where ProjectID in (1, 2, 3, 4, 5, 7, 9, 10, 15, 76, 115, 117, 119, 120, 121, 122, 123) and (YouTubeViews between 950000 and 1000000) or (YouTubeViews between 1950000 and 2000000) or (YouTubeViews between 2950000 and 3000000) or (YouTubeViews between 3950000 and 4000000) or (YouTubeViews between 4950000 and 5000000) ORDER BY ReleaseDate Asc;"
    mycursor.execute(query)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        itemlist.append(form_proj_info_dict(row))
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/ssm/add_release", methods=['POST'])
def add_release():
    """

    :return:
    """
    body = flask.request.get_json()
    if body is not None:
        global mydb
        mycursor = ssm_connection()
        keys = []
        values = []
        for item in body.keys():  # формирование массивов из ключей словаря и соответствующих значений
            keys.append(item)
            values.append(str(body[item]))
        delimeter = ", "
        key_string = delimeter.join(keys)  # формирование строки для вставки в поля запроса. Как поля используются ключи словаря.
        placeholder = '%s'
        param_subs = ','.join((placeholder,) * len(values))  # формирование строки с определенным кол-вом параметров для запроса (%s, %s) - такого типа.
        query = "INSERT INTO releases ("+key_string+") VALUES ("+param_subs+")"
        val = tuple(values)
        try:
            mycursor.execute(query, val)
            mydb.commit()
        except mysql.connector.errors.Error as e:
            print("Error: "+e.msg)
            flask.abort(400)
    return "Release added."


@app.before_request
def validate_auth():
    """
    Если аутентификация включена, сравнивает enviroment variable AUTH с полем auth из тела запроса, присланного на сервер.
    Если они не совпадают, то сервер отвечает '401' клиенту.
    :return:
    """
    if AUTH:
        body = flask.request.get_json()
        try:
            if body is None or body["auth"] != os.getenv('AUTH'):
                flask.abort(401)
        except KeyError:
            flask.abort(401)


read_creds()  # Считывает данные для входа при запуске скрипта
