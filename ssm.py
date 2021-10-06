import base64
import json
import subprocess
import time
import flask
import mysql.connector
import requests
from flask import Blueprint
from datetime import datetime
from urllib.parse import urlparse, parse_qs


mydb = mysql.connector.connect()
fhost = ""
fuser = ""
fpass = ""
fdbname_ssm = ""
fkassa_login = ""
fkassa_password = ""


def read_creds():
    """
    Построчно считывает данные для подключения к бд из файла credentials.txt.
    """
    global fhost, fuser, fpass, fdbname_ssm, fkassa_login, fkassa_password
    with open("credentials.json") as f:
        credentials = json.load(f)
        fhost = credentials['db_hostname']
        fuser = credentials['db_user']
        fpass = credentials['db_password']
        fdbname_ssm = credentials['db_name_ssm']
        fkassa_login = credentials['kassa24_login']
        fkassa_password = credentials['kassa24_password']


read_creds()  # Считывает данные для входа при запуске скрипта
ssm = Blueprint('ssm', __name__)


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


def time_in_range(start, end, x):
    """Return true if x is in the range (start, end)"""
    if start <= end:
        return start < x < end
    else:
        return start < x or x < end


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
    item["youtube_release_date"] = str(row[24])
    item["season"] = row[25]
    item["tail"] = row[5]
    item["traffic_per_day"] = row[6]
    item["traffic_per_tail"] = row[7]
    item["youtube_views"] = row[8]
    item["aitube_views"] = row[27]
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
    item["cpu"] = calculate_cp(row[15], row[17])
    item["cpc"] = calculate_cp(row[15], row[3])
    item["male"] = row[19]
    item["female"] = row[20]
    item["retention"] = row[21]
    item["retention_kz"] = row[28]
    gender = "M-F"
    if row[19] is not None and row[19] != '0%':
        if float(row[19]) > 60.0:
            gender = "M"
        elif float(row[20]) > 60.0:
            gender = "F"
    item["gender"] = gender
    return item


def get_project_averages(project_id):
    """
    Выводит средние значения для всего проекта по Хвосту, Досматриваемости и CPC.
    :param project_id: int - id проекта
    :return: float, float, float
    """
    mycursor = ssm_connection()
    # Вытаскиваем среднюю длину хвоста, где значение хвоста задано, и значени хвоста у первой серии не учитывается (для этого нужна сортировка по дате и лимит 1)
    query = "SELECT AVG(tail) FROM (SELECT EpisodesName, tail, ReleaseDate FROM releases WHERE ProjectID = %s AND Tail IS NOT NULL ORDER BY ReleaseDate ASC LIMIT 1, 1000) AS t;"
    val = (project_id, )
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    avg_tail = 0
    for row in query_result:
        avg_tail = 0
        if row[0] is not None:
            avg_tail = float(row[0])
    # Вытаскиваем досматриваемость, цену и переходы.
    query = "SELECT AudienceRetention, Price, Traffic, EpisodesName FROM releases WHERE ProjectID = %s ORDER BY ReleaseDate ASC;"
    val = (project_id, )
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    summary_retention = 0
    count_retention = 0
    summary_cpc = 0
    count_cpc = 0
    avg_cpc = 0
    avg_retention = 0
    first_ep = ["1 серия", "1 часть", "серия 1", "#1", "еp.1", "еp. 1", "часть 1", "ep.1", "ep. 1", "1 бөлім", "бөлім 1", "трейлер", "тизер", "анонс"]
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
        try:
            summary_retention += float(row[0][:-1].replace(",", "."))
        except ValueError:
            summary_retention += 0
    # Если досматриваемость незаполнена, ее нет, то средняя отдается как 0, в ином случае считается средняя арифметическая, тоже самое для СРС
    if count_retention != 0:
        avg_retention = summary_retention / count_retention
    if count_cpc != 0:
        avg_cpc = summary_cpc / count_cpc
    return avg_tail, avg_retention, avg_cpc


@ssm.route("/get_projects", methods=['GET'])
def get_projects():
    """
    Собирает ID проекта в базе и его название, пол и возраст аудитории в список.
    Возвращает json-объект, список проектов.
    :return: json
    """
    mycursor = ssm_connection()
    query = "SELECT ProjectID, ProjectName, Gender, Age, UtmName FROM project ORDER BY ProjectName;"
    mycursor.execute(query)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        item = dict()
        item["id"] = row[0]
        item["name"] = row[1]
        item["gender"] = row[2]
        item["age"] = row[3]
        item["utm_name"] = row[4]
        itemlist.append(item)
    return json.dumps(itemlist, indent=4)


@ssm.route("/info_byprojectid=<int:project_id>", methods=['GET'])
def get_fullinfo_by_projectid(project_id):
    """
    Собирает список релизов определенного проекта, заданного через project_id
    Возвращает json-объект, список релизов.
    :param project_id: int
    :return: json(list[dict])
    """
    mycursor = ssm_connection()
    query = "SELECT * FROM releases WHERE projectID = %s ORDER BY ReleaseDate ASC;"
    val = (project_id,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        itemlist.append(form_proj_info_dict(row))
    return json.dumps(itemlist, indent=4)


@ssm.route("/info_byprojectname=<project>", methods=['GET'])
def get_fullinfo_by_projectname(project):
    """
    Собирает список релизов определенного проекта, заданного через название проекта (полное или частичное)
    Возвращает json-объект, список релизов.
    :param project: int
    :return: json(list[dict])
    """
    project = str(project) + "%"
    mycursor = ssm_connection()
    query = "SELECT * FROM releases WHERE projectID IN (SELECT ProjectID FROM project WHERE ProjectName LIKE %s) ORDER BY ReleaseDate ASC;"
    val = (project,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        itemlist.append(form_proj_info_dict(row))
    return json.dumps(itemlist, indent=4)


@ssm.route("/releases_for_<int:n>_<period>", methods=['GET'])
def get_releases_by_period(n, period):
    """
    Собирает список релизов за определенный период, указанный в параметрах, где
    n - кол-во выбранных периодов, period - период 1 из ниже перечисленных.
    Возвращает json-объект, список релизов.
    Автоматом делает выборку по дате релиза в айтубе, если передать в теле запроса {"type":"youtube"}, то выборка будет по дате с ютуба
    :param period: enum (DAY, WEEK, MONTH, YEAR)
    :param n: int
    :return: json(list[dict])
    """
    # CURDATE - INTERVAL N PERIOD - вычисление релизов за период
    datetype = "ReleaseDate"
    body = flask.request.get_json()
    if body is not None:
        try:
            if body["type"] == "youtube":
                datetype = "YouTubeReleaseDate"
        except KeyError:
            pass
    periods = ["DAY", "WEEK", "MONTH", "YEAR"]
    period = period.upper()
    if period not in periods:
        flask.abort(403)
    mycursor = ssm_connection()
    query = "SELECT * FROM releases " \
            "WHERE " + datetype + " > DATE_SUB(CURDATE(), INTERVAL %s " + period + ") ORDER BY projectID DESC;"
    val = (n,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        itemlist.append(form_proj_info_dict(row))
    return json.dumps(itemlist, indent=4)


@ssm.route("/releases_between_<date1>_and_<date2>", methods=['GET'])
def get_releases_between(date1, date2):
    """
    Собирает список релизов за определенные даты, указанные в параметрах.
    Возвращает json-объект, список релизов.
    Автоматом делает выборку по дате релиза в айтубе, если передать в теле запроса {"type":"youtube"}, то выборка будет по дате с ютуба
    :param date1: str (date format yyyy.mm.dd)
    :param date2: str (date format yyyy.mm.dd)
    :return: json(list[dict])
    """
    datetype = "ReleaseDate"
    body = flask.request.get_json()
    if body is not None:
        try:
            if body["type"] == "youtube":
                datetype = "YouTubeReleaseDate"
        except KeyError:
            pass
    mycursor = ssm_connection()
    date1 = date1.replace(".", "/")
    date2 = date2.replace(".", "/")
    query = "SELECT * FROM releases WHERE ("+datetype+" BETWEEN %s AND %s) ORDER BY projectID DESC;"
    val = (date1, date2)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        itemlist.append(form_proj_info_dict(row))
    return json.dumps(itemlist, indent=4)


@ssm.route("/kpi_<year>_<country>", methods=['GET'])
def get_kpi_by_country_year(year, country):
    """
    Собирает данные по KPI с значениемя за указанный год и страну.
    Возвращает json-объект, список словарей.
    :param year: int
    :param country: str
    :return: json(list[dict])
    """
    country = country.upper()
    mycursor = ssm_connection()
    query = "SELECT idkpi, value, target, month FROM kpi_mao WHERE year = %s AND country = %s;"
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
    return json.dumps(itemlist, indent=4)


@ssm.route("/updatekpimau", methods=['POST'])
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
            query = "UPDATE kpi_mao SET value = %s WHERE country = %s AND month = %s AND year = %s AND month_year = %s;"
            mycursor.execute(query, val)
        mydb.commit()
        return_dict = dict()
        return_dict["message"] = "MAU KPI of" + country + " updated."
        return return_dict


@ssm.route("/update_yt_trends", methods=['POST'])
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
                query = "UPDATE youtube_trends SET place = %s WHERE video_name = %s AND DATE(date) = CURDATE();"
                values = (video['place'], video['video_name'])
            else:
                query = "INSERT INTO youtube_trends (video_name, channel, views, place, date) VALUES (%s, %s, %s, %s, NOW());"
                values = (video['video_name'], video['channel'], video['views'], video['place'])
            mycursor.execute(query, values)
    mydb.commit()
    return_dict = dict()
    return_dict["message"] = str(count)+" videos added."
    return return_dict


@ssm.route("/get_yt_trends", methods=['GET'])
def get_yt_trends():
    """
    Получает ютуб тренды за текущую дату.
    Возвращает их в виде json-объекта, списка словарей.
    :return: json
    """
    mycursor = ssm_connection()
    query = "SELECT id, video_name, channel, views, place, youtube_id, youtube_retention, youtube_CTR, youtube_male, youtube_female, youtube_shows, youtube_AVBU, youtube_country_retention FROM youtube_trends WHERE DATE(date) = CURDATE() ORDER BY date DESC;"
    mycursor.execute(query)
    if mycursor.rowcount == 0:  # Костыль. Фиксит ошибку которая не показывает никакие видосы ночью нового дня.
        query = "SELECT id, video_name, channel, views, place, youtube_id, youtube_retention, youtube_CTR, youtube_male, youtube_female, youtube_shows, youtube_AVBU, youtube_country_retention FROM youtube_trends WHERE DATE(date) = DATE_SUB(CURDATE(), INTERVAL 1 DAY) ORDER BY date DESC;"
        mycursor.execute(query)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        item = dict()
        item["video_name"] = row[1]
        item["channel"] = row[2]
        item["views"] = row[3]
        item["place"] = row[4]
        item["youtube_id"] = row[5]
        item["youtube_retention"] = row[6]
        item["youtube_CTR"] = row[7]
        item["youtube_male"] = row[8]
        item["youtube_female"] = row[9]
        item["youtube_shows"] = row[10]
        item["youtube_AVBU"] = row[11]
        item["youtube_country_retention"] = row[12]
        itemlist.append(item)
    return json.dumps(itemlist, indent=4)


@ssm.route("/add_yt_channel", methods=['POST'])
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
        return_dict = dict()
        return_dict["message"] = "Channel "+channel+" added."
        return return_dict


@ssm.route("/get_kpi_aitu", methods=['GET'])
def get_kpi_aitu():
    """
    Получает значения KPI Aitu.
    Возвращает json-объект, список словарей.
    :return: json(list[dict])
    """
    mycursor = ssm_connection()
    sql = "SELECT target, `left`, top_50, top_100, quiz, releases, today, `quarter`, quarter_left FROM kpi_aitu;"   # Кавычки в запросе не убирать, иначе сломает запрос.
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
    return json.dumps(itemlist, indent=4)


@ssm.route("/update_kpi_aitu", methods=['POST'])
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
    query = 'UPDATE kpi_aitu SET target = %s, `left` = %s, top_50 = %s, top_100 = %s, quiz = %s, releases = %s, today = %s, `quarter` = %s, quarter_left = %s WHERE id = 1'
    # Кавычки в запросе не убирать, иначе сломает запрос.
    val = (int(items[0]), int(items[1]), int(items[2]), int(items[3]), int(items[4]), int(items[5]), int(items[6]), int(items[7]), int(items[8]))
    mycursor.execute(query, val)
    mydb.commit()
    return_dict = dict()
    return_dict["message"] = "AITU KPI updated."
    return return_dict


@ssm.route("/update_logs", methods=['POST'])
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
    try:
        query = "UPDATE log SET date = NOW() WHERE name = %s"
        value = (data["type"],)
        mycursor = ssm_connection()
        global mydb
        mycursor.execute(query, value)
        mydb.commit()
        return_dict = dict()
        return_dict["message"] = data["type"] + " log updated."
        return return_dict
    except KeyError:
        flask.abort(403)


@ssm.route("/get_logs", methods=['GET'])
def get_logs():
    """
    Получает данные из таблицы логов.
    Возвращает json-объект, словарь.
    :return: json(list[dict])
    """
    mycursor = ssm_connection()
    try:
        query = "SELECT * FROM log ORDER BY `date` ASC;"
        mycursor.execute(query)
        query_result = mycursor.fetchall()
        itemlist = []
        for row in query_result:
            item = dict()
            item["id"] = row[0]
            item["full_name"] = row[3]
            item["name"] = row[2]
            item["date"] = str(row[1])
            itemlist.append(item)
        return json.dumps(itemlist, indent=4)
    except KeyError:
        flask.abort(403)


@ssm.route("/get_dashb_params", methods=['GET'])
def get_dashboard_params():
    """
    Получает параметры для дэшборда. Параметры заданы в коде.
    Возвращает json-объект, список словарей.
    :return: json(list[dict])
    """
    itemlist = []
    keys = ["color", "position", "cpv_youtube_m", "cpv_youtube_f", "cpv_youtube_mid", "cpu_aitube_m", "cpu_aitube_f", "cpu_aitube_mid", "youtube_ud", "cpv_aitube_m", "cpv_aitube_f", "cpv_aitube_mid"]
    items_1 = ["red", "bad", 12.5, 23.25, 17.88, 213.2, 396, 304.6, 20, 62.5, 116.06, 89.28]
    items_2 = ["yellow", "middle", 5, 9.3, 7.15, 85.3, 158.4, 121.85, 40, 25, 46.42, 35.71]
    items_3 = ["green", "great", 1, 1.86, 1.43, 17.1, 31.6, 24.35, 60, 5, 9.28, 7.14]
    item_list = [items_1, items_2, items_3]
    for j in range(0, 3):
        item = dict()
        for i in range(0, 9):
            item[keys[i]] = item_list[j][i]
        itemlist.append(item)
    return json.dumps(itemlist, indent=4)


@ssm.route("/get_milestone_releases", methods=['GET'])
def get_milestone_releases():
    """
    Выводит все релизы (актуальные), которые приближаются к milestone
    Возвращает json-объект, список словарей
    :return: json(list[dict])
    """
    mycursor = ssm_connection()
    milestone_projects = "(1, 2, 3, 4, 5, 7, 9, 10, 15, 76, 115, 117, 119, 120, 121, 122, 123)"
    query = "SELECT * FROM releases WHERE ProjectID IN "+milestone_projects+" AND (YouTubeViews BETWEEN 950000 AND 1000000) OR (YouTubeViews BETWEEN 1950000 AND 2000000) OR (YouTubeViews BETWEEN 2950000 AND 3000000) OR (YouTubeViews BETWEEN 3950000 AND 4000000) OR (YouTubeViews BETWEEN 4950000 AND 5000000) ORDER BY ReleaseDate ASC;"
    mycursor.execute(query)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        itemlist.append(form_proj_info_dict(row))
    return json.dumps(itemlist, indent=4)


@ssm.route("/add_release", methods=['POST'])
def add_release():
    """
    Добавляет релиз в базу данных, данные принимаются в теле запроса
    В базу данные записываются в поля, которые соответствуют переданным ключам в запросе.
    :return: str
    """
    body = flask.request.get_json()
    if body is not None:
        global mydb
        mycursor = ssm_connection()
        keys = []
        values = []
        for item in body.keys():  # формирование массивов из ключей словаря и соответствующих значений
            if item == "auth":
                pass
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
        return_dict = dict()
        return_dict["message"] = "Release added."
        return return_dict
    else:
        flask.abort(400)


@ssm.route("/get_file_<filename>.<ext>", methods=['GET'])
def get_custom_json_data(filename, ext):
    """
    Выводит данные из файла, который хранится в директории data
    Название файла, с которого нужно считать, передается в запросе как параметр.
    Возвращает json-объект, содержимое файла
    :return: json(list[dict])
    """
    with open("ssm-backend/data/"+filename+'.'+ext) as f:
        data = f.read()
    if data is not None:
        if ext == 'html':
            item = dict()
            item['html'] = data
            return json.dumps(item, indent=4)
        elif ext == 'json':
            return json.dumps(json.loads(data), indent=4)
        else:
            flask.abort(400)
    else:
        flask.abort(404)


@ssm.route("/get_pr_status", methods=['GET'])
def get_pr_status():
    """
    Выводит данные из таблички статуса задач пр отдела (pr_status).
    Возвращает json-объект, список словарей
    :return: json(list[dict])
    """
    mycursor = ssm_connection()
    query = "SELECT * FROM pr_status;"
    mycursor.execute(query)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        temp = dict()
        temp["id"] = row[0]
        temp["task"] = row[1]
        temp["accountable"] = row[2]
        temp["project"] = row[3]
        temp["project_manager"] = row[4]
        temp["date_create"] = str(row[5])
        temp["deadline"] = str(row[6])
        temp["status"] = row[7]
        temp["channel"] = row[8]
        itemlist.append(temp)
    return json.dumps(itemlist, indent=4)


@ssm.route("/get_pr_mentions", methods=['POST'])
def get_pr_mentions():
    """
    Выводит данные из таблички упоминаний Salen (pr_mentions).
    В теле запроса можно передать параметр year, тогда выборка будет только за определенный год. Например, {"year": 2021}
    Возвращает json-объект, список словарей
    :return: json(list[dict])
    """
    body = flask.request.get_json()
    mycursor = ssm_connection()
    if body is not None:
        year = body["year"]
        query = "SELECT * FROM pr_mentions WHERE YEAR(release_date) = %s;"
        val = (year, )
        mycursor.execute(query, val)
    else:
        query = "SELECT * FROM pr_mentions;"
        mycursor.execute(query)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        temp = dict()
        temp["id"] = row[0]
        temp["link"] = row[1]
        temp["project_name"] = row[2]
        temp["source"] = row[3]
        temp["key_msg"] = row[4]
        temp["tone"] = row[5]
        temp["release_date"] = str(row[6])
        temp["author"] = row[7]
        itemlist.append(temp)
    return json.dumps(itemlist, indent=4)


@ssm.route("/meet", methods=['POST'])
def post_meeting():
    """
    Записывает данные из тела запроса в табличку (meet_schedule)
    Возвращает json, словарь.
    :return: json(list[dict])
    """
    body = flask.request.get_json()
    if body is None:
        flask.abort(400)
    global mydb
    add_event = True
    mycursor = ssm_connection()
    # Проверка, не попадает ли новая запись в промежутки предыдущих записей
    query = "SELECT time, time_finish FROM meet_schedule WHERE room = %s AND mdate = %s;"
    values = (body['room'], body['date'])
    mycursor.execute(query, values)
    query_results = mycursor.fetchall()
    for row in query_results:
        sql_time_start = time.strptime(row[0], "%H:%M")
        sql_time_finish = time.strptime(row[1], "%H:%M")
        input_start_time = time.strptime(body["time"], "%H:%M")
        output_start_time = time.strptime(body["finish"], "%H:%M")
        if time_in_range(sql_time_start, sql_time_finish, input_start_time) \
                or time_in_range(sql_time_start, sql_time_finish, output_start_time):
            add_event = False
            break
    if add_event:
        # Запись нового meet event.
        query = "INSERT INTO meet_schedule (author, time, room, time_finish, mdate) VALUES (%s, %s, %s, %s, %s)"
        try:
            values = (body["author"], body["time"], body["room"], body["finish"], body["date"])
        except KeyError:
            flask.abort(400)
        else:
            mycursor.execute(query, values)
            mydb.commit()
            return_dict = dict()
            return_dict["message"] = "Meet event added."
            return return_dict
    else:
        flask.abort(403)


@ssm.route("/meet/<mdate>", methods=['GET'])
def get_meeting_date(mdate):
    """
    Возвращает список встреч из таблицы meet_schedule (за определенную дату)
    :param mdate: date (format dd.mm.yyyy)
    :return: json(list[dict])
    """
    mycursor = ssm_connection()
    query = "SELECT * FROM meet_schedule WHERE mdate = %s ORDER BY `time`;"
    value = (mdate,)
    mycursor.execute(query, value)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        item = dict()
        item["id"] = row[0]
        item["author"] = row[1]
        item["date"] = row[5]
        item["time"] = row[2]
        item["finish"] = row[4]
        item["room"] = row[3]
        itemlist.append(item)
    return json.dumps(itemlist, indent=4)


@ssm.route("/meet", methods=['DELETE'])
def delete_meeting():
    """
    Удаляет данные из таблицы расписания брони переговорок (meet_schedule)
    Возвращает строку с
    :return: str
    """
    global mydb
    body = flask.request.get_json()
    if body is None:
        flask.abort(400)
    try:
        idmeet = body["id"]
    except KeyError:
        flask.abort(400)
    else:
        mycursor = ssm_connection()
        value = (idmeet,)
        query = "SELECT * FROM meet_schedule WHERE idmeet = %s"
        mycursor.execute(query, value)
        return_dict = dict()
        mycursor.fetchall()
        if mycursor.rowcount == 0:
            return_dict["message"] = "Meet event with id = " + str(idmeet) + " was not found."
            return return_dict
        query = "DELETE FROM meet_schedule WHERE idmeet = %s;"
        mycursor.execute(query, value)
        mydb.commit()
        return_dict["message"] = "Meet event with id = " + str(idmeet) + " deleted."
        return return_dict


@ssm.route("/project_stats=<projectid>", methods=['GET'])
def get_project_stats(projectid):
    """
    todo: comms
    :return:
    """
    mycursor = ssm_connection()
    query_list = [
        {"name": "yt_sum_views", "query": "SELECT sum(YoutubeViews) FROM releases WHERE ProjectID = %s;"},
        {"name": "yt_views_first_release", "query": "SELECT YouTubeViews FROM releases WHERE ProjectID = %s ORDER BY YoutubeReleaseDate LIMIT 1;"},
        {"name": "yt_avg_views", "query": "SELECT AVG(YouTubeViews) FROM releases WHERE ProjectID = %s;"},
        {"name": "yt_sum_comments", "query": "SELECT SUM(YouTubeCommentsCount) FROM releases WHERE ProjectID = %s;"},
        {"name": "at_sum_views", "query": "SELECT SUM(AitubeViews) FROM releases WHERE ProjectID = %s;"},
        {"name": "at_sum_uniqs_year", "query": "SELECT SUM(UniqUserPerYear) FROM releases WHERE ProjectID = %s;"},
        {"name": "at_sum_traffic", "query": "SELECT SUM(Traffic) FROM releases WHERE ProjectID = %s;"},
        {"name": "avg_uniqs_per_month", "query": "SELECT avg(avg) FROM (select AVG(UniqUsersReleaseMonth) AS avg FROM releases WHERE ProjectID = %s GROUP BY MONTH(ReleaseDate)) AS t;"}
        ]
    itemlist = []
    result_dict = dict()
    itemlist.append(result_dict)
    value = (projectid, )
    for query in query_list:
        mycursor.execute(query['query'], value)
        query_result = mycursor.fetchall()
        for row in query_result:
            try:
                result_dict[query["name"]] = float(row[0])
            except TypeError:
                result_dict[query["name"]] = 0

    query = "SELECT AVG(Male), AVG(Female) FROM releases WHERE projectid = %s AND season = %s"  # Получаем средний пол проекта
    mycursor.execute(query, value)
    gender = "M-F"
    for row in mycursor.fetchall():
        if row[0] is not None and row[0] != '0%':
            if float(row[0]) > 60.0:
                gender = "M"
            elif float(row[1]) > 60.0:
                gender = "F"
    result_dict["gender"] = gender
    return json.dumps(itemlist, indent=4)


@ssm.route("/project_stats=<projectid>&season=<int:season>")
def project_stats_season_handler(projectid, season):
    """
    todo: comms
    :param projectid:
    :param season:
    :return:
    """
    if season is not None:
        if season < 0:
            flask.abort(403)
        elif season == 0:
            return get_project_stats(projectid)
        else:
            return get_project_stats_season(projectid, season)
    else:
        flask.abort(403)


def get_project_stats_season(projectid, season):
    """
    todo: comms
    :param projectid:
    :param season:
    :return:
    """
    mycursor = ssm_connection()
    query_list = [
        {"name": "yt_sum_views", "query": "SELECT sum(YoutubeViews) FROM releases WHERE ProjectID = %s AND Season = %s;"},
        {"name": "yt_views_first_release", "query": "SELECT YouTubeViews FROM releases WHERE ProjectID = %s AND Season = %s ORDER BY YoutubeReleaseDate LIMIT 1;"},
        {"name": "yt_avg_views", "query": "SELECT AVG(YouTubeViews) FROM releases WHERE ProjectID = %s AND Season = %s;"},
        {"name": "yt_sum_comments", "query": "SELECT SUM(YouTubeCommentsCount) FROM releases WHERE ProjectID = %s AND Season = %s;"},
        {"name": "at_sum_views", "query": "SELECT SUM(AitubeViews) FROM releases WHERE ProjectID = %s AND Season = %s;"},
        {"name": "at_sum_uniqs_year", "query": "SELECT SUM(UniqUserPerYear) FROM releases WHERE ProjectID = %s AND Season = %s;"},
        {"name": "at_sum_traffic", "query": "SELECT SUM(Traffic) FROM releases WHERE ProjectID = %s AND Season = %s;"},
        {"name": "avg_uniqs_per_month", "query": "SELECT avg(avg) FROM (select AVG(UniqUsersReleaseMonth) AS avg FROM releases WHERE ProjectID = %s AND Season = %s GROUP BY MONTH(ReleaseDate)) AS t;"}
        ]
    itemlist = []
    result_dict = dict()
    itemlist.append(result_dict)
    value = (projectid, season)
    for query in query_list:
        mycursor.execute(query['query'], value)
        query_result = mycursor.fetchall()
        for row in query_result:
            try:
                result_dict[query["name"]] = float(row[0])
            except TypeError:
                result_dict[query["name"]] = 0

    query = "SELECT AVG(Male), AVG(Female) FROM releases WHERE projectid = %s AND season = %s"  # Получаем средний пол проекта
    mycursor.execute(query, value)
    gender = "M-F"
    for row in mycursor.fetchall():
        if row[0] is not None and row[0] != '0%':
            if float(row[0]) > 60.0:
                gender = "M"
            elif float(row[1]) > 60.0:
                gender = "F"
    result_dict["gender"] = gender
    return json.dumps(itemlist, indent=4)


@ssm.route("/shop", methods=['POST'])
def post_shop():
    body = flask.request.get_json()
    if body is None:
        flask.abort(400)
    try:
        post_type = body['post_type']
    except KeyError:
        flask.abort(400)
    else:
        if post_type == 'доставка':
            query = "INSERT INTO shop " \
                    "(id, post_type, name, phone, email, country, city, adress, full_price, rules_ok, basket, order_date) " \
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"
            values = (body['order_number'], body['post_type'], body['name'], body['phone'], body['email'], body['contry'], body['city'], body['adress'], body['full_price'], body['rules_ok'], str(body['basket']))

        elif post_type == 'самовывоз':
            query = "INSERT INTO shop " \
                    "(id, post_type, name, phone, email, full_price, rules_ok, basket, order_date) " \
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())"
            values = (body['order_number'], body['post_type'], body['name'], body['phone'], body['email'], body['full_price'], body['rules_ok'], str(body['basket']))

        else:
            flask.abort(400)

        global mydb
        mycursor = ssm_connection()
        try:
            mycursor.execute(query, values)
        except mysql.connector.errors.IntegrityError:
            return_message = "Order with id = "+str(body['order_number'])+" already present in database."
            return flask.Response("{'error':"+return_message+"}", status=400, mimetype='application/json')
        mydb.commit()
        subprocess.call(['python3.8', 'ssm-backend/update_shop_gsheet.py'])
        if body['full_price'] == 0:
            response = {"url": "https://salemsocial.kz/good_status_ok"}
            return response
        return kassa24_send_query(body)


@ssm.route("/shop/<stype>", methods=['GET'])
def get_shop(stype):
    mycursor = ssm_connection()
    if stype == 'all':
        query = "SELECT * FROM shop;"
    elif stype == 'paid':
        query = "SELECT * FROM shop WHERE payment_status = 1;"
    elif stype == 'unpaid':
        query = "SELECT * FROM shop WHERE payment_status = 0;"
    else:
        flask.abort(400)
    mycursor.execute(query)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        item = dict()
        item['order_number'] = row[0]
        item['post_type'] = row[1]
        item['name'] = row[2]
        item['phone'] = row[3]
        item['email'] = row[4]
        item['full_price'] = row[5]
        item['rules_ok'] = row[6]
        item['basket'] = row[7]
        item['payment_status'] = row[11]
        if row[1] == 'доставка':
            item['country'] = row[8]
            item['city'] = row[9]
            item['adress'] = row[10]
        itemlist.append(item)
    return json.dumps(itemlist, indent=4)


@ssm.route("/shop/count", methods=['GET'])
def get_orders_shop_count():
    mycursor = ssm_connection()
    query = "SELECT COUNT(*) FROM shop;"
    mycursor.execute(query)
    query_result = mycursor.fetchall()
    itemlist = [dict()]
    for row in query_result:
        itemlist[0]["orders_count"] = row[0]
    return json.dumps(itemlist)


@ssm.route("/kassa24_callback", methods=['POST'])
def kassa24_handle_callback():
    body = flask.request.get_json()
    ip_address = flask.request.headers['X-Real-IP']
    kassa_ip = '35.157.105.64'
    if ip_address != kassa_ip:
        flask.abort(403)
    if body['status'] == 1:
        mycursor = ssm_connection()
        global mydb
        query = "UPDATE shop SET payment_status = %s WHERE id = %s;"
        value = (1, str(body['metadata']['order_id']))
        mycursor.execute(query, value)
        mydb.commit()
    else:
        response = "Payment with order id = "+str(body['metadata']['order_id'])+" was not completed."
        return response
    response = "Payment with order id = "+str(body['metadata']['order_id'])+" has been completed."
    subprocess.call(['python3.8', 'ssm-backend/update_shop_gsheet.py'])
    return response


def kassa24_send_query(inp):
    return_url = 'https://salemsocial.kz/'
    callback_url = 'https://maksimsalnikov.pythonanywhere.com/ssm/kassa24_callback'
    kassa_request_url = "https://ecommerce.pult24.kz/payment/create"
    description_str = "Заказ #"+str(inp['order_number'])+"\nСодержание:\n"
    for item in inp['basket']:
        description_str += str(item['name'])+"\n"
    payload = {
        "orderId": str(inp['order_number']),
        "merchantId": fkassa_login,
        "amount": inp['full_price']*100,
        "returnUrl": return_url,
        "callbackUrl": callback_url,
        'description': description_str,
        'metadata': {'order_id': inp['order_number']},
        'customerData': {'email': inp['email'], 'phone': inp['phone']}
    }
    headers = {
        "Authorization": "Basic "+base64.b64encode((fkassa_login+':'+fkassa_password).encode('ascii')).decode('ascii'),
        "Content-Type": "application/json",
        "Content-Length": str(len(payload))
    }
    r = requests.post(url=kassa_request_url, headers=headers, json=payload)
    return_redirect_url = r.json()['url']
    response = {"url": return_redirect_url}
    if r.status_code == 201:
        return response
    else:
        return flask.abort(r.status_code)


@ssm.route("/month_traffic", methods=['GET'])
def get_month_traffic():
    mycursor = ssm_connection()
    query = "SELECT * FROM main_month_traffic WHERE All_Traffic IS NOT NULL;"
    mycursor.execute(query)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        item = dict()
        item['id'] = row[0]
        item['year'] = row[1]
        item['month'] = row[2]
        item['all_traffic'] = row[3]
        item['position_1_name'] = row[4]
        item['position_1_traffic'] = row[5]
        item['position_2_name'] = row[6]
        item['position_2_traffic'] = row[7]
        item['position_3_name'] = row[8]
        item['position_3_traffic'] = row[9]
        itemlist.append(item)
    return json.dumps(itemlist, indent=4)
