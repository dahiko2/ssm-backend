from flask import Blueprint
import flask
import mysql.connector
import json

mydb = mysql.connector.connect()
fhost = ""
fuser = ""
fpass = ""
fdbname_insta = ""


def read_creds():
    """
    Построчно считывает данные для подключения к бд из файла credentials.txt.
    """
    global fhost, fuser, fpass, fdbname_insta
    with open("credentials.txt") as f:
        fhost = f.readline().strip()
        fuser = f.readline().strip()
        fpass = f.readline().strip()
        fdbname_insta = f.readline().strip()


read_creds()  # Считывает данные для входа при запуске скрипта
insta_bp = Blueprint('instagram', __name__)


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


@insta_bp.route("/<account>", methods=['GET'])
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


@insta_bp.route("/<account>/posts", methods=['GET'])
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


@insta_bp.route("/update_post", methods=['POST'])
def update_insta_post():
    body = flask.request.get_json()
    if body is not None:
        global mydb
        mycursor = instagram_connection()
        if body["is_video"] is False:
            isvideo = 0
        else:
            isvideo = 1
        if isvideo == 0:
            query = """INSERT INTO posts (shortlink, isvideo, comments, likes, uploaddate, profileID)
                     SELECT %s, %s, %s, %s, %s, idprofile
                     from profile WHERE profilename = %s
                     LIMIT 1;"""
            val = (body["shortcode"], isvideo, int(body["comments"]),
                   int(body["likes"]), body["upload_date"], body["username"])
            try:
                mycursor.execute(query, val)
            except mysql.connector.errors.IntegrityError:
                query = """UPDATE posts SET comments = %s, likes = %s where shortlink = %s;"""
                val = (int(body["comments"]), int(body["likes"]),
                       body["shortcode"])
                mycursor.execute(query, val)
        else:
            query = """INSERT INTO posts (shortlink, isvideo, comments, likes, uploaddate, video_views, profileID)
             SELECT %s, %s, %s, %s, %s, %s, idprofile
             from profile WHERE profilename = %s
             LIMIT 1;"""
            val = (body["shortcode"], isvideo, int(body["comments"]),
                   int(body["likes"]), body["upload_date"],
                   body["video_view_count"], body["username"])
            try:
                mycursor.execute(query, val)
            except mysql.connector.errors.IntegrityError:
                query = """UPDATE posts SET comments = %s, likes = %s, video_views = %s where shortlink = %s;"""
                val = (int(body["comments"]), int(body["likes"]),
                       body["video_view_count"], body["shortcode"])
                mycursor.execute(query, val)

        mydb.commit()
    else:
        flask.abort(400)


@insta_bp.route("/update_profile", methods=['POST'])
def update_insta_profile():
    body = flask.request.get_json()
    if body is not None:
        global mydb
        mycursor = instagram_connection()
        query = "INSERT INTO profile (profilename, posts, followers) VALUES (%s, %s, %s)"
        values = (body["username"], body["posts"],
                  body["followers"])
        try:
            mycursor.execute(query, values)
        except mysql.connector.errors.IntegrityError:
            query = "UPDATE profile SET posts = %s, followers = %s where profilename = %s;"
            values = (body["posts"], body["followers"],
                      body["username"])
            mycursor.execute(query, values)
        mydb.commit()
    else:
        flask.abort(400)


@insta_bp.route("/<account>/posts/top_likes<int:n>", methods=['GET'])
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


@insta_bp.route("/<account>/posts/top_comments<int:n>", methods=['GET'])
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


@insta_bp.route("/<account>/posts/top_videos<int:n>", methods=['GET'])
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


@insta_bp.route("/top_followers", methods=['GET'])
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


@insta_bp.route("/top_video<int:n>", methods=['GET'])
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


@insta_bp.route("/top_likes<int:n>", methods=['GET'])
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


@insta_bp.route("/top_comments<int:n>", methods=['GET'])
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
