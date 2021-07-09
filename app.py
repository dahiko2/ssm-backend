import json
import mysql.connector
import flask
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from flask_cors import CORS


class MyDict(dict):
    def __init__(self):
        self = dict()

    def add(self, key, value):
        self[key] = value


AUTH = False
mydb = mysql.connector.connect()
fhost = ""
fuser = ""
fpass = ""
fauth = ""
fdbname_insta = ""
fdbname_ssm = ""

app = flask.Flask(__name__)
CORS(app)
cors = CORS(app, resources={
    r"/*": {
        "origins": "https://salemsocial.kz/"
    }
})


def read_creds():
    global fhost, fuser, fpass, fauth, fdbname_insta, fdbname_ssm
    with open("credentials.txt") as f:
        fhost = f.readline()
        fuser = f.readline()
        fpass = f.readline().strip()
        fauth = f.readline()
        fdbname_insta = f.readline().strip()
        fdbname_ssm = f.readline().strip()


def instagram_connection():
    global mydb
    mydb = mysql.connector.connect(
        host=fhost,
        user=fuser,
        password=fpass,
        database=fdbname_insta
    )
    return mydb.cursor()


def ssm_connection():
    global mydb
    mydb = mysql.connector.connect(
        host=fhost,
        user=fuser,
        password=fpass,
        database=fdbname_ssm
    )
    return mydb.cursor()


def calculate_cpv(price, youtube_view):
    if price is None or youtube_view is None or youtube_view == 0:
        return 0
    return price / youtube_view


def calculate_cpc(price, traffic):
    if price is None or traffic is None or traffic == 0:
        return 0
    return price/traffic


def calculate_cpu(price, uniq_user):
    if price is None or uniq_user is None or uniq_user == 0:
        return 0
    return price/uniq_user


def get_yt_id(url):
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
    youtube_channels = []
    query = "SELECT name FROM channels;"
    mycursor.execute(query)
    query_result = mycursor.fetchall()
    for row in query_result:
        youtube_channels.append(row[0])
    return youtube_channels


def get_today_trends_videos(mycursor):
    today_videos = []
    query = "SELECT video_name FROM youtube_trends WHERE DATE(date) = CURDATE();"
    mycursor.execute(query)
    query_result = mycursor.fetchall()
    for row in query_result:
        today_videos.append(row[0])
    return today_videos


@app.route("/instagram/<account>")
def get_instagram_profile(account):
    mycursor = instagram_connection()
    query = "SELECT * FROM profile WHERE profilename = %s;"
    val = (str(account),)
    mycursor.execute(query, val)
    query_result_info = mycursor.fetchall()
    if query_result_info is None:
        return "{}"

    query = "SELECT SUM(likes) from posts where profileID in (select idprofile from profile where profilename = %s);"
    val = (str(account),)
    mycursor.execute(query, val)
    query_result_likes = mycursor.fetchone()

    query = "SELECT SUM(comments) from posts where profileID in (select idprofile from profile where profilename = %s);"
    val = (str(account),)
    mycursor.execute(query, val)
    query_result_comments = mycursor.fetchone()

    mydict = MyDict()
    for row in query_result_info:
        # engagement = ((float(query_result_likes[0]) + float(query_result_comments[0])) / float(row[2])) / float(row[3]) * 100
        # print(engagement)
        mydict.add(row[0], ({"name": row[1], "posts": row[2], "followers": row[3], "likes": str(query_result_likes[0]),
                             "comments": str(query_result_comments[0])}))
    result = json.dumps(mydict, indent=4)
    return result


@app.route("/instagram/<account>/posts")
def get_instagram_posts(account):
    mycursor = instagram_connection()
    query = "select * from posts where profileID in (select idprofile from profile where profilename = %s);"
    val = (str(account),)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    mydict = MyDict()
    for row in query_result:
        mydict.add(row[0], (
            {"shortlink": row[1], "isvideo": row[2], "comments": row[3], "likes": row[4], "uploaddate": str(row[5]),
             "video_views": row[6]}))
    result = json.dumps(mydict, indent=4)
    return result


@app.route("/instagram/<account>/posts/top_likes<int:n>")
def get_instagram_posts_top_by_likes(account, n):
    mycursor = instagram_connection()
    query = "select * from posts where profileID in (select idprofile from profile where profilename = %s) ORDER BY likes DESC LIMIT %s;"
    val = (str(account), n)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    mydict = MyDict()
    for row in query_result:
        mydict.add(row[0], (
            {"shortlink": row[1], "isvideo": row[2], "comments": row[3], "likes": row[4], "uploaddate": str(row[5]),
             "video_views": row[6]}))
    result = json.dumps(mydict, indent=4)
    return result


@app.route("/instagram/<account>/posts/top_comments<int:n>")
def get_instagram_posts_top_by_comments(account, n):
    mycursor = instagram_connection()
    query = "select * from posts where profileID in (select idprofile from profile where profilename = %s) ORDER BY comments DESC LIMIT %s;"
    val = (str(account), n)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    mydict = MyDict()
    for row in query_result:
        mydict.add(row[0], (
            {"shortlink": row[1], "isvideo": row[2], "comments": row[3], "likes": row[4], "uploaddate": str(row[5]),
             "video_views": row[6]}))
    result = json.dumps(mydict, indent=4)
    return result


@app.route("/instagram/<account>/posts/top_videos<int:n>")
def get_instagram_posts_top_by_videos(account, n):
    mycursor = instagram_connection()
    query = "select * from posts where profileID in (select idprofile from profile where profilename = %s) ORDER BY video_views DESC LIMIT %s;"
    val = (str(account), n)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    mydict = MyDict()
    for row in query_result:
        mydict.add(row[0], (
            {"shortlink": row[1], "comments": row[3], "likes": row[4], "uploaddate": str(row[5]),
             "video_views": row[6]}))
    result = json.dumps(mydict, indent=4)
    return result


@app.route("/instagram/top_followers")
def get_top_by_followers():
    mycursor = instagram_connection()
    query = "SELECT idprofile, profilename, followers FROM profile ORDER BY followers DESC"
    mycursor.execute(query)
    query_result = mycursor.fetchall()
    mydict = MyDict()
    for row in query_result:
        mydict.add(row[0], ({"profilename": row[1], "followers": row[2]}))
    result = json.dumps(mydict, indent=4)
    return result


@app.route("/instagram/top_video<int:n>")
def get_top_by_video_views(n):
    mycursor = instagram_connection()
    query = "select idposts, shortlink, comments, likes, video_views,  profilename from posts, profile where profileID = idprofile ORDER BY video_views DESC LIMIT %s;"
    val = (n,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    mydict = MyDict()
    for row in query_result:
        mydict.add(row[0], (
            {"shortlink": row[1], "comments": row[2], "likes": row[3], "video_views": row[4], "profilename": row[5]}))
    result = json.dumps(mydict, indent=4)
    return result


@app.route("/instagram/top_likes<int:n>")
def get_top_by_likes(n):
    mycursor = instagram_connection()
    query = "select idposts, shortlink, comments, likes, video_views,  profilename from posts, profile where profileID = idprofile ORDER BY likes DESC LIMIT %s;"
    val = (n,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    mydict = MyDict()
    for row in query_result:
        mydict.add(row[0], (
            {"shortlink": row[1], "comments": row[2], "likes": row[3], "video_views": row[4], "profilename": row[5]}))
    result = json.dumps(mydict, indent=4)
    return result


@app.route("/instagram/top_comments<int:n>")
def get_top_by_comments(n):
    mycursor = instagram_connection()
    query = "select idposts, shortlink, comments, likes, video_views,  profilename from posts, profile where profileID = idprofile ORDER BY comments DESC LIMIT %s;"
    val = (n,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    mydict = MyDict()
    for row in query_result:
        mydict.add(row[0], (
            {"shortlink": row[1], "comments": row[2], "likes": row[3], "video_views": row[4], "profilename": row[5]}))
    result = json.dumps(mydict, indent=4)
    return result


@app.route("/ssm/info_byprojectid=<int:project_id>")
def get_fullinfo_by_projectid(project_id):
    mycursor = ssm_connection()
    query = "SELECT * FROM ssm.releases where projectID = %s"
    val = (project_id,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        item = dict()
        item["episode_name"] = row[1]
        item["unique_count"] = row[2]
        item["traffic"] = row[3]
        item["release_date"] = str(row[4])
        item["tail"] = row[5]
        item["traffic_per_day"] = row[6]
        item["traffic_per_tail"] = row[7]
        item["youtube_views"] = row[8]
        item["avg_view_by_user"] = row[9]
        item["shows"] = row[10]
        item["ctr"] = row[11]
        item["uniq_users_youtube"] = row[12]
        item["subscribers"] = row[13]
        item["price"] = row[15]
        item["cpv"] = calculate_cpv(row[15], row[8])
        item["cpu"] = calculate_cpu(row[15], row[2])
        item["cpc"] = calculate_cpc(row[15], row[3])
        itemlist.append(item)
    result = json.dumps(itemlist, indent=4)
    return result
        

@app.route("/ssm/info_byprojectname=<project>")
def get_fullinfo_by_projectname(project):
    project = str(project) + "%"
    mycursor = ssm_connection()
    query = "SELECT * FROM releases where projectID in (select ProjectID from project where ProjectName like %s);"
    val = (project,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        item = dict()
        item["episode_name"] = row[1]
        item["unique_count"] = row[2]
        item["traffic"] = row[3]
        item["release_date"] = str(row[4])
        item["tail"] = row[5]
        item["traffic_per_day"] = row[6]
        item["traffic_per_tail"] = row[7]
        item["youtube_views"] = row[8]
        item["avg_view_by_user"] = row[9]
        item["shows"] = row[10]
        item["ctr"] = row[11]
        item["uniq_users_youtube"] = row[12]
        item["subscribers"] = row[13]
        item["price"] = row[15]
        item["cpv"] = calculate_cpv(row[15], row[8])
        item["cpu"] = calculate_cpu(row[15], row[2])
        item["cpc"] = calculate_cpc(row[15], row[3])
        itemlist.append(item)
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/ssm/releases_for_<int:n>_<period>")
def get_releases_by_period(period, n):
    # CURDATE - INTERVAL N PERIOD
    periods = ["DAY", "WEEK", "MONTH", "YEAR"]
    period = period.upper()
    if period not in periods:
        return "{\"Error\": Wrong period, acceptable values is DAY, WEEK, MONTH, YEAR}"
    mycursor = ssm_connection()
    query = "SELECT releaseID, ProjectName, EpisodesName, UniqUser, Traffic, ReleaseDate, Tail, TrafficPerDay, TrafficPerTail, YoutubeViews," \
            "AverageViewsByUser, Shows, CTR, UniqUserYoutube, Subscribers, Price FROM ssm.releases, ssm.project " \
            "WHERE ReleaseDate > DATE_SUB(CURDATE(), INTERVAL %s " + period + ") and releases.ProjectID = project.projectID;"
    val = (n,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        item = dict()
        item["project_name"] = row[1]
        item["episode_name"] = row[2]
        item["unique_count"] = row[3]
        item["traffic"] = row[4]
        item["release_date"] = str(row[5])
        item["tail"] = row[6]
        item["traffic_per_day"] = row[7]
        item["traffic_per_tail"] = row[8]
        item["youtube_views"] = row[9]
        item["avg_view_by_user"] = row[10]
        item["shows"] = row[11]
        item["ctr"] = row[12]
        item["uniq_users_youtube"] = row[13]
        item["subscribers"] = row[14]
        item["price"] = row[15]
        item["cpv"] = calculate_cpv(row[15], row[9])
        item["cpu"] = calculate_cpu(row[15], row[3])
        item["cpc"] = calculate_cpc(row[15], row[4])
        itemlist.append(item)
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/ssm/releases_between_<date1>_and_<date2>")
def get_releases_between(date1, date2):
    mycursor = ssm_connection()
    date1 = date1.replace(".", "/")
    date2 = date2.replace(".", "/")
    query = "SELECT releaseID, ProjectName, EpisodesName, UniqUser, Traffic, ReleaseDate, Tail, TrafficPerDay, TrafficPerTail, YoutubeViews," \
            "AverageViewsByUser, Shows, CTR, UniqUserYoutube, Subscribers, Price FROM releases, project where releases.ProjectID = project.projectID and (ReleaseDate between %s and %s);"
    val = (date1, date2)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    itemlist = []
    for row in query_result:
        item = dict()
        item["project_name"] = row[1]
        item["episode_name"] = row[2]
        item["unique_count"] = row[3]
        item["traffic"] = row[4]
        item["release_date"] = str(row[5])
        item["tail"] = row[6]
        item["traffic_per_day"] = row[7]
        item["traffic_per_tail"] = row[8]
        item["youtube_views"] = row[9]
        item["avg_view_by_user"] = row[10]
        item["shows"] = row[11]
        item["ctr"] = row[12]
        item["uniq_users_youtube"] = row[13]
        item["subscribers"] = row[14]
        item["price"] = row[15]
        item["cpv"] = calculate_cpv(row[15], row[9])
        item["cpu"] = calculate_cpu(row[15], row[3])
        item["cpc"] = calculate_cpc(row[15], row[4])
        itemlist.append(item)
    result = json.dumps(itemlist, indent=4)
    return result


@app.route("/ssm/kpi_<year>_<country>")
def get_kpi_by_country_year(year, country):
    old_response = True
    mycursor = ssm_connection()
    query = "SELECT idkpi, value, target, month FROM kpi_mao where year = %s and country = %s;"
    val = (year, country)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    if old_response:
        mydict = MyDict()
        for row in query_result:
            mydict.add(row[0], ({"value": row[1], "target": row[2], "month": row[3]}))
        result = json.dumps(mydict, indent=4)
    else:
        itemlist = []
        for row in query_result:
            item = dict()
            item["value"] = row[1]
            item["target"] = row[2]
            item["month"] = row[3]
            itemlist.append(item)
        result = json.dumps(itemlist, indent=4)
    return result


@app.route("/ssm/updatekpi_<value>_<country>")
def update_kpi_mao(value, country):
    global mydb
    mycursor = ssm_connection()
    months = [0, "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
    now = datetime.today()
    query = "INSERT INTO kpi_mao (value, country, month, year, month_year) VALUES (%s, %s, %s, %s, %s);"
    if now.month < 10:
        month = '0' + str(now.month)
    else:
        month = str(now.month)
    val = (value, country, months[now.month], now.year, month+str(now.year)+str(country))
    try:
        mycursor.execute(query, val)
    except mysql.connector.errors.IntegrityError:
        query = "UPDATE kpi_mao SET value = %s where country = %s and month = %s and year = %s and month_year = %s;"
        mycursor.execute(query, val)
    mydb.commit()
    return flask.Response(status=200)


@app.route("/ssm/updatekpi", methods=['POST'])
def update_kpi_mao2():
    body = flask.request.get_json()
    if body is None:
        flask.abort(403)
    data = json.loads(str(body).replace("'", '"'))
    try:
        value = data['value']
        country = data['country']
    except KeyError:
        flask.abort(403)

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
    except mysql.connector.errors.IntegrityError:
        query = "UPDATE kpi_mao SET value = %s where country = %s and month = %s and year = %s and month_year = %s;"
        mycursor.execute(query, val)
    mydb.commit()
    return flask.Response(status=200)


@app.route("/ssm/update_yt_trends", methods=['POST'])
def update_yt_trends():
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


@app.route("/ssm/get_yt_trends")
def get_yt_trends():
    mycursor = ssm_connection()
    query = "SELECT id, video_name, channel, views, place FROM youtube_trends WHERE DATE(date) = CURDATE() ORDER BY date DESC;"
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
    body = flask.request.get_json()
    if body is None:
        flask.abort(403)
    channels = json.loads(str(body).replace("'", '"'))
    try:
        channel = channels['channel']
    except KeyError:
        flask.abort(403)

    mycursor = ssm_connection()
    global mydb

    query = "INSERT INTO channels (name) VALUES (%s);"
    values = (channel, )
    mycursor.execute(query, values)
    mydb.commit()
    return "Channel "+channel+" added."


@app.route("/ssm/get_kpi_aitu")
def get_kpi_aitu():
    mycursor = ssm_connection()
    sql = "SELECT target, `left`, top_50, top_100, quiz, releases, today, `quarter`, quarter_left from kpi_aitu;"
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
    query = 'UPDATE kpi_aitu set target = %s, `left` = %s, top_50 = %s, top_100 = %s, quiz = %s, releases = %s, today = %s, `quarter` = %s, quarter_left = %s WHERE id = 2'
    val = (int(items[0]), int(items[1]), int(items[2]), int(items[3]), int(items[4]), int(items[5]), int(items[6]), int(items[7]), int(items[8]))
    mycursor.execute(query, val)
    mydb.commit()
    return "ok."


@app.route("/ssm/update_logs", methods=['POST'])
def update_logging():
    body = flask.request.get_json()
    if body is None:
        flask.abort(403)
    data = json.loads(str(body).replace("'", '"'))
    types = [{"kpi_mao": 1}, {"kpi_aitu": 2}, {"aitube_utm": 3}]
    try:
        for item in types:
            if data["type"] == list(item.keys())[0]:
                query = "UPDATE log SET date = %s WHERE idlog = %s"
                value = (data["date"], item[list(item.keys())[0]])
                break
    except KeyError:
        flask.abort(403)
    else:
        mycursor = ssm_connection()
        mycursor.execute(query, value)
        return "ok"


@app.route("/ssm/get_logs_<logtype>", methods=['GET'])
def get_logs(logtype):
    types = [{"kpi_mao": 1}, {"kpi_aitu": 2}, {"aitube_utm": 3}]
    mycursor = ssm_connection()
    try:
        for item in types:
            if logtype == list(item.keys())[0]:
                value = (item[list(item.keys())[0]],)
                query = "SELECT date FROM log WHERE idlog = %s"
                mycursor.execute(query, value)
                break
    except KeyError:
        flask.abort(403)
    else:
        query_result = mycursor.fetchall()
        for row in query_result:
            result = {"date": str(row[0])}
        return json.dumps(result, indent=4)
    flask.abort(403)


@app.route("/ssm/get_dashb_params", methods=['GET'])
def get_dashboard_params():
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


@app.before_request
def validate_auth():
    if AUTH:
        body = flask.request.get_json()
        try:
            if body is None or body["auth"] != fauth:
                flask.abort(401)
        except KeyError:
            flask.abort(401)


read_creds()
