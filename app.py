import json
import mysql.connector
import flask
from datetime import datetime


class MyDict(dict):
    def __init__(self):
        self = dict()

    def add(self, key, value):
        self[key] = value


AUTH = False
mydb = None
fhost = ""
fuser = ""
fpass = ""
fauth = ""

app = flask.Flask(__name__)

def read_creds():
    global fhost, fuser, fpass, fauth
    with open("credentials.txt") as f:
        fhost = f.readline()
        fuser = f.readline()
        fpass = f.readline()
        fauth = f.readline()
    fpass = fpass.strip()


def instagram_connection():
    global mydb
    mydb = mysql.connector.connect(
        host=fhost,
        user=fuser,
        password=fpass,
        database="instagram"
    )
    return mydb.cursor()


def ssm_connection():
    global mydb
    mydb = mysql.connector.connect(
        host=fhost,
        user=fuser,
        password=fpass,
        database="ssm"
    )
    return mydb.cursor()


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


@app.route("/ssm/uniq_byprojectid=<int:project_id>")
def get_unique_users_by_projectid(project_id):
    mycursor = ssm_connection()
    query = "SELECT ReleaseID, EpisodesName, UniqUser, Traffic FROM ssm.releases where projectID = %s"
    val = (project_id,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    mydict = MyDict()
    for row in query_result:
        mydict.add(row[0], ({"episode_name": row[1], "unique_count": row[2], "traffic": row[3]}))
    result = json.dumps(mydict, indent=4)
    return result


@app.route("/ssm/fullinfo_byprojectid=<int:project_id>")
def get_fullinfo_by_projectid(project_id):
    mycursor = ssm_connection()
    query = "SELECT * FROM ssm.releases where projectID = %s"
    val = (project_id,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    mydict = MyDict()
    for row in query_result:
        mydict.add(row[0], (
            {"episode_name": row[1], "unique_count": row[2], "traffic": row[3], "release_date": str(row[4]),
             "tail": row[5],
             "traffic_per_day": row[6], "traffic_per_tail": row[7], "youtube_views": row[8],
             "avg_view_by_user": row[9], "shows": row[10], "ctr": row[11], "uniq_users_youtube": row[12],
             "subscribers": row[13]}))
    result = json.dumps(mydict, indent=4)
    return result


@app.route("/ssm/fullinfo_byprojectname=<project>")
def get_fullinfo_by_projectname(project):
    project = str(project) + "%"
    mycursor = ssm_connection()
    query = "SELECT * FROM releases where projectID in (select ProjectID from project where ProjectName like %s);"
    val = (project,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    mydict = MyDict()
    for row in query_result:
        mydict.add(row[0], (
            {"episode_name": row[1], "unique_count": row[2], "traffic": row[3], "release_date": str(row[4]),
             "tail": row[5],
             "traffic_per_day": row[6], "traffic_per_tail": row[7], "youtube_views": row[8],
             "avg_view_by_user": row[9], "shows": row[10], "ctr": row[11], "uniq_users_youtube": row[12],
             "subscribers": row[13]}))
    result = json.dumps(mydict, indent=4)
    return result


@app.route("/ssm/releases_for_<int:n>_<period>")
def get_releases_by_period(period, n):
    # CURDATE - INTERVAL N PERIOD
    periods = ["DAY", "WEEK", "MONTH", "YEAR"]
    period = period.upper()
    if period not in periods:
        return "{}"
    mycursor = ssm_connection()
    query = "SELECT releaseID, ProjectName, EpisodesName, UniqUser, Traffic, ReleaseDate, Tail, TrafficPerDay, TrafficPerTail, YoutubeViews," \
            "AverageViewsByUser, Shows, CTR, UniqUserYoutube, Subscribers, Price FROM ssm.releases, ssm.project " \
            "WHERE ReleaseDate > DATE_SUB(CURDATE(), INTERVAL %s " + period + ") and releases.ProjectID = project.projectID;"
    val = (n,)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    mydict = MyDict()
    for row in query_result:
        mydict.add(row[0], (
            {"project_name": row[1], "episode_name": row[2], "unique_count": row[3], "traffic": row[4],
             "release_date": str(row[5]), "tail": row[6],
             "traffic_per_day": row[7], "traffic_per_tail": row[8], "youtube_views": row[9],
             "avg_view_by_user": row[10], "shows": row[11], "ctr": row[12], "uniq_users_youtube": row[13],
             "subscribers": row[14]}))
    result = json.dumps(mydict, indent=4)
    return result


@app.route("/ssm/release_between_<date1>_and_<date2>")
def get_releases_between(date1, date2):
    mycursor = ssm_connection()
    date1 = date1.replace(".", "/")
    date2 = date2.replace(".", "/")
    query = "SELECT releaseID, ProjectName, EpisodesName, UniqUser, Traffic, ReleaseDate, Tail, TrafficPerDay, TrafficPerTail, YoutubeViews," \
            "AverageViewsByUser, Shows, CTR, UniqUserYoutube, Subscribers, Price FROM releases, project where releases.ProjectID = project.projectID and (ReleaseDate between %s and %s);"
    val = (date1, date2)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    mydict = MyDict()
    for row in query_result:
        mydict.add(row[0], (
            {"project_name": row[1], "episode_name": row[2], "unique_count": row[3], "traffic": row[4],
             "release_date": str(row[5]), "tail": row[6],
             "traffic_per_day": row[7], "traffic_per_tail": row[8], "youtube_views": row[9],
             "avg_view_by_user": row[10], "shows": row[11], "ctr": row[12], "uniq_users_youtube": row[13],
             "subscribers": row[14]}))
    result = json.dumps(mydict, indent=4)
    return result


@app.route("/ssm/kpi_<year>_<country>")
def get_kpi_by_country_year(year, country):
    mycursor = ssm_connection()
    query = "SELECT idkpi, value, target, month FROM kpi_mao where year = %s and country = %s;"
    val = (year, country)
    mycursor.execute(query, val)
    query_result = mycursor.fetchall()
    mydict = MyDict()
    for row in query_result:
        mydict.add(row[0], ({"value": row[1], "target": row[2], "month": row[3]}))
    result = json.dumps(mydict, indent=4)
    return result


def calculate_cpa(price, aitube, youtube):
    return price / (aitube+youtube)


def calculate_cpc(price, traffic):
    return price/traffic


def calculate_cpu(price, uniq_user):
    return price/uniq_user


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
