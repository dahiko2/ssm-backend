import time
import mysql.connector
from googleapiclient.discovery import build
from google.oauth2 import service_account
import json

mydb = mysql.connector.connect()
fhost = ""
fuser = ""
fpass = ""
fdbname_ssm = ""


def read_creds():
    """
    Построчно считывает данные для подключения к бд из файла credentials.txt.
    """
    global fhost, fuser, fpass, fdbname_ssm
    with open("credentials.json") as f:
        credentials = json.load(f)
        fhost = credentials['db_hostname']
        fuser = credentials['db_user']
        fpass = credentials['db_password']
        fdbname_ssm = credentials['db_name_ssm']


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


def import_to_gsheet():
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    SERVICE_ACCOUNT_FILE = 'key_ssm_shop.json'
    gsheetid = "179sx_nEMoQvwx1BguVG-tIT2jwDxX_JsmsV5K4xMhiM"

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()

    mycursor = ssm_connection()
    query = "SELECT * FROM shop ORDER BY order_date ASC;"
    mycursor.execute(query)
    query_res = mycursor.fetchall()
    result_list = []
    for row in query_res:
        status = "Оплачено" if row[11] else "Не оплачено"
        if row[7] is not None:
            basket = json.loads(row[7].replace("'", '"'))
            basket_res = ""
            for item in basket:
                basket_res += str(item['name']) + ' - Цена: ' + str(item['price']) + ' - Размер: ' + str(item['size']) + ' | '
        else:
            basket_res = ""
        result = [f"{row[0]}", f"{row[1]}", f"{row[2]}", f"{row[3]}", f"{row[4]}", f"{row[5]}", f"{row[6]}", f"{basket_res}", f"{row[8]}", f"{row[9]}", f"{row[10]}", f"{status}"]
        result_list.append(result)
    print(len(result_list)+1)
    y = True
    while y:
        try:
            sheet.values().update(spreadsheetId=gsheetid,
                                  range="Заказы!A2", valueInputOption="RAW",
                                  body={"values": result_list}).execute()
            # time.sleep(1)
        except Exception as e:
            print(e)
            time.sleep(2)
        y = False


if __name__ == '__main__':
    read_creds()  # Считывает данные для входа при запуске скрипта
    import_to_gsheet()
