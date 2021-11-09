import datetime
import os.path
import json
import time
import dateutil.parser
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

DEBUG = False
wd_path = "/home/maksimsalnikov/ssm-backend/gcalendar/"


def get_creds():
    SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']
    creds = None
    if os.path.exists(wd_path+'token.json'):
        creds = Credentials.from_authorized_user_file(wd_path+'token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                wd_path+'client_secret_gcalendar.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open(wd_path+'token.json', 'w') as token:
            token.write(creds.to_json())
    return creds


def get_last_calendar_events(creds, start=None, end=None):
    calendar_id = "ssmedia.kz_7i86em8m8i9gjm6ggdi6tgour0@group.calendar.google.com"
    service = build('calendar', 'v3', credentials=creds)
    if start is None:
        time_min = datetime.datetime.now(datetime.timezone.utc)
    else:
        time_min = start
    if end is None:
        time_max = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(365))
    else:
        time_max = end
    calendars_resource = service.events().list(calendarId=calendar_id, timeMin=time_min.isoformat(), timeMax=time_max.isoformat()).execute()
    calendars = calendars_resource.get('items', [])
    releases_list = []
    for item in calendars:
        if item['status'] == "confirmed":
            if 'recurrence' in item.keys():
                if time_min.replace(tzinfo=None) <= dateutil.parser.isoparse(item['start']['dateTime'][:-6]) <= time_max.replace(tzinfo=None):
                    #releases_list.append({"name":item['summary'].strip(), "date": item['start']['dateTime']})
                    pass
            else:
                releases_list.append({"name": item['summary'].strip(), "date": str(dateutil.parser.isoparse(item['end']['dateTime']).replace(tzinfo=None))})

    return releases_list


def get_releases_from_calendar(startdate=None, enddate=None):
    credentials = get_creds()
    events_list = get_last_calendar_events(credentials, startdate, enddate)
    return events_list


if __name__ == '__main__':
    arr = get_releases_from_calendar()
    for item in arr:
        print(item)
