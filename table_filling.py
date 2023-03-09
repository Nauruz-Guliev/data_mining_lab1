from googleapiclient.discovery import build
import pandas as pd
import seaborn as sns
from urlextract import URLExtract
import psycopg2
import datetime
import test
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import urllib.request as urllib2
from urllib.parse import urlparse
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import urllib.request as urllib2
import test
import multiprocessing
import test
from threading import Thread
import functools

api_key = 'AIzaSyBbvIBQeZfb5SG01Y-10krITgJGZ4VRZkw'
# Иван Вдудь
youtube_channel_id = 'UCMCgOm8GZkHp8zJ6l7_hIuA'

youtube = build(
    'youtube', 'v3', developerKey=api_key)


def get_channel_stats(youtube, channel_id):
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id)
    response = request.execute()

    data = dict(channel_name=response['items'][0]['snippet']['title'],
                subscriber_count=response['items'][0]['statistics']['subscriberCount'],
                views=response['items'][0]['statistics']['viewCount'],
                video_count=response['items'][0]['statistics']['videoCount'],
                playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
    return data


## Function to get video ids


channel_data = get_channel_stats(youtube, youtube_channel_id)
playlist_id = channel_data['playlist_id']
subscriber_count = channel_data['subscriber_count']


def get_video_ids(youtube, playlist_id):
    request = youtube.playlistItems().list(
        part='contentDetails',
        maxResults=50,
        playlistId=playlist_id)
    response = request.execute()
    video_ids = []
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    more_pages = True
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token)
            response = request.execute()

            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
            next_page_token = response.get('nextPageToken')
    return video_ids


video_ids = get_video_ids(youtube, playlist_id)


def extract_ref_url(description):
    return URLExtract().find_urls(description.split('\n\n')[0])


def convert_youtube_time_to_seconds(t):
    t = t.replace("PT", "", 1)
    if "H" in t:
        t = t.replace("H", ":", 1)
    else:
        t = "0:" + t
    if "M" in t:
        t = t.replace("M", ":", 1)
    else:
        t = t + ":0"
    if "S" in t:
        t = t.replace("S", "", 1)
    else:
        t = t + ":0"
    if "::" in t:
        t = t.replace("::", ":", 1)
    x = time.strptime(t, '%H:%M:%S')
    return datetime.timedelta(hours=x.tm_hour, minutes=x.tm_min, seconds=x.tm_sec).total_seconds()


def timeout(limit=None):
    if limit is None:
        limit = DEFAULT_TIMEOUT
    if limit <= 0:
        raise TimeoutError() # why not ValueError here?
    def wrap(function):
        return _Timeout(function,limit)
    return wrap


def extract_url(url):
    parsed_uri = urlparse(url)
    result = '{uri.netloc}'.format(uri=parsed_uri)
    return result


def timeout(timeout):
    def deco(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            res = [Exception('function [%s] timeout [%s seconds] exceeded!' % (func.__name__, timeout))]

            def newFunc():
                try:
                    res[0] = func(*args, **kwargs)
                except Exception as e:
                    res[0] = e

            t = Thread(target=newFunc)
            t.daemon = True
            try:
                t.start()
                t.join(timeout)
            except Exception as je:
                print('error starting thread')
                raise je
            ret = res[0]
            if isinstance(ret, BaseException):
                raise ret
            return ret

        return wrapper

    return deco


@timeout(5)
def parse_url(urlToOpen):
    opener = urllib2.build_opener(urllib2.HTTPRedirectHandler)
    req = Request(
        url=urlToOpen,
        headers={
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Accept-Encoding': 'none',
            'Accept-Language': 'en-US,en;q=0.8',
            'Connection': 'keep-alive'}
    )
    try:
        request = opener.open(req)
        return extract_url(request.url)
    except Exception as e:
        return extract_url(urlToOpen)
    return result


def get_parsed_url(url):
    try:
        return parse_url(url)
    except Exception as e:
        return extract_url(url)


def get_video_details(youtube, video_ids):
    all_video_stats = []
    for i in range(0, len(video_ids), 50):
        requestFirst = youtube.videos().list(
            part='snippet,statistics',
            id=','.join(video_ids[i:i + 50]))
        responseFirst = requestFirst.execute()

        requestSecond = youtube.videos().list(
            part='contentDetails',
            id=','.join(video_ids[i:i + 50]))
        responseSecond = requestSecond.execute()
        for videoFirst, videoSecond in zip(responseFirst['items'], responseSecond['items']):
            domain_url = extract_ref_url(videoFirst['snippet']['description'])
            print(domain_url)
            video_stats = dict(title=videoFirst['snippet']['title'],
                               ref_url=domain_url,
                               description_length=len(videoFirst['snippet']['description']),
                               ref_url_domain=get_parsed_url(domain_url[0]) if len(domain_url) else "",
                               published_date=videoFirst['snippet']['publishedAt'],
                               views=videoFirst['statistics'].get('viewCount'),
                               likes=videoFirst['statistics'].get('likeCount'),
                               comments=videoFirst['statistics'].get('commentCount'),
                               duration=convert_youtube_time_to_seconds(videoSecond['contentDetails'].get('duration')))
            all_video_stats.append(video_stats)
    return all_video_stats


video_details = get_video_details(youtube, video_ids)
print(video_details)


def get_recent_video_details(video_details, date):
    start_date = datetime.datetime.strptime(date, '%d%m%Y').date()
    sorted_video_details = []
    for video in video_details:
        date = datetime.datetime.strptime(video['published_date'], "%Y-%m-%dT%H:%M:%SZ").date()
        if start_date < date:
            sorted_video_details.append(video)
    return sorted_video_details


start_date = '01012022'
recent_video_details = get_recent_video_details(video_details, start_date)


def db_insert(record_to_insert, tableName):
    global connection, cursor
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="nauruz0304",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="postgres")
        cursor = connection.cursor()
        sql_create_table_query = "CREATE TABLE IF NOT EXISTS " + tableName + "(id serial PRIMARY KEY, " \
                                 + "title VARCHAR, ref_url_domain VARCHAR, ref_url VARCHAR, published_date VARCHAR, " \
                                   "views VARCHAR, description_length VARCHAR," \
                                   "likes VARCHAR, comments VARCHAR, duration VARCHAR, subscriber_count VARCHAR);"
        sql_insert_query = "INSERT INTO " + tableName + "(title, ref_url, ref_url_domain, published_date, views, " \
                                                        "description_length,likes, comments, " \
                                                        "duration, subscriber_count) " \
                                                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        cursor.execute(sql_create_table_query)
        cursor.execute(sql_insert_query, record_to_insert)
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into + " + tableName + " table".format(error))
    finally:
        if connection:
            cursor.close()
            connection.close()


def insert_video_details(data, tableName):
    for detail in data:
        detail_to_insert = (detail['title'], detail['ref_url'][:1][:1], detail['ref_url_domain'],
                            detail['published_date'], detail['views'], detail['description_length'], detail['likes'],
                            detail['comments'], detail['duration'], subscriber_count)
        db_insert(detail_to_insert, tableName)


insert_video_details(recent_video_details, channel_data['channel_name'])
