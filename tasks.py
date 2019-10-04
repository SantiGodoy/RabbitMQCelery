# -*- coding: utf-8 -*-

from celery import Celery, group, chord, chain
from celery.schedules import crontab
import os, json, dropbox
import numpy as np
import matplotlib.pyplot as plt
import tweepy
plt.rcdefaults()

app = Celery('tasks', broker='amqp://guest@localhost//', backend = 'redis://')
token = '
dbx = dropbox.Dropbox(token)
twitters = ['Marvel', 'DCComics', 'LVPIbai']

# Config
app.conf.beat_schedule = {
    'task3': {
        'task': 'tasks.process',
        'schedule': 10.0
    }
}


@app.task
def getTweets(nombre):
    consumer_key = ""
    consumer_secret = ""
    access_token = ""
    access_token_secret = ""

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    timeline = api.user_timeline(screen_name=nombre, count=100, include_rts=False, parser=tweepy.parsers.JSONParser())

    return timeline

@app.task
def writeRetweetsInFile(timelines):
    count = [0] * len(twitters)
    i = 0
    fichero = 'tweets.txt'

    jsonFile = open(fichero, 'w')

    for timeline in timelines:
        for tweet in timeline:
            count[i] += tweet['retweet_count']
        jsonFile.write(str(count[i]) + "\n")
        i += 1
    jsonFile.close()
    upload_dropbox.delay(fichero)
    
    return count
    
@app.task
def upload_dropbox(str):
    with open("./" + str, 'rb') as f:
        plot_data = f.read()

    try:
        response = dbx.files_upload(plot_data,"/" + str, mode=dropbox.files.WriteMode.overwrite)
    except dropbox.exceptions.ApiError as err:
        print('*** API error', err)

@app.task
def generarGrafica(retweets_count):
    fichero = "rts_plot.png"

    plt.bar(np.arange(len(twitters)), retweets_count)
    plt.xticks(np.arange(len(twitters)), twitters, rotation=90)
    plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
    plt.savefig(fichero, pad_inches=1, dpi=300)

@app.task
def process():
    chord([getTweets.s(twitter) for twitter in twitters])(chain(writeRetweetsInFile.s(), generarGrafica.s()))


