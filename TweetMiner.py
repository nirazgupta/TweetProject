import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import TweepError
import json
import sqlite3
import datetime as dt
from Authentication import authentication

#create the connection with 'tweet2.db' database as 'con'
con = sqlite3.connect('tweet2.db')

#creates cursor on connection as 'c'
c = con.cursor()

def create_cand_table():
    c.execute('''CREATE TABLE IF NOT EXISTS candidateTable(date TEXT, tweet TEXT, user TEXT)''')

def create_tweet_table():
    c.execute('''CREATE TABLE IF NOT EXISTS tweetTable(created_at TEXT, tweet TEXT, user TEXT)''')

consumer_key = 'mBhnrL7jjl80KhL2NiHbmQTba'
consumer_secret = '4tGjiBJQac3t43rqiU8W8sRzUFVwZJjjJa5KGGKwpRlHSyHqQD'
access_token = '2598113214-G1C9CQV2Du7ZnMKy1fMvIz2R06dYbXmGVGbgY1p'
access_secret = 'GiwmMK6VV7oYILGR7LiumO0U7QNcDHxXojZOUxkYD98O2'



class Listener(StreamListener):
    def on_data(self, data):
        whole_data = json.loads(data)
        tweet = whole_data["text"]
        date = dt.datetime.strptime(whole_data['created_at'], "%a %b %d %H:%M:%S %z %Y")
        clean_date = date.replace(second=0, microsecond=0, tzinfo=None)
        FinalDate = clean_date.strftime('%Y-%m-%d %H:%M')
        user = whole_data["user"]["screen_name"]
        c.execute("INSERT INTO tweetTable (created_at, tweet, user) VALUES (?,?,?)", (FinalDate, tweet, user))
        con.commit()

        print(data)
        savefile = open('output.txt', 'a', encoding='utf-8')
        savefile.write(data)
        savefile.write('\n')
        savefile.close()
        return True

    def on_error(self, status):
        print(status)



def execute_streaming():
    try:
        print('Success! Connected.')
        search_list = []
        n = int(input('How many keywords you want to search?'))
        for x in range(n):
            search_keyword = input('Enter the key you want search for: ')
            search_list.append(search_keyword)
            print(str(search_list))
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)
        twitterStream = Stream(auth, Listener())
        twitterStream.filter(track=search_list)
    except TweepError as e:
        print(e.response.status)

create_tweet_table()
execute_streaming()
