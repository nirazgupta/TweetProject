# TweetProject
TweetProject is a social media data mining and data processing project written in Python which fetches data from Twitter API and uses logic built with Python and SQL to process and generate top 250 words from 1 terabyte of JSON data.

# Libraries used
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import TweepError
import json
import sqlite3
import datetime as dt
from Authentication import authentication
import time
from fnmatch import fnmatch
import datetime as dt
import time # for measuring time
import os # to work with paths
from fnmatch import fnmatch # to match name and pattern
import csv # to work with csv
import sqlite3 #to work with database
from __future__ import generators # needs to be at the top of your module
from string import punctuation
import string
from operator import itemgetter
import fileinput
import sys
import os.path
import pandas as pd
import codecs
import re
import preprocessor as p
import numpy as np

# Project structure
* TweetMiner -> executing this programs, connects to the Twitter API and write the data to text file.
* CandidatesTweetsMerger.py -> this program looks for files located in the CandidatesTweets folder, merge the files to single file, extract the data to the sqlite database.
* code_to_merge_file.py -> this program walks through the given root directory and looks for files matching the given pattern to merge them together as well as process and extract data and then insert them to the sqlite database.
* code_by_user -> this program basically runs sql query on the sqlite database, one query for tweets tweeted by user and other for tweets retweeted by others and code 1 if it was tweeted by user else 0. Then the program uses csv dictwriter to write the coded content in a new file.
* wordreader3v5.py -> this program uses the coded file to extract only tweet text from the file and tokenize the words in batches and write them to new file for each batch as well as inserts the data to database.
* top_250_words.py -> this program finally runs sql query to generate 250 words from entire words and write them to a new file.

Note: The size of data for which the program was developed was 1 terabyte. Size after extracting only date, tweet and user was
30+ GB. Due to size contraint these files could note be included in the repo.
