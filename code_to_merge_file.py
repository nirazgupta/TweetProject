## This program will search for all the .txt files in subdirectories of a given path and merge them into a single file
## while also extrating only the date, tweet and the user

import time
import os
from fnmatch import fnmatch
import json
import datetime as dt
import sqlite3
# from tqdm import tqdm

start_time = time.time()

con = sqlite3.connect('tweet.db')

#creates cursor on connection as 'c'
c = con.cursor()



def process_json_file(root, tweet_file, path_file, pattern):
    c.execute('''CREATE TABLE IF NOT EXISTS tweetTable(created_at TEXT, tweet TEXT, user TEXT)''')
    for path, subdirs, files in os.walk(root):
        for name in files:
            if fnmatch(name, pattern):
                file_paths = os.path.join(path, name)
                file_paths_list = os.path.join(path, name).split('\n')
                for f in file_paths_list:
                    path_file.write(file_paths + '\n')
                    with open(f, 'r', encoding='utf-8') as infile:
                        print('current file path:', f)
                        for line in infile:
                            try:
                                tweet = json.loads(line)
                                text = tweet['text'].replace('\n', '')
                                date = dt.datetime.strptime(tweet['created_at'], "%a %b %d %H:%M:%S %z %Y").replace(second=0, microsecond=0, tzinfo=None).strftime('%Y-%m-%d %H:%M')
                                name = tweet['user']['screen_name']
                                #print(date + ' , ' + text + ' , ' + name)
                                tweet_file.write(date + ' , ' + text + ' , ' + name + '\n')
                                #c.execute("INSERT INTO tweetTable (created_at, tweet, user) VALUES(?, ?, ?)", (date, text, name))                              
                            except:
                                continue
                        #con.commit()
							
#root_path = 'F:\\Twitter\\test'
root_directory = os.getcwd().strip()
json_files_directory = root_directory + '/Twitter_Project'
pattern = "*.txt"
final_tweet_data = open(root_directory+'/final_tweet_data.txt', 'w')
path_file = open(root_directory+'/file_paths.txt', 'w')

process_json_file(json_files_directory, final_tweet_data, path_file, pattern)
end_time = time.time()
print("Elapsed time was %g seconds" % (end_time - start_time))

