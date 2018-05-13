import csv
import sqlite3
import json
import ijson
import datetime as dt
import time

start_time = time.time()
#create the connection with 'tweet2.db' database as 'con'
con = sqlite3.connect('tweet.db')

#creates cursor on connection as 'c'
c = con.cursor()

def create_cand_table():
    c.execute('''CREATE TABLE IF NOT EXISTS candidateTable(date TEXT, tweet TEXT, user TEXT)''')

def create_tweet_table():
    c.execute('''CREATE TABLE IF NOT EXISTS tweetTable(created_at TEXT, tweet TEXT, user TEXT)''')

class CandidateFile:

    no_of_files = 0

    def __init__(self, filename):
        self.filename = filename

        CandidateFile.no_of_files += 1
        #print(filename)

    def readfile(self):
        with open(self.filename, 'r', encoding='utf-8') as csvfile:
            readCSV = csv.reader(csvfile, delimiter=';')
            next(readCSV, None)
            for row in readCSV:
                cand = row[0]
                date = row[1]
                tweet = row[4]
                c.execute("INSERT INTO candidateTable(date, tweet, user) VALUES (?,?,?)", (date, tweet, cand))
            con.commit()

    def writefile(self):
        with open(self.filename, 'r', encoding='utf-8') as csvfile, open('F:\Twitter\candidate_extracted.csv', 'w', encoding='utf-8') as outfile:
            readCSV = csv.reader(csvfile, delimiter=';')
            next(readCSV, None)
            for row in readCSV:
                cand = row[0]
                date = row[1]
                tweet = row[4]
                #final_tweet_data.write(line.rstrip('\r\n'))
                outfile.write(date+';'+cand+';'+tweet+'\n')
            con.commit()


class TweetFile:
    def __init__(self, filename):
        self.filename = filename
        self.tweet_data = []
        self.created_at = []
        self.user = []


    def extract_data(self):
        counter = 0
        with open(self.filename, 'r', 2000000) as infile, open('F:\\Twitter\\tweet_extracted.txt', 'w') as tweetfile1:
            for line in infile:
                try:
                    tweet = json.loads(line)
                    x = tweet['text'].replace('\n', '')
                    date = dt.datetime.strptime(tweet['created_at'], "%a %b %d %H:%M:%S %z %Y").replace(second=0, microsecond=0, tzinfo=None).strftime('%Y-%m-%d %H:%M')
                    #clean_date = date.replace(second=0, microsecond=0, tzinfo=None)
                    #FinalDate = clean_date.strftime('%Y-%m-%d %H:%M')
                    name = tweet['user']['screen_name']
                    #print(date, x, name)
                    # self.tweet_data.append(x)
                    # self.created_at.append(FinalDate)
                    # self.user.append(name)
                    print(tweet)
                    #print(date + ' , '+ x + ' , '+ name + '\n')

                    tweetfile1.write(date+' , ' + x + ' , ' + name + '\n')
                    ###The following lines are commented out, but can be used if required and it also works as another approach
                    ###i.e. if we don't want to use lists, we can instead run the cursor with execute function with INSERT
                    ###query with required arguments and commit the query. This will work fine.
                    #c.execute("INSERT INTO tweetTable (created_at, tweet, user) VALUES(?, ?, ?)", (date, x, name))
                    #con.commit()
                except:
                    continue

    def insert_data(self):
        zipped_data = zip(self.created_at, self.tweet_data, self.user)
        c.executemany("INSERT INTO tweetTable (created_at, tweet, user) VALUES(?, ?, ?)", (zipped_data))
        con.commit()


create_cand_table()
create_tweet_table()

#inputfile = input('Choose the type of file: (candidate(c)/tweet(t)/quit(q)) ')
# candFile = input("Enter candidate file: ") + '.csv'
# tweetFile = input("Enter tweet file: ") + '.txt'
#while True:
 #   if inputfile.lower() == 'candidate' or inputfile.lower() == 'c':
 #       file = CandidateFile(input('Enter the filename: ') + '.csv')
#        print(str(file.filename ) + ' is selected')
#        infile_obj = file.readfile()
#    elif inputfile.lower() == 'tweet' or inputfile.lower() == 't':
 #       file = TweetFile(input('Enter the filename: ') + '.txt')
 #       print(str(file.filename) + ' is selected')
 #       file.extract_data()
 #       file.insert_data()
 #   elif inputfile.lower() == 'quit' or inputfile.lower() == 'q':
  #      break

candidate_file_path = 'F:\\Twitter\\candidate_tweets_merged.csv'
tweet_file_path = 'F:\\Twitter\\final_tweet_data.txt'
cand_file = CandidateFile(candidate_file_path)
cand_file.readfile()
cand_file.writefile()

tweet_file = TweetFile(tweet_file_path)
tweet_file.extract_data()
#tweet_file.insert_data()

#file1 = CandidateFile(input('Enter file name') + '.csv')
#file1read = file1.readfile()

con.commit()
con.close()
#print(CandidateFile.no_of_files)

end_time = time.time()
print("Elapsed time was %g seconds" % (end_time - start_time))