from __future__ import generators # needs to be at the top of your module
import sqlite3
import csv
con = sqlite3.connect('tweet.db')
# con.text_factory = bytes
cur1 = con.cursor()
cur2 = con.cursor()

query1 = '''SELECT DISTINCT created_at, tweet, user FROM tweetTable WHERE user = (SELECT DISTINCT user from candidateTable)'''
query2 = '''SELECT created_at, tweet, user FROM tweetTable where user <> (select DISTINCT user from candidateTable)'''

def ResultIter(cursor, arraysize=1024):
    'An iterator that uses fetchmany to keep memory usage down'
    print('fetching records')
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result


#def ResultIter(cursor, chunk_size=1024):
 #   print('fetching data')
  #  return iter(lambda: cursor.fetchmany(chunk_size), '')


cur1.execute(query1)
cur2.execute(query2)


with open('coded5.csv', 'w', newline='', encoding='utf-8') as f:
    #writer = csv.writer(f)
    writer = csv.DictWriter(f, delimiter=',', fieldnames = ["Date", "Tweet", "User", "Code"])
    writer.writeheader()
    #writer.writerow(['Date', 'Tweet', 'User', 'Code'])
    for row in ResultIter(cur1):
        #row += ('1')
        print({'date':row[0], 'text':row[1].replace(',',' '), 'user':row[2], 'code':1})
        writer.writerow({'Date':row[0], 'Tweet':row[1].replace(',',' '), 'User':row[2], 'Code':1})
        #writer.writerow(row)        
    for row in ResultIter(cur2):
        writer.writerow({'Date':row[0], 'Tweet':row[1].replace(',',' '), 'User':row[2], 'Code':0})
        #writer.writerow(row)
con.commit()
con.close()
