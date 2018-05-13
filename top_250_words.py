import sqlite3
import os

root_directory = os.getcwd().strip()
words_file = root_directory+'/top250Words.txt'

con = sqlite3.connect('tweet.db')
cur = con.cursor()

top_250_words_sql = '''select word, count(word) as counted from raw_top_words
                        group by word
                        order by count(word) desc
                        limit 250'''

cur.execute(top_250_words_sql)
data = cur.fetchall()
# print(data)

create_table_sql = '''CREATE TABLE IF NOT EXISTS top_words(word_id INTEGER PRIMARY KEY, word TEXT, count INTEGER)'''
cur.execute(create_table_sql)


cur.executemany("INSERT INTO top_words(word, count) values(?,?)", data)
con.commit()

with open(words_file, 'w', encoding='utf-8') as f:
    for i in data:
        f.write(i[0] + '\n')


