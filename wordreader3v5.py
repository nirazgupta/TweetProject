## Using python 3


from string import punctuation
import string
from operator import itemgetter
import fileinput
import sys
import os.path
import time
import csv
import pandas as pd
import codecs
import nltk
from nltk.corpus import stopwords
import re
import preprocessor as p
import numpy as np
import sqlite3

start_time = time.time()
# NLTK's default German stopwords
default_stopwords = set(nltk.corpus.stopwords.words('english'))

# reload(sys)
# sys.setdefaultencoding('utf8')

def get_file_with_parents(filepath, levels=1):
    common = filepath
    for i in range(levels + 1):
        common = os.path.dirname(common)
    return os.path.relpath(filepath, common)

root = os.getcwd()
root1 = get_file_with_parents('/coded5.csv', 1)

source_file_path = os.path.join(root, root1)


#source_file = open(source_file_path, encoding='utf-8')

wordfreq_dir = 'wordfreq'
wordfreq_dir_path = os.path.join(root, wordfreq_dir)

wordfreq_file = 'wordlist'
wordfreq_file_path = os.path.join(wordfreq_dir_path, wordfreq_file)
print(wordfreq_file_path)



con = sqlite3.connect('tweet.db')
print(con)
#creates cursor on connection as 'c'
c = con.cursor()
print(c)


forbidden = ['', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0',
			 '10', '11', '12', '13', '14', '15', '16', '17', '18', '19',
			 '20', '21', '22', '23', '24', '25', '26', '27', '28', '29',
			 '30', '31', '32', '33', '34', '35', '36', '37', '38', '39',
			 '40', '41', '42', '43', '44', '45', '46', '47', '48', '49',
			 '50', '51', '52', '53', '54', '55', '56', '57', '58', '59',
			 '60', '61', '62', '63', '64', '65', '66', '67', '68', '69',
			 '70', '71', '72', '73', '74', '75', '76', '77', '78', '79',
			 '80', '81', '82', '83', '84', '85', '86', '87', '88', '89',
			 '90', '91', '92', '93', '94', '95', '96', '97', '98', '99',
			 '100', '2015', '2017', 'you', 'oh', 'if', 'me', 'we', 'us',
			 'he', 'she', 'u', 'en', 'de', 'que', 'my', 'only', 'who', 'get',
			 'la', 'el', 'y', 'the', 'a', 'an', 'the', 'and', 'i', 'am', 'is',
			 'are', 'was', 'were', 'be', 'being', 'been', 'shall', 'will',
			 'may', 'can', 'has', 'have', 'had', 'do', 'does', 'did', 'should',
			 'would', 'could', 'might', 'must', 'aboard', 'about', 'above',
			 'across', 'after', 'against', 'along', 'amid', 'among', 'anti',
			 'around', 'as', 'at', 'before', 'behind', 'below', 'beneath',
			 'beside', 'besides', 'between', 'beyond', 'but', 'by', 'concerning',
			 'considering', 'despite', 'down', 'during', 'except' , 'excepting',
			 'excluding', 'following', 'for', 'from', 'in', 'inside', 'into', 'like',
			 'minus', 'near', 'of', 'off', 'on', 'onto', 'opposite', 'outside',
			 'over', 'past', 'per', 'plus', 'regarding', 'round', 'save', 'since',
			 'than', 'through', 'to', 'too', 'toward', 'towards', 'under', 'underneath',
			 'unlike', 'until', 'up', 'upon', 'versus', 'via', 'with', 'within',
			 'without', 'him', 'her', 'it', 'his', 'hers', 'its', 'or', 'so', 'yet',
			 'nor', 'jan', 'feb', '+0000', '2016', '^', '0000', '01', '02', '03',
			 '04', '05', '06', '07', '08', '09', 'mon', 'tue', 'wed', 'thu', 'fri',
			 'sat', 'sun', 'r','-' ,'rt', '…', 'amp', '�', 'ht…', 'a…', 'aaaand', 'h…',
			 'i','me','my','myself','we','our','ours','ourselves','you','your','yours','yourself','yourselves','he','him',
			'his','himself','she','her','hers','herself','it','its','itself','they','them','their','theirs','themselves',
			'what','which','who','whom','this','that','these','those','am','is','are','was','were','be','been','being',
			'have','has','had','having','do','does','did','doing','a','an','the','and','but','if','or','because','as','until',
			'while','of','at','by','for','with','about','against','between','into','through','during','before','after',
			'above','below','to','from','up','down','in','out','on','off','over','under',
			'again','further','then','once','here','there','when','where','why','how','all','any',
			'both','each','few','more','most','other','some','such','no','nor','not','only','own',
			'same','so','than','too','very','s','t','can','will','just','don','should','now', "i\'ve", 'j', "he's", 'w', "doesn't"
			"i'm", "htt..."]

forbiddencounter = len(forbidden)


http = 'http'


filecount = 0
# def chunked(file, chunk_size):
#     return iter(lambda: file.read(chunk_size), '')

chunked = pd.read_csv('coded5.csv', sep=',', chunksize=1024*100, index_col=False)
c.execute('''CREATE TABLE IF NOT EXISTS raw_top_words(word_id INTEGER PRIMARY KEY, word TEXT, count INTEGER)''')

for data in chunked:
	tweet = data['Tweet'].values
	p.set_options(p.OPT.URL, p.OPT.EMOJI)
	tweet = [p.clean(str(line)) for line in tweet] 
	
	N = 250
	counter = 0
	truen = 0
	words = {}
	
	words_gen = []
	for word in str(tweet).split():
		words_gen.append(word.strip(punctuation).lower())

	# #words_gen = (str(word).strip('[]').strip(punctuation).lower() for line in chunked for word in line['Tweet'].str.split())
	
	for word in words_gen:
		words[word] = words.get(word, 0) + 1

	
	top_words = sorted(words.items(), key=itemgetter(1), reverse=True)[:N]

	for word, frequency in top_words:
		if http in word:
			counter = counter + 1
		else:
			for x in forbidden:
				if x == word:
					counter = counter + 1

	truen = N + counter


	top_words2 = sorted(words.items(), key=itemgetter(1), reverse=True)[:truen]

	
	bigcounter = 0
	for word, frequency in top_words2:
		#if any(x in top_words2 for x not in forbidden):# <---This needs work, shows some promise
		if http not in word:
			if len(word)>4 and word not in forbidden:
				counter1 = 0
				counter2 = 0
				for x in forbidden:
					if x == word:
						counter1 = counter1 + 1
						counter2 = counter2 + 1
					else:
						counter2 = counter2 + 1
						if counter2 == forbiddencounter:
							if counter1 == 0:
								c.execute("INSERT INTO raw_top_words(word, count) VALUES(?, ?)", (word, frequency))
								with open(wordfreq_file_path+str(filecount)+'.txt', 'a', encoding='utf-8') as outf:
									#write_str = "{}\n".format(word)
									print(word, frequency)
									
									#outf.write(write_str)
							
		else:
			continue
	filecount += 1
	con.commit()
	words = {}


	

