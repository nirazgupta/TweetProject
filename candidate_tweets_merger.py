# import required modules
import time # for measuring time
import os # to work with paths
from fnmatch import fnmatch # to match name and pattern
import csv # to work with csv
import sqlite3 #to work with database

## create connection con by calling the sqlite3 module on tweet.db, it will automatically create the db if not already created or connect to the existing db
con = sqlite3.connect('tweet.db')

#creates cursor on connection as 'c'
c = con.cursor()

start_time = time.time()

## get the root directory path
root_directory = os.getcwd()

## create function merge_cand_file()
def merge_cand_file(root, cand_file, path_file, pattern):
    ## loop for path, subdirectory, files in the root directory using os.walk module
    for path, subdirs, files in os.walk(root):
        ## loop for name in the files
        for name in files:
            ## match the name and pattern i.e. 'csv' using fnmatch module
            if fnmatch(name, pattern):
                ## set file path by joining the path and the file name
                file_paths = os.path.join(path, name)
                ## create a list of the paths using split function
                file_paths_list = os.path.join(path, name).split('\n')
                ## loop for paths in the file_paths_list to get each path
                for f in file_paths_list:
                    ## printing the paths just for debugging purpose, not really required
                    print(f)
                    ## writing the paths to the candidate_file_paths.txt file. This is also optional
                    path_file.write(file_paths + '\n')
                    ## opening files in each path as infile
                    with open(f, 'r', encoding='utf-8') as infile:
                        ## reading the opened file using csv reader, seperated by ';'
                        readCSV = csv.reader(infile, delimiter=';')
                        ## used the next function to skip the header
                        next(readCSV, None)
                        ## loop through row of infile and write the row to the candidate_file_paths.txt file
                        for row in infile:
                            cand_file.write(row)

## create function named extract_cand_data() 
def extract_cand_data(cand_extracted_file):
    ## create candidateTable in db with columns: date, tweet, user using execute() function on cursor
    c.execute('''CREATE TABLE IF NOT EXISTS candidateTable(date TEXT, tweet TEXT, user TEXT)''')
    ## open the candidate_tweets_merged.csv file in read mode as csvfile
    with open(root_directory+'/candidate_tweets_merged.csv', 'r', encoding='utf-8') as csvfile:
        ## read the content of the csvfile using csv.reader() function
        readCSV = csv.reader(csvfile, delimiter=';')
        ## skip the header by calling next() function
        next(readCSV, None)
        ## loop for row in the readCSV object
        for row in readCSV:
            cand = row[0] ## store the candidate name in 'cand' variable
            date = row[1] ## store the date in 'date' variable
            tweet = row[4] ## store the tweet in 'tweet' variable
            ## write data in these variables to the file
            cand_extracted_file.write(date + ';' + cand + ';' + tweet + '\n')
            ## insert data from these variables into the candidateTable 
            c.execute("INSERT INTO candidateTable(date, tweet, user) VALUES (?,?,?)", (date, tweet, cand))
        con.commit()


## Below are the paths of files

root = root_directory+'/CandidateTweets' ## root directory with path to CandidateTweets folder which contains csv files
pattern = "*.csv" ## pattern we will supply to look for files ending with csv extension

## open new file named candidate_tweets_merged.csv if not exists in write mode to write the merged content 
candidate_tweets_merged = open(root_directory+'/candidate_tweets_merged.csv', 'w', encoding='utf-8')

## open new file named candidate_file_paths.txt to write the paths (this file is not really needed)
candidate_path_file = open(root_directory+'/candidate_file_paths.txt', 'w')

## open new file named candidate_extracted.txt in write mode to write the extracted content from the candidate_tweets_merged.csv file
cand_extracted_file = open(root_directory+'/candidate_extracted.txt', 'w', encoding='utf-8')

## call the merge_cand_file() function with the arguments to begin the merge process
merge_cand_file(root, candidate_tweets_merged, candidate_path_file, pattern)

## call the extract_cand_data() function to begin extraction
extract_cand_data(cand_extracted_file)

## optional code to measure the time taken
end_time = time.time()
print("Elapsed time was %g seconds" % (end_time - start_time))
