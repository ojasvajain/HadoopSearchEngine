#!/usr/bin/env python
'''
Reducer Purpose: To produce inverted index and store it in a HBase Database, 
1) Input format: Word,Frequency,FancyHitBit,DocId
2) Row Format: Word - DocId1(Freq,FHBit)$DocId2(Freq,FHBit)$...
3) Store the output row in hbase database 

Everytime Mapreduce job is run, a new column is created in InvertedIndex table which stores the InvertedIndex string of that job.
Wanted to append to existing invertedIndex string but there was an unknown issue in modifying existing entries in hbase table.
'''

import fileinput
import happybase

connection = happybase.Connection('172.31.10.32')      #ip of host running thrift server
table = connection.table('InvertedIndex') 

prev_word = ''
isFirst = True
invertedIndexString = ''

def insertInTable(word,invertedIndexString):  #insert in InvertedIndex table

	invertedIndexString = invertedIndexString[:len(invertedIndexString)-1]  #remove last $
	
	row = table.row(word)   #returns a dictionary
	postings_no = len(row.keys())+1  #new inverted index string will be stored in a new column named ('postings' + postings_no)  
	
	table.put(word, {'cf:postings{0}'.format(postings_no) : invertedIndexString})            #insert into table

for line in fileinput.input():   #read input line by line. Input format is mentioned above. 

	items=line.strip().split('\t')
	
	if(len(items)!=4):
		continue
		
	word,freq,fhbit,docid=items
	
	if(prev_word!=word and isFirst==False):
		insertInTable(prev_word,invertedIndexString)
		invertedIndexString=''
		
	invertedIndexString+="{0}({1},{2})$".format(docid,freq,fhbit)
	prev_word=word
	isFirst=False

insertInTable(prev_word,invertedIndexString)  #to insert last word 

connection.close()
