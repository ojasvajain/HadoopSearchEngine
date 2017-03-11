#!/usr/bin/env python

'''
Input: WARC Format File
Output: Word,Frequency,FancyHitBit,DocId

Tasks to be performed:
1)Store (DocId,FancyHits,Url) rows in HBase database if its not already there.
2)Tokenize the doc and print in the following format: Word,Frequency,FancyHitBit,DocId
'''



import fileinput
import netifaces as ni
import time
import html2text
from bs4 import BeautifulSoup 
import happybase   #To connect to HBase Database
import sys

import nltk
from nltk.corpus import stopwords
from stemming.porter2 import stem


 		
connection = happybase.Connection('172.31.10.32')    #ip of server where thrift server is running
UrlsTable = connection.table('Urls')                 #stores rows of urls that have been encountered, to prevent same website getting indexed more than once

WebPagesTable = connection.table('Webpages')        #stores rows of format: docId,fancyhits,url

domain_list = ['en.wikipedia.org','reddit.com','quora.com','goal.com/en','geeksforgeeks.org','marca.com/en','medium.com','stackoverflow.com','flipkart.com','amazon.in','linkedin.com','fossbytes.com','twitter.com','manipal.edu','youtube.com','flipkart.com','huffingtonpost.in','bbc.com','angel.co','facebook.com']

respCheck = ('WARC-Type: response').lower()      #variables meant for parsing warc files to find html response
isRespDetected=False

urlCheck = ('WARC-Target-URI:').lower()
url = ''

contentLengthCheck = ('Content-Length:').lower()
contentLength = -1

htmlRespCheck1 = ('HTTP').lower()
htmlRespCheck2 = ('200 OK').lower()
htmlDocStart = '<!'
isReadingHtmlContent = False
html_content = ''

htmlDocEndCheck = ('WARC/1.0').lower()

ip_addr = ni.ifaddresses('eth0')[2][0]['addr']   #for creating salted id



def getDecodedString(string):

	if(type(string)!=unicode):
		try:
			string=string.decode('utf-8','strict')
		except UnicodeError:
				try:
					string=string.decode('iso-8859-1','strict')
				except UnicodeError:
					try:
						string=string.decode('windows-1252','strict')
					except UnicodeError:
						return ''
	return string	
	

def isNewUrl(url):     #called by checkUrlInterested() to see if the url exists in the database or not

	row=UrlsTable.row(url)   
	
	if(len(row.keys())==0):	   #it's a new url
		UrlsTable.put(url,{'cf:value': '1'})   #it is mandatory have a column family in hbase
		return True
		
	return False

def getKeywords(url):  #extract the domain name from url, generate all meaningful substrings of it

	name_string = ""
	for index in range(0,len(url)):
		if(url[index] != '.'):
			name_string += url[index]
		else:					 #check if there is a slash before another dot
			indexSecondDot = url[index+1:len(url)].find('.') 
			if(indexSecondDot == -1):  						# type: www.goal.com/
				break
			else:		
				indexSlash = url.find('/')  
				if(indexSlash > indexSecondDot): 			#type:www.index.commoncrawl.org/
					name_string += url[index]
				else:  										 #http://www.espncricinfo.com/australia/content/story/370882.html
					break
	keywords = []
			
	#now generate all strings
	for i in range(0,len(name_string)-2):
		for j in range(2+i,len(name_string)):   
			keywords.append(name_string[i:j+1])
			
	return keywords
		
				
		
def addToDict(word_dict,html,fancyhitBit):			#this method tokenizes the htmldoc and adds tokens to dictionary with their frequency

	stops = set(stopwords.words("english")) 		#used set to avoid linear search
	html = getDecodedString(html)
	all_tokens = nltk.word_tokenize(html)
	
	for token in all_tokens:
		token = token.lower()
		if(len(token)<3 or token in stops):
			continue
			
		token = stem(token)
		isPresent = word_dict.get(token,False)
		if(isPresent):  
			updatedValue = isPresent[0]+1
			word_dict[token] = [updatedValue,fancyhitBit]
			
		else:
			word_dict[token] = [1,fancyhitBit]
		
	
def getFancyHits(html_content):	 #returns a list containing title,keywords,description (in same order)

	soup=BeautifulSoup(html_content,'html.parser')
	fancyHits = []
	
	#Text can be in title and meta tags.	
	if(soup.find("title") is not None):
		fancyHits.append(soup.find("title").string)
		
	if(soup.find("meta") is not None):
	
		if(soup.find("meta",attrs = {'name':'keywords'})):
			keywords = soup.find("meta",attrs =  {'name':'keywords'})['content']
			fancyHits.append(keywords)
			
		if(soup.find("meta",attrs = {'name':'description'})):	
			description = soup.find("meta",attrs = {'name':'description'})['content']
			fancyHits.append(description)
			
	return fancyHits	
	
	
def getText(html_content):  #extract text from webpages

	soup=BeautifulSoup(html_content,'html.parser')
	h = html2text.HTML2Text()
	h.ignore_links = True
	h.google_doc = True	
	h.ignore_images = True
	
	try:
		return h.handle(soup.prettify())	
	except ValueError:    #rare cases
		return ''


def generateDocId(url):  #generating a salted id for document

	return url[:10]+ip_addr+str(time.time())

def checkUrlInterested(line):   #check if url is of domain mentioned in domain_lists

	url = line[len(urlCheck):].strip()
	
	if(url.startswith('https://')):
		url=url[8:len(url)]
		
	else: #starts with http
		url=url[7:len(url)]
		
	if(url.startswith('www.')):
		url=url[4:len(url)]
		
	if(url.endswith('/')):   #remove trailing '/' if present
		url=url[:len(url)-1]
		
	for domain in domain_list:
	
		if(url.startswith(domain) and isNewUrl(url)):
			return url
			
	return ''
	
	
def getContentLength(line):     #called when line starts with 'Content Length:' 

	content_length=line[len(contentLengthCheck):].strip()
	
	return int(content_length)

def resetVariables():       #reset variables to parse next html doc present in warc file

	global isReadingHtmlContent,isRespDetected,contentLength,url,html_content
	isReadingHtmlContent=isRespDetected=False
	contentLength=-1
	url=''
	html_content=''	
	
	
def prepareToInsert(url,html_content):   #returns a list of following format: (DocId,url,FancyHits)

	insertList=[]
	docId = generateDocId(url)
	fancyHits=getFancyHits(html_content)
	text=getText(html_content).strip()
	
	#write format: 'docId'	'url' 'fancy hits separated by semicolon'
	fancyHitString=""
	
	for item in fancyHits:
		fancyHitString+=item+";"
		
	fancyHitString=fancyHitString[0:len(fancyHitString)-1]	#remove last semicolon
	
	if(len(text)!=0):   #if there was no problem in getting text
		insertList=[docId,url,fancyHitString,text]
		
	return insertList
	
def prepareToPrint(insertList): 		#insertList=[docId,url,fancyHitString]

	docId,url,fancyhitString,text=insertList
	word_dict=dict()
	
	urlKeywords = getKeywords(url)
	fancyhitString+=';'
	
	for keyword in urlKeywords:
		fancyhitString += keyword + ' '
		
	addToDict(word_dict,text,0)
	addToDict(word_dict,fancyhitString,1)
	
	return word_dict
	
	
for line in fileinput.input():   #read WARC file line by line

	if(isReadingHtmlContent and contentLength>0 and len(url)>0 and isRespDetected):
	
		if(not line.lower().strip().startswith(htmlDocEndCheck)):
			html_content += getDecodedString(line)
			
		else:  #done reading
			html_content = html_content.strip()[html_content.find(htmlDocStart):]
			insertList = prepareToInsert(url,html_content) #insertList=[docId,url,fancyHitString,text]
			
			if(len(insertList)==4):    
				WebPagesTable.put(insertList[0], {'cf:url': insertList[1],'cf:fancyhits': insertList[2]})
				word_dict = prepareToPrint(insertList)
				
				for key in word_dict.keys():
						print "{0}\t{1}\t{2}\t{3}".format(key.encode('utf-8'),word_dict[key][0],word_dict[key][1],insertList[0])
						
			resetVariables()
			
			
	elif(len(url)>0 and contentLength>0 and isRespDetected):
		line_mod = line.lower().strip()
		
		if(line_mod.startswith(htmlRespCheck1) and line_mod.endswith(htmlRespCheck2)):    #'HTTP/1.1 200 OK' detected
			isReadingHtmlContent=True
			html_content+=line
		
	elif(contentLength>0 and isRespDetected):  
	
		if(line.lower().startswith(urlCheck)):  #'WARC-Target-URI: http...' detected
			url=checkUrlInterested(line)
			
			if(len(url)==0): #url is not of interest
				resetVariables()
				
	elif(isRespDetected):  
	
		if(line.lower().startswith(contentLengthCheck)):   #'Content-Length: 18641' detected
			contentLength=getContentLength(line)
		
	elif(line.lower().startswith(respCheck)):  #'WARC-Type: response' has been detected
		isRespDetected=True
		
connection.close()
																
		

	
