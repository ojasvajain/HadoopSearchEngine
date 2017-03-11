import nltk
from nltk.corpus import stopwords
from stemming.porter2 import stem

'''
Contains getRelevantResults() that contains the page ranking algoritm.
'''


class Result:    				#Each object represents the search result as displayed on the webpage. 

	title = ""
	url = ""
	
	def __init__(self,docId,Table_Webpages):
		docInfo = Table_Webpages.row(docId , columns = ['cf:fancyhits' , 'cf:url'])    #querying DB to find url & fancyhits using docid
		fancyhits = docInfo['cf:fancyhits'].strip().split(';')   #fancyhits format: title ; keywords ; domain name permutations
		
		self.url = 'http://' + docInfo['cf:url']
		self.title = fancyhits[0]	
	
	
		
def getKeywords(searchQuery):      #applies tokenization and stemming on the search query and returns a list of keywordss

	keywords = []
	stops = set(stopwords.words("english"))       #stop words removal
    all_tokens = nltk.word_tokenize(searchQuery)    #tokenization
    
    for token in all_tokens:
		token = token.lower()
		if(len(token)<3 or token in stops):
			continue
		if(len(token)<3 or token in stops):
			continue
		token = stem(token)                         #stemming
		keywords.append(token)
		
	return keywords
        
def calculateScore(freqs):    #freq string format: x1,x2). x1 = number of normal hits. x2 = fancyhit bit.

	FH_SCORE = 25 			      #To give more weightage to fancyhit, this number is multiplied to fancyhit bit.
	freqs = freqs.translate(None,')')
	values = freqs.strip().split(',')
	
	return int(values[0]) + int(values[1])*FH_SCORE                

def totalScore(keyword_dicts,currentId):   #keyword_dicts: list of dictionaries, one for each keyword. currentId is the docId whose total score is to be found

	totalScore=0
	
	for keyword_dict in keyword_dicts:
		score = int(keyword_dict.get(currentId,0)) 
		totalScore += score
		
	return totalScore
	
def getRelevantResults(searchQuery,Table_InvertedIndex,Table_Webpages,numResults): #arguments: searchQuery entered, hbase table object of InvertedIndex, hbase table of Webpages, maximum number of results to be displayed
 
	keywords = getKeywords(searchQuery)
	keyword_dicts = [dict() for i in range(len(keywords))]  #In each dict, key = docid, value = score. One dict for each keyword.
        
	for i in range(0,len(keywords)): 
   				
		postings = Table_InvertedIndex.row(keywords[i])   #returs a dictionary, key = posting_no, value = posting_string
		            
		for key in postings.keys(): 			#each key corresponds to a set of postings
		            
			docid = ""
			freqs = ""
			docIdEnd = False
			
			for j in range(0,len(postings[key])):	#go through postings string, character by character so that we can detect end of docid. 
				c = postings[key][j]
				   		
				if(c == '('):
					docIdEnd = True
					
				elif(c=='$' or j==(len(postings[key])-1)):
					keyword_dicts[i][docid] = calculateScore(freqs)
					
					docid = ""                      #reset variables to start reading next docid
					freqs = ""
					docIdEnd = False
					
				elif(docIdEnd == False):
					docid += c
					
				elif(docIdEnd == True):
					freqs += c
				
	keyword_dicts = sorted(keyword_dicts,key=lambda k: len(k.keys()))   #sorted dicts in increasing order of number of keys to decrease number of intersection operations 
       	
       	 
	relevantList=keyword_dicts[0].keys()            #relevantList would contain docids of docs having all keywords
	
	for i in range (1,len(keyword_dicts)):
		keyword_dict = keyword_dicts[i]
       	 	
      	 	if(len(keyword_dict)==0):  #would make the intersection as null, so ignore it
       	 		continue
       	 		
      		relevantList = list(set(relevantList).intersection(keyword_dict.keys()))    #performing intersection
      		
      	 
	numResults = min(numResults,len(relevantList))    #numResults passed as arguments can be more than relevant docs found
	
	
	for i in range(0,numResults-1):    #applying selection sort to find top 'numResults' results in relevantList
		maxItemIndex = i
		for j in range(i+1,len(relevantList)):
			currentId = relevantList[j]
			if(totalScore(keyword_dicts,currentId) > totalScore(keyword_dicts,relevantList[maxItemIndex])):
				maxItemIndex = j
		relevantList[maxItemIndex],relevantList[i] = relevantList[i],relevantList[maxItemIndex]
		
		
	results = []    #list of Result objects
	for i in  range(0,numResults):
			results.append(Result(relevantList[i],Table_Webpages))
			
	return results
