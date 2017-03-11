'''
Flask Web Server
index.html - displays option to enter search query.
Uses PageRanking to get the list and passes the list of result objects to 
results.html - displays results.
'''



from flask import Flask, render_template, request
import os
import happybase
import PageRank   

template_dir = os.path.abspath('templates')
app = Flask(__name__, template_folder=template_dir)


#connect to thrift server and tables
connection = happybase.Connection('172.31.10.32')     #connect to thrift server
Table_InvertedIndex = connection.table('InvertedIndex') 
Table_Webpages = connection.table('Webpages')

@app.route('/')
def index():

    	return render_template('index.html')


@app.route('/results',methods = ['POST','GET'])
def results():

	if request.method == 'POST':
		MAX_RESULTS = 20
		searchQuery=request.form['SearchQuery']   #get text entered 
		results=PageRank.getRelevantResults(searchQuery,Table_InvertedIndex,Table_Webpages,MAX_RESULTS)  
		
		return render_template("results.html",results = results)  #pass list of results for rendering

if __name__ == '__main__':
    	app.run(debug=True,host='0.0.0.0')
