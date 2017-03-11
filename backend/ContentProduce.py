#!/usr/bin/env python

'''Execute in the following way: 'python ContentProduce.py 2 paths.txt'    

This script reads the paths file line by line. Each line is a url to a WARC file. It downloades the specified number of files and stores the url of next file in the NextPath file. So the next time script is run, the script contiues from the url contained in NextPath file.

'''

import fileinput
import wget
import sys
import os

nextPath = open("NextPath","r").read().strip()   #NextPath file contains the path of next file to be downloaded 
nextPath_write = open("NextPath","w")
nextPathReached = False

count = 0
writeNextLine = 0
maxFiles = int(sys.argv[1])      #Each file is of approx 1GB. 

fileName = sys.argv[2]           #This file contains the paths from where to download the files
pathFile = open(fileName,'r')

for line in pathFile.readlines():   

	line = line.strip()
	
	if(writeNextLine==1):
		nextPath_write.write(line)
		writeNextLine = 0
		break
	
	if(nextPathReached == False):
		if(line == nextPath):
			nextPathReached = True
		else:
			continue
			
	if(nextPathReached == True):   
		url = "https://commoncrawl.s3.amazonaws.com/" + line
		gzipfile = wget.download(url)
		count += 1
		if(count == maxFiles):
			writeNextLine = 1
		
if(writeNextLine==1):
	nextPath.write("FINISH")		
	
nextPath_write.close()
		
			
