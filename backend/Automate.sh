#!/bin/bash
: '
This script is meant to automate the process of generating WARC files and uploading them on HDFS, which can be then processed on Hadoop cluster. Due to space constraints, the script makes the ContentProduce.py to generate a few files at a time. This script runs on all the datanodes.
'

numFiles=18
pathFile="paths"

python ContentProduce.py $numFiles $pathFile  #This generates WARC files

for file in CC-MAIN*.gz;
do
	echo $file
	hdfs dfs -put $file input_folder/$file   #Upload file one by one
	sudo rm -r $file   #remove it from local
done

