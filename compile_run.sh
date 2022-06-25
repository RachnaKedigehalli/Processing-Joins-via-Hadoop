#!/bin/bash

filename=$1
rm Query/*
javac -classpath ${HADOOP_CLASSPATH} -d Query/ QueryAllURL.java QueryAnyURL.java QuerySomeURL.java QueryFileInputFormat.java QueryRecordReader.java
jar -cvf Query.jar -C Query/ .
hdfs dfs -rm -r -f /project/output/output_${filename}
hdfs dfs -rm -r -f /project/temp
hadoop jar Query.jar Query${filename}URL /project/input/Wikipedia-EN-20120601_KEYWORDS.TSV /project/input/Wikipedia-EN-20120601_REVISION_URIS.TSV /project/temp /project/output/output_${filename}
hdfs dfs -rm -r -f /project/temp
echo "Finished execution"
