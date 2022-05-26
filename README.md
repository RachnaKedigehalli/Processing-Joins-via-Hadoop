# Processing-Joins-via-Hadoop
Boolean retrieval queries as pairs of map and reduce functions performing reduce-side joins.

### Instruction to run the code:
* Make sure the input files are in hdfs. You can use the following commands to do so:
```
hdfs dfs -mkdir /input
hdfs dfs -copyFromLocal input_file /input
```

* Either run the script compile.sh in the following way:
1. For Query 1: Find URLs of Wikipedia articles that contain all of the stemmed keywords infantri, reinforc, brigad, and fire., command: `./compile_run.sh All`
2. For Query 2: Find URLs of Wikipedia articles that contain any of the stemmed keywords infantri, reinforc, brigad, or fire., command: `./compile_run.sh Any` 
3. For Query 3: Find URLs of Wikipedia articles that contain the stemmed keyword reinforc but not any of the stemmed keywords infantri, brigad, or fire., command: `./compile_run.sh Some`

OR

* Follow the steps given bellow:
1. Compile the files using:
```
Set 
HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)

Run command: 
javac -classpath ${HADOOP_CLASSPATH} -d Query/ QueryAllURL.java QueryAnyURL.java QuerySomeURL.java QueryFileInputFormat.java QueryRecordReader.java
```

2. Create the jar file using:
```
jar -cvf Query.jar -C Query/
```

3. Run hadoop:\
For Query 1: Find URLs of Wikipedia articles that contain all of the stemmed keywords infantri, reinforc, brigad, and fire:
```
hadoop jar Query.jar QueryAllURL input_file1 input_file2 /temp /output/output_All
```
For Query 2: Find URLs of Wikipedia articles that contain any of the stemmed keywords infantri, reinforc, brigad, or fire:
```
hadoop jar Query.jar QueryAnyURL input_file1 input_file2 /temp /output/output_Any
```
For sub-problem 3:
```
hadoop jar Query.jar QuerySomeURL input_file1 input_file2 /temp /output/output_Some
```

4. Since step 4 creates a temporary directory, delete it using:
	hdfs dfs -rm -r -f /temp
