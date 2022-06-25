import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class QueryRecordReader extends RecordReader<Text, Text> {

    // It’s a builtin class that split each file line by line
    LineRecordReader lineRecordReader;
    Text key = new Text();
    Text value = new Text();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        lineRecordReader = new LineRecordReader();
        lineRecordReader.initialize(inputSplit, context);
    }

    // It’s the method that will be used to transform the line into key value
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!lineRecordReader.nextKeyValue()) {
            return false;
        }
        String line = lineRecordReader.getCurrentValue().toString();
        String[] keyValue = line.split("\t");
        key.set(keyValue[0]);
        value.set(keyValue[1]);

	//System.out.println("PRINTTAG: Key in record reader: " + key);
        //System.out.println("PRINTTAG: Value in record reader: " + value);

	// String[] keyFields = keyValue[0].split(" ");
        // valueFields = keyValue[1].split(" ");

        // key = new CustomDate(keyFields[0],keyFields[1],keyFields[2]);
        // value = new CityTempurature(valueFields[0],valueFields[1]);
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }
}
