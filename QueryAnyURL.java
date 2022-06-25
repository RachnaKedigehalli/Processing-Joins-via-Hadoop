import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class QueryAnyURL {

	public static class QueryAnyMapper extends Mapper<Text, Text, Text, Text> {


		@Override
		public void map(Text docId, Text text_word, Context context) throws IOException, InterruptedException {
			String word = text_word.toString();
			if (word.equals("infantri") || word.equals("reinforc") || word.equals("brigad") || word.equals("fire")) {
				context.write(docId, text_word);
			}
		}
	}

	public static class QueryAnyReducer extends Reducer<Text, Text, Text, BooleanWritable> {
		private BooleanWritable result = new BooleanWritable();

		@Override
		public void reduce(Text key, Iterable<Text> words, Context context)
				throws IOException, InterruptedException {
			
			Boolean keyword_contained[] = {false, false, false, false};
			for (Text text_word : words) {
				String word = text_word.toString();
				if (word.equals("infantri")) {
					keyword_contained[0] = true;
				}
				else if (word.equals("reinforc")) {
					keyword_contained[1] = true;
				}
				else if (word.equals("brigad")) {
					keyword_contained[2] = true;
				}
				else if (word.equals("fire")) {
					keyword_contained[3] = true;
				}
			}
			result.set(keyword_contained[0] || keyword_contained[1] || keyword_contained[2] || keyword_contained[3]);
			context.write(key, result);
		}
	}

	public static class URLMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text docId, Text url, Context context) throws IOException, InterruptedException {
			context.write(docId, url);
		}
	}

	public static class URLReducer extends Reducer<Text, Text, Text, BooleanWritable> {
		private BooleanWritable result = new BooleanWritable();
		private Text url = new Text(); 
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			Boolean is_present = false;
			for (Text value: values) {
				if (value.toString().equals("true")) {
					is_present = true;
				}
				else if(value.toString().equals("false")) {
				}
				else {
					url.set(value);
				}
			}
			
			if (is_present) {
				result.set(true);
				context.write(url, result);
			}
			else {
				result.set(false);
				context.write(url, result);
			}

		}
	}

	public static void main(String[] args) throws Exception {
		
		// Job 1
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "query1");
		
		job1.setJar("Query.jar");

		job1.setMapperClass(QueryAnyMapper.class);
		//job.setCombinerClass(IntSumReducer.class); // enable to use 'local aggregation'
		job1.setReducerClass(QueryAnyReducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		
		job1.setInputFormatClass(QueryFileInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		
		job1.waitForCompletion(true);

		// Job 2
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "url");
		
		job2.setJar("Query.jar");

		job2.setMapperClass(URLMapper.class);
		//job.setCombinerClass(IntSumReducer.class); // enable to use 'local aggregation'
		job2.setReducerClass(URLReducer.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		
		job2.setInputFormatClass(QueryFileInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job2, new Path(args[1]), QueryFileInputFormat.class, URLMapper.class);
		MultipleInputs.addInputPath(job2, new Path(args[2]), QueryFileInputFormat.class, URLMapper.class);
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
