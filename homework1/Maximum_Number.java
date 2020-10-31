import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class Maximum_Number {

	public static class NumberMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

			String[] items = values.toString().split("\t");
			int num = 0;

			if (items.length == 2) {
				String pair = items[0];
				String[] friendsList = items[1].split(",");
				num  = friendsList.length;
				context.write(new Text("1"), new Text(pair+" "+num));
			}
		}
	}

	public static class NumberReducer extends Reducer<Text, Text, Text, IntWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int result = 0;
			String ans_pair = "";
			for (Text item: values) {
				String a[] = item.toString().split(" ");
				int num = Integer.parseInt(a[1]);
				String pair = a[0];
				if(num>result){
					result = num;
					ans_pair = pair;
				}
			}
			context.write(new Text(ans_pair), new IntWritable(result));
		}
	}
	


	public static void main(String[] args) throws Exception {

		{
			Configuration conf1 = new Configuration();
			Job job1 = Job.getInstance(conf1, "Maximum_number friendship");

			job1.setJarByClass(Maximum_Number.class);
			job1.setMapperClass(NumberMapper.class);
			job1.setReducerClass(NumberReducer.class);

			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);

			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);

			String outputPath = "question2";
			FileInputFormat.addInputPath(job1,new Path("question1/part-r-00000"));
			FileOutputFormat.setOutputPath(job1, new Path(outputPath));

			FileUtils.deleteDirectory(new File(outputPath));

			System.exit(job1.waitForCompletion(true) ? 0 : 1);
			}
		}
	}
