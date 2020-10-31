import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Maximum_age {
	static HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();

	public static class Age_Mapper extends Mapper<LongWritable, Text, Text, Text> {

//		static HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			String inputpath = "userdata.txt";
			Path path = new Path(inputpath);
			FileSystem fsystem = FileSystem.get(conf);
			FileStatus[] fstatus = fsystem.listStatus(path);
			Calendar today = Calendar.getInstance();
			int curYear = today.get(Calendar.YEAR);
			for (FileStatus status : fstatus) {
				Path pt = status.getPath();
				BufferedReader br = new BufferedReader(new InputStreamReader(fsystem.open(pt)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] info = line.split(",");
					if (info.length == 10) {
					SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
						Date date = null;
						try {
							date = (Date) sdf.parse(info[9]);
						} catch (ParseException e) {
							e.printStackTrace();
						}
						Calendar birthday = Calendar.getInstance();
					birthday.setTime(date);
					curYear = today.get(Calendar.YEAR);
					int preYear = birthday.get(Calendar.YEAR);
						int age = curYear - preYear;
						map.put(Integer.parseInt(info[0]), age);
					}
					line = br.readLine();
				}
			}
		}

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String[] friends = values.toString().split("\t");//tuple in file1
			if (friends.length == 2) {
				context.write(new Text(friends[0]), new Text(friends[1]));
					}
		   }
	}
	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//String[] friendslist = values.toString().split(",");
			int max_age = 0;
			for (Text item : values) {
				String[] friendsList = item.toString().split(",");
				for (String id : friendsList) {
					int age = map.get(Integer.parseInt(id));
					max_age = Math.max(max_age, age);
				}
			}
			context.write(key, new Text(String.valueOf(max_age)));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//Usage: <Common friends input file path> <user_data file path> <output_path> <User-ID1> <User-ID2>


		Job job1 = Job.getInstance(conf, "Mutual-Friends of userA and userB");

		job1.setJarByClass(Friends_Information.class);
		job1.setMapperClass(Age_Mapper.class);
		job1.setReducerClass(ReducerClass.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		String outputPath = "question4";
		FileInputFormat.addInputPath(job1,new Path("soc-LiveJournal1Adj.txt"));
		FileOutputFormat.setOutputPath(job1, new Path(outputPath));

		FileUtils.deleteDirectory(new File(outputPath));
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}

}
