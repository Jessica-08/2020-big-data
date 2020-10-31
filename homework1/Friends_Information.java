import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Friends_Information {

	static HashMap<Integer, String> map = new HashMap<Integer, String>();

	public static class InformationMapper extends Mapper<LongWritable, Text, Text, Text> {


		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			String inputpath = "userdata.txt";
			Path path = new Path(inputpath);
			FileSystem fsystem = FileSystem.get(conf);
			FileStatus[] fstatus = fsystem.listStatus(path);
			for (FileStatus status : fstatus) {
				Path pt = status.getPath();
				BufferedReader br = new BufferedReader(new InputStreamReader(fsystem.open(pt)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] info = line.split(",");
					if (info.length == 10) {
						map.put(Integer.parseInt(info[0]), info[1] + ":" + info[info.length - 1]);
					}
					line = br.readLine();
				}
			}
		}

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int input_friend1 = Integer.parseInt(conf.get("InputFriend1"));
			int input_friend2 = Integer.parseInt(conf.get("InputFriend2"));
			String[] friends = values.toString().split("\t");//tuple in file1
			if (friends.length == 2) {
				int friendID = Integer.parseInt(friends[0]);
				Text item_key = new Text(input_friend1 + " " + input_friend2);
				if(friendID == input_friend1||friendID == input_friend2){
					context.write(item_key, new Text(friends[1]));
				}
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashSet<String> mutualFriend = new HashSet<>();
			Text mutualInfo = new Text();
			String ans = new String();
			for(Text items:values){
				String[] friendsList = items.toString().split(",");
				for (String friend : friendsList) {
					if(mutualFriend.contains(friend)){
						ans+=map.get(Integer.parseInt(friend));
					} else {
						mutualFriend.add(friend);
					}
				}
			}
			mutualInfo.set(new Text(ans));
			context.write(key, mutualInfo);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//Usage: <Common friends input file path> <user_data file path> <output_path> <User-ID1> <User-ID2>

		conf.set("InputFriend1", "5");
		conf.set("InputFriend2", "6");
		conf.set("Data",String.valueOf(new Path("userdata.txt")));

		Job job1 = Job.getInstance(conf, "Mutual-Friends of userA and userB");

		job1.setJarByClass(Friends_Information.class);
		job1.setMapperClass(InformationMapper.class);
		job1.setReducerClass(ReducerClass.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

//		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
//		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));
		String outputPath = "question3";
		FileInputFormat.addInputPath(job1,new Path("soc-LiveJournal1Adj.txt"));
		FileOutputFormat.setOutputPath(job1, new Path(outputPath));

		FileUtils.deleteDirectory(new File(outputPath));

		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}

}
