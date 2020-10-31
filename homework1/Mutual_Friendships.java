import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Mutual_Friendships{

	public static class Mutual_FriendshipsMapper extends Mapper<LongWritable, Text, Text, Text> {

			public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

				String[] items = values.toString().split("\t");//一行行的数据

				if (items.length == 2) {

					int friend1ID = Integer.parseInt(items[0]); //ID
					int friend2ID;
					Text item_key = new Text();	
					String[] friendsList = items[1].split(",");	//friend of this ID			
					for (String friend2 : friendsList) {
						friend2ID = Integer.parseInt(friend2);
						if((friend1ID==0 && friend2ID ==1 )||(friend1ID==1 && friend2ID ==0 )||(friend1ID==20 && friend2ID ==28193 )||(friend1ID==28193 && friend2ID ==20 )||(friend1ID==1 && friend2ID ==29826 )||(friend1ID==29826 && friend2ID ==1 )||(friend1ID==6222 && friend2ID ==19272 )||(friend1ID==19272 && friend2ID ==6222 )||(friend1ID==28041 && friend2ID == 28056)||(friend1ID==28056 && friend2ID ==28041)){
							if (friend1ID < friend2ID) {
								item_key.set(friend1ID + "," + friend2ID);
							} else {
								item_key.set(friend2ID + "," + friend1ID);
							}
							context.write(item_key, new Text(items[1]));//[ID1,ID1's friend][ID2,ID2's friend]
						}
					}
				}
			}
	}

  public static class Mutual_FriendshipsReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> Map = new HashMap<String,Integer>();
			StringBuilder sb = new StringBuilder();
			Text mutualFriend = new Text();
			for (Text item : values) {
				String[] friendsList = item.toString().split(",");
				for (String friend : friendsList) {
					if(Map.containsKey(friend)){
						sb.append(friend+",");
					}else {
						Map.put(friend, 1);
					}
				}
			}
			if(sb.length()>0) {
				sb.deleteCharAt(sb.length()-1);
			}
			mutualFriend.set(new Text(sb.toString()));
			context.write(key, mutualFriend);
		}

	}


  public static void main(String[] args) throws Exception{

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Mutual Friends");

		job.setJarByClass(Mutual_Friendships.class);
		job.setMapperClass(Mutual_FriendshipsMapper.class);
		job.setReducerClass(Mutual_FriendshipsReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		String outputPath = "question1";
		FileInputFormat.addInputPath(job,new Path("soc-LiveJournal1Adj.txt"));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		FileUtils.deleteDirectory(new File(outputPath));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}