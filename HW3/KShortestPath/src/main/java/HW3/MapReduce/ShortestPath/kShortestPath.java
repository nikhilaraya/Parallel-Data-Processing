package HW3.MapReduce.ShortestPath;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;




public class kShortestPath extends Configured implements Tool {

	private static final Logger logger = LogManager.getLogger(kShortestPath.class);
	private static String targetUser = "-1";
	
	public static void main(String[] args) {
		if(args.length != 4) {
			throw new Error("four arguments are required specifying the input,intermediate and output directory and iteration");
		}
		try {
			ToolRunner.run(new kShortestPath(), args);
		} catch(final Exception e) {
			logger.error("",e);
		}

	}

	public int run(String[] args) throws Exception {
		final Configuration conf = getConf();
		// Setting the input in the key value format by splitting them on ',' delimiter
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
		final Job job = Job.getInstance(conf, "kShortestPath");
		job.setJarByClass(kShortestPath.class);
		final Configuration jobConf = job.getConfiguration();
		// Setting the output in the key value format by splitting them on ',' delimiter
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		
//		// deleting the existing output directory
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		// Setting the path for input directory
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(AdjacencyListMapper.class);
		job.setReducerClass(AdjacencyListReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		
		// generate random number
		//int randomUserId = new Random().ints(1,4).limit(1).toArray()[0];
		int randomUserId = 3;
		targetUser =  new Integer(randomUserId).toString();
		int job1Status = job.waitForCompletion(true) ? 0 : 1;
		int iterationNum = 0;
		String dynamicOutputDir = "";
		if(job1Status == 0) {
			// deleting the existing output directory
//			final FileSystem fileSystemiteration = FileSystem.get(conf);
//			if (fileSystem.exists(new Path("iteration"))) {
//				fileSystem.delete(new Path("iteration"), true);
//			}
			boolean iterate = true;
			String dynamicInputDir = "";
			dynamicOutputDir=args[1];
			int i=0;
			while(iterate) {
				
				dynamicInputDir = dynamicOutputDir;
				dynamicOutputDir = args[3]+"_"+iterationNum;
				
				final Job job2 = Job.getInstance(conf, "kShortestPath");
				job2.setJarByClass(kShortestPath.class);
				final Configuration job2Conf = job2.getConfiguration();
				// Setting the output in the key value format by splitting them on ',' delimiter
				job2Conf.set("mapreduce.output.textoutputformat.separator", ",");
				job2.setInputFormatClass(KeyValueTextInputFormat.class);
				
				FileInputFormat.addInputPath(job2, new Path(dynamicInputDir));
				FileOutputFormat.setOutputPath(job2, new Path(dynamicOutputDir));	
				job2.setMapperClass(ShortestPathMapper.class);
				job2.setReducerClass(ShortestPathReducer.class);
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);
				int job2Status = job2.waitForCompletion(true) ? 0 : 1;
				long updateCounter = 0;
				if(job2Status == 0) {
					updateCounter=job2.getCounters().findCounter(IterationsUpdateCounter.UpdateCounter).getValue();				
				}
				if(updateCounter > 0) {
					iterate = true;
					job2.getCounters().findCounter(IterationsUpdateCounter.UpdateCounter).setValue(0);
					iterationNum = iterationNum+1;
				}else {
					iterate =false;
				}
			}
			
			conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
			final Job job3 = Job.getInstance(conf, "kShortestPath");
			job3.setJarByClass(kShortestPath.class);
			final Configuration jobConf3 = job3.getConfiguration();
			job3.setInputFormatClass(KeyValueTextInputFormat.class);
			
			// Setting the path for input directory
			FileInputFormat.addInputPath(job3, new Path(dynamicOutputDir));
			FileOutputFormat.setOutputPath(job3, new Path(args[2]));
			job3.setMapperClass(LongestComputeMapper.class);
			job3.setReducerClass(LongestComputeReducer.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			return job3.waitForCompletion(true) ? 0 : 1;	
			
		}else {
			return 0;
		}
		
			
	}
	
	
	public static class AdjacencyListMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
		context.write(new Text(key.toString()),value);
		}
	}
	
	public static class AdjacencyListReducer extends Reducer<Text, Text, Text, Text>{
		private Text outvalue = new Text();

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
//			String someVariable = conf.get("SourceUser");
			Integer maxInt = Integer.MAX_VALUE;
			String formatOutput = new String();
			formatOutput = formatOutput+key.toString()+"|";
			for(Text adjacent: values) {
				formatOutput = formatOutput +adjacent.toString()+",";
			}
			int length = formatOutput.length();
			//logger.info("length of the string"+formatOutput.length()+"------"+formatOutput);
			formatOutput = formatOutput.substring(0, length-1);
			
			if(key.toString().equals("200")) {
				formatOutput= formatOutput + "|T"+"|"+"0";
			}else {
				formatOutput= formatOutput + "|F"+"|"+maxInt.toString();
			}
			outvalue.set(formatOutput);
			context.write(key,outvalue);
		}
	}
	
	public static class ShortestPathMapper extends Mapper<Text, Text, Text, Text> {
		Text outVal = new Text();
		Text outKey = new Text();
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
			context.write(key, value);
			String nodeVal = value.toString();
			String[] extractingVals = nodeVal.split("\\|");
			if(extractingVals[2].equals("T")) {
				String distance = extractingVals[3];
				String[] adjNodes = extractingVals[1].split(",");
				for(String adjNode : adjNodes) {
					outKey.set(adjNode);
					outVal.set((Integer.parseInt(distance) +1)+"");
					context.write(outKey,outVal);
				}
			}			
		}
	}
	
	
	public static class ShortestPathReducer extends Reducer<Text, Text, Text, Text>{
		Text outVal = new Text();
		
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			int dMin = Integer.MAX_VALUE;
			String M = "";
			for(Text value: values) {
				String[] objectStr = value.toString().split("\\|");
				if(objectStr.length>1) {
					M = value.toString();
				}else {
					int distance = Integer.parseInt(value.toString());
					if(distance<dMin) {
						dMin = distance;
					}
				}
			}
			
			String[] splitM = M.split("\\|");
			if(splitM.length > 1 && M!="" && (!M.isEmpty())) {
			int mDist = Integer.parseInt(splitM[3]);
			if(dMin < mDist) {
				splitM[3] = dMin+"";
				splitM[2] = "T";
				context.getCounter(IterationsUpdateCounter.UpdateCounter).increment(1);
			}
			M = Arrays.stream(splitM).collect(Collectors.joining("|"));
			}
			if(M.isEmpty())
			{
				M = key.toString()+"|"+"|T|"+ Integer.valueOf(dMin).toString();
			}
			outVal.set(M);
			context.write(key, outVal);
		}
	}
	
	public static class LongestComputeMapper extends Mapper<Object, Text, Text, Text> {
		
		Text outVal = new Text();
		Text outKey = new Text();
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String nodeVal = value.toString();
			String[] extractingVals = nodeVal.split("\\|");
			if(extractingVals.length>1) {
				if(Integer.parseInt(extractingVals[3])!= 2147483647) {
						outKey.set("distances");
						outVal.set(extractingVals[3]);
						context.write(outKey,outVal);
				}
			}
		}
	}
	
	public static class LongestComputeReducer extends Reducer<Text, Text, Text, Text>{
		Text outVal = new Text();
		Text outKey = new Text();
		public void reduce(final Text key, final Iterable<Text> values, final Context context)
				throws IOException, InterruptedException {
			Integer maxDistance = Integer.MIN_VALUE;
			for(Text value: values) {
				Integer presentVal = Integer.parseInt(value.toString());
				if(presentVal > maxDistance) {
					maxDistance = presentVal;
				}
			}
			outKey.set("Longest");
			outVal.set(maxDistance.toString());
			context.write(outKey, outVal);
		}
	}
			

}
