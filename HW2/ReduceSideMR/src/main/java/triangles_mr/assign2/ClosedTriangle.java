package triangles_mr.assign2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class ClosedTriangle extends Configured implements Tool {

	private static final Logger logger = LogManager.getLogger(ClosedTriangle.class);
	public static void main(String[] args) {
		if(args.length != 3) {
			throw new Error("Two arguments are required specifying the input and output directory");
		}
		try {
			ToolRunner.run(new ClosedTriangle(), args);
		} catch(final Exception e) {
			logger.error("",e);
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		final Configuration conf = getConf();
		// Setting the input in the key value format by splitting them on ',' delimiter
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
		// Setting the Max value as 30000
		conf.set("Max_value", "1000");
		
		final Job job = Job.getInstance(conf, "ClosedTriangle");
		
		job.setJarByClass(ClosedTriangle.class);
		final Configuration jobConf = job.getConfiguration();
		// Setting the output in the key value format by splitting them on '\t' delimiter
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[2]))) {
//			fileSystem.delete(new Path(args[2]), true);
//		}
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		// Setting the path for input directory one is the output of the path 2 length job and the other is the edges.csv
		MultipleInputs.addInputPath(job, new Path(args[0]),
				KeyValueTextInputFormat.class, Path2Mapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				KeyValueTextInputFormat.class, EdgesMapper.class);
	
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		int jobCompletion = job.waitForCompletion(true) ? 0 : 1;
		// Getting the Global Counter value from the enum ReduceCounter
		if(jobCompletion == 0) {
			long cn=job.getCounters().findCounter(ReduceCounter.Triangle_Count).getValue();
			logger.info("--------------NO OF TRIANGLES---------"+(cn/3));
		}
		return jobCompletion;
	}
	
	public static class EdgesMapper extends Mapper<Text, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();
		
		private Text maxV = new Text();
		
		// Method to get the max value from the configuration
		public void setup(Context context) {
			maxV = new Text(context.getConfiguration().get("Max_value"));	
		}

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			// Check for max value on key and value
			if(Integer.parseInt(key.toString()) < Integer.parseInt(maxV.toString())  
					&& Integer.parseInt(value.toString()) < Integer.parseInt(maxV.toString()) ) {
			
			outkey.set(key.toString());
			
			outvalue.set("B," +value);
			context.write(outkey, outvalue);
		}
		}
	}
	
	public static class Path2Mapper extends Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			outkey.set(value);

			outvalue.set("A,"+key.toString());
			context.write(outkey, outvalue);
		}
	}
	
	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text>{
		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();
		
		
		@Override
		public void reduce(final Text key, final Iterable<Text> values,final Context context) throws IOException, InterruptedException{
			// Clear our lists
			listA.clear();
			listB.clear();
			// Iterating through all the values of a key
			for (Text t : values) {
				// Check if the input is from table A
				if (t.charAt(0) == 'A') {
					// Removing the flag A and adding to the list
					Text Strim = new Text(t.toString().substring(2));
					listA.add(Strim);
				} 
				// Check if the input is from table B
				else if (t.charAt(0) == 'B') {
					// Removing the flag B and adding to the list
					Text trimming = new Text(t.toString().substring(2));
					listB.add(trimming);
				}
			}
			if (!listA.isEmpty() && !listB.isEmpty()) {
				for (Text A : listA) {
					for (Text B : listB) {
						// Check for closing edge and incrementing the global count
						if(A.compareTo(B) == 0) {
							context.getCounter(ReduceCounter.Triangle_Count).increment(1);
							context.write(A,B);
						}

					}
				}
			}
		}
	}

}
