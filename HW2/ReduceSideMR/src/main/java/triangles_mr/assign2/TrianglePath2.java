package triangles_mr.assign2;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class TrianglePath2 extends Configured implements Tool 
{
	private static final Logger logger = LogManager.getLogger(TrianglePath2.class);
    public static void main( String[] args )
    {
    	if(args.length != 2) {
			throw new Error("Two arguments are required specifying the input and output directory");
		}
		try {
			ToolRunner.run(new TrianglePath2(), args);
		} catch(final Exception e) {
			logger.error("",e);
		}
    }

	public int run(String[] args) throws Exception {
		final Configuration conf = getConf();
		// Setting the Max value as 30000
		conf.set("Max_value", "1000");
		// Setting the input in the key value format by splitting them on ',' delimiter
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
		final Job job = Job.getInstance(conf, "TrianglePath2");
		job.setJarByClass(TrianglePath2.class);
		final Configuration jobConf = job.getConfiguration();
		// Setting the output in the key value format by splitting them on ',' delimiter
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		
		// deleting the existing output directory
		final FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(new Path(args[1]))) {
			fileSystem.delete(new Path(args[1]), true);
		}
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		// Setting the path for input directory
		MultipleInputs.addInputPath(job, new Path(args[0]),
				KeyValueTextInputFormat.class, EdgesMapper.class);
		
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class EdgesMapper extends Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();

		private Text maxV = new Text();
		
		// Method to get the max value from the configuration
		public void setup(Context context) {
			maxV = new Text(context.getConfiguration().get("Max_value"));	
		}
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// Check for max value on key and value
			if(Integer.parseInt(key.toString()) < Integer.parseInt(maxV.toString())  
					&& Integer.parseInt(value.toString()) < Integer.parseInt(maxV.toString()) ) {
			
			// 'A' table with the value as key (to), the record with flag A as the value 
			outkey.set(value);
			outvalue.set("A,"+key.toString()+","+value);
			context.write(outkey, outvalue);
			
			// 'B' table with the key as key (from), the record with flag B as the value 
			outkey.set(key.toString());			
			outvalue.set("B," + key.toString()+","+value);
			context.write(outkey, outvalue);
			}
		}
	}
	
	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text>{
		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();
		
		
		@Override
		public void reduce(final Text key, final Iterable<Text> values,final Context context) throws IOException, InterruptedException{
			// Clear out the lists
			listA.clear();
			listB.clear();

			// Iterating through all the values of a key
			for (Text t : values) {
				// Check if the input is from table A
				if (t.charAt(0) == 'A') {
					// Removing the flag A and adding to the list
					Text Strim = new Text(t.toString().substring(2).split(",")[0]);
					listA.add(Strim);
				} 
				// Check if the input is from table B
				else if (t.charAt(0) == 'B') {
					// Removing the flag B and adding to the list
					Text trimming = new Text(t.toString().substring(2).split(",")[1]);
					listB.add(trimming);
				}
			}
			// Joining the two record which do not have the same from and to ex: 1->2->1 is removed in the if condition below
			if (!listA.isEmpty() && !listB.isEmpty()) {
				for (Text A : listA) {
					for (Text B : listB) {
						if(A.compareTo(B) != 0)
							context.write(A, B);
					}
				}
			}
		}
	}
}
