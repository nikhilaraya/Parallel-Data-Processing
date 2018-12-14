package HW4.MR.KMeans;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class KMeans extends Configured implements Tool{
	
	private static final Logger logger = LogManager.getLogger(KMeans.class);

	public static void main(String[] args) {
		if(args.length!=4) {
			throw new Error("Three arguments are required specifying the input and output directory and k");
		}
		try {
			ToolRunner.run(new KMeans(), args);
		}catch(final Exception e) {
			logger.error("",e);
		}

	}
	
	public static class FollowerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable oneFollower = new IntWritable(1);
		private final Text userID = new Text();
		
		//Read each line from the edges.csv file and emit the userId and 1
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			context.write(value, oneFollower);
		}
	
	}
	
	public static class FollowerReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private final IntWritable result = new IntWritable();
		
		// Calculating the number of followers for the userID
		@Override
		public void reduce(final Text key, final Iterable<IntWritable> values,final Context context) throws IOException, InterruptedException{
			int sum = 0;
			for( final IntWritable val: values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public int run(String[] args) throws Exception {
		final Configuration conf = getConf();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		// Adding the k value to the configuration
		conf.set("KValue", args[2]);
		final Job job = Job.getInstance(conf,"K Means");
		job.setJarByClass(KMeans.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

		/*
		 * Job 1: to read the  edges.csv file and find the follower count. The map
		 *  functions takes each line as input and emits the userID and count 1.The reducer
		 *   function returns the number of followers for each userId.
		 */
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapperClass(FollowerMapper.class);
		job.setCombinerClass(FollowerReducer.class);
		job.setReducerClass(FollowerReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		int jobComp =  job.waitForCompletion(true) ? 0 : 1;
		
		if(jobComp == 0) {
			/*
			 * Job 2: generates the initial centroids based on the user input value for k.
			 *  The map function reads from the output file of job1 and emits the number of
			 *   followers to the reducer which calculates the minimum and maximum number of
			 *    follower counts and writes the initial centroids to the file
			 */
		final Job job1 = Job.getInstance(conf,"K Means");
		job1.setJarByClass(KMeans.class);
		final Configuration job1Conf = job1.getConfiguration();
		job1Conf.set("mapreduce.output.textoutputformat.separator", ",");
		job1.setInputFormatClass(KeyValueTextInputFormat.class);
		job1.setMapperClass(MaxFollowerMapper.class);
		job1.setReducerClass(MaxFollowerReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[3]+"_0"));
		
		int jobComp1 =  job1.waitForCompletion(true) ? 0 : 1;
		
		if(jobComp1 == 0) {
			boolean iterate = true;
			int iteration = 1;
			long sse = 0;
			while(iterate && (iteration<=10)) {
				/*
				 * Job 3: The centroids from the previous iteration are read from the 
				 * cache and the closest centroids are emitted with the corresponding object.
				 * The reduce method calculates the new centroid and the SSE
				 */
				conf.set("centroidDir", args[3]+"_"+(iteration-1)+"/");
				final Job job2 = Job.getInstance(conf, "KMeans");
				job2.setJarByClass(KMeans.class);
				final Configuration job2Conf = job2.getConfiguration();
				job2Conf.set("mapreduce.output.textoutputformat.separator", ",");
				job2.setInputFormatClass(KeyValueTextInputFormat.class);
				
				FileInputFormat.setInputPaths(job2, new Path(args[1]));
				TextOutputFormat.setOutputPath(job2, new Path(args[3]+"_"+iteration));
				
				//job2.addCacheFile(new Path(args[3]+"_"+(iteration-1)+"/part-r-00000").toUri());
				
				FileSystem fs = FileSystem.get(new Path(args[3]+"_"+(iteration-1)+"/").toUri(),conf); 
			    RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(args[3]+"_"+(iteration-1)+"/"), true);
			    while(fileStatusListIterator.hasNext()){
			        LocatedFileStatus fileStatus = fileStatusListIterator.next();
			        job2.addCacheFile(fileStatus.getPath().toUri());
			    }
			    
				job2.setMapperClass(KMeansMapper.class);
				job2.setReducerClass(KMeansReducer.class);
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);
				int job2Status = job2.waitForCompletion(true) ? 0 : 1;
				
				if(job2Status == 0) {
					long calcSSE = job2.getCounters().findCounter(SSE.globalSSE).getValue();
					if(iteration == 1) {
						sse = calcSSE;
						iterate = true;
						iteration++;
					}else {
					if((sse-calcSSE)>0.01) {
						iterate = true;
						iteration++;
						sse = calcSSE;
					}else {
						iterate = false;
					}
				}
				}
			}
			
		}
		}
		return jobComp;
	}
	
	public static class KMeansMapper extends Mapper<Object, Text, Text, Text>{
		private ArrayList<String> centroid = new ArrayList<String>();  
		private Text kValue = new Text();
		private Text outkey = new Text();
		
		@Override
		public void setup(Context context) throws IOException{
			String centroidDir = context.getConfiguration().get("centroidDir");
			URI[] uris = context.getCacheFiles();
			if (uris == null || uris.length == 0) {
				throw new RuntimeException(
						"Centroids file is not set in DistributedCache");
			}
			// Reading from cache file and adding to the hashmap
			for (int i = 0; i < uris.length; i++) {
				FileSystem fs = FileSystem.get(new Path(centroidDir).toUri(), context.getConfiguration());
				Path path = new Path(uris[i].toString());
				BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(path)));
				String line;
				while((line=rdr.readLine())!=null) {
					String splitCtr = line.split(",")[0];
					//Adding the centroids to the centroid array
					centroid.add(splitCtr);
				}
		}
		}
		
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			kValue = new Text(context.getConfiguration().get("KValue"));
			int k = Integer.parseInt(kValue.toString());
			String closestCenter = centroid.get(0);
			// calculating the nearest centroid for the v value by iterating through all the 
			// values in the centroid
			double minDist = dist(closestCenter,value.toString());			
			for(int i=1;i<centroid.size();i++) {
				if(dist(centroid.get(i),value.toString())< minDist){
					closestCenter = centroid.get(i);
					minDist = dist(centroid.get(i),value.toString());
				}
			}			
			// emiting the closest centroid for the value
			outkey.set(closestCenter);
			context.write(outkey, value);
		}
	}
	
	public static class KMeansReducer extends Reducer<Text, Text, Text, Text>{
		private final IntWritable result = new IntWritable();
		private Text outkey = new Text();
		private Text outvalue = new Text();
		@Override
		public void reduce(final Text key, final Iterable<Text> values,final Context context) throws IOException, InterruptedException{
			double sum = 0;
			int count = 0;
			double localSSE = 0;
			Double newCentroid = 0.0;
			
			// Calculating the new centroid using the sum and count
			// Calculating the local SSE for the centroid (key)
			for(Text val: values) {
				sum = sum + Integer.parseInt(val.toString());
				count = count+1;
				localSSE = localSSE+dist(key.toString(), val.toString());
			}
			newCentroid = (double) (sum/count);
			outkey.set(newCentroid.toString());
			outvalue.set(new Double(localSSE).toString());
			
			// Accessing the global SSE value and updating it with the localSSE
			long sse = context.getCounter(SSE.globalSSE).getValue();
			sse = (long) (sse+localSSE);
			context.getCounter(SSE.globalSSE).setValue(sse);
			context.write(outkey,outvalue);
		}
	}
	
	/*
	 * Method to return the distance 
	 */
	public static double dist(String ctr,String obj) {	
		double ct = Double.parseDouble(ctr);
		double ob = Double.parseDouble(obj);
		return Math.abs(ct-ob);
	}
	
	/*
	 * Mapper class to find out the maximum and minimum number of follower count
	 */
	public static class MaxFollowerMapper extends Mapper<Object, Text, Text, Text>{	
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {			
			context.write(new Text("dummy"),value);
		}
	}
	
	public static class MaxFollowerReducer extends Reducer<Text, Text, Text, Text>{
		private Text kValue = new Text();
		
		@Override
		public void reduce(final Text key, final Iterable<Text> values,final Context context) throws IOException, InterruptedException{
			kValue = new Text(context.getConfiguration().get("KValue"));
			Integer k = Integer.parseInt(kValue.toString());
			Integer minValue = Integer.MAX_VALUE;
			Integer maxValue = Integer.MIN_VALUE;
			
			for(Text txt: values) {
				Integer val = Integer.parseInt(txt.toString());
				if(maxValue< val){
					maxValue = val;
				}
				if(minValue > val) {
					minValue = val;
				}
			}
			
//			double distribution = (maxValue-minValue)/k;
//			Double newVal =0.0;
//			for(int i=1;i<=k;i++) {
//				if(i==1) {
//					newVal = minValue+distribution;
//				}else{
//					newVal = newVal+distribution;
//				}				
//				context.write(new Text(newVal.toString()),new Text("centroid"));
//			}
//			=====================bad distribution========================================
			Double newVal =0.0;
			for(int i=1;i<=k;i++) {
				if(i==1) {
					newVal = maxValue-100.0;
				}else{
					newVal = newVal-100;
				}				
				context.write(new Text(newVal.toString()),new Text("centroid"));
			}
		}
	}
	
	
	

}

