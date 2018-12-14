package triangles.path2.replicationJoin;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ReplicationJoinPath {

	private static final Logger logger = LogManager.getLogger(ReplicationJoinPath.class);
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("Max_value", "1000");
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
		
		if(args.length != 2) {
			throw new Error("Two arguments are required specifying the input and output directory");
		}
		final FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(new Path(args[1]))) {
			fileSystem.delete(new Path(args[1]), true);
		}
		
		Job job = new Job(conf, "Replicated Join");
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		job.setJarByClass(ReplicationJoinPath.class);

		job.setMapperClass(ReplicatedJoinMapper.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// Adding the file to cache
		job.addCacheFile(new Path(args[0]+"/sampleMaxFilter.csv").toUri());
		int jobCompletion = job.waitForCompletion(true) ? 0 : 1;
		if(jobCompletion == 0) {
			long cn=job.getCounters().findCounter(TriangleCount.Triangle_Count).getValue();
			logger.info("--------------NO OF TRIANGLES---------"+(cn/3));
		}
		System.exit(0);
	}
	
	public static class ReplicatedJoinMapper extends Mapper<Object,Text,Text,Text>{
		// Initialising HashMap
		private HashMap<String,ArrayList<String>> edgesMap = new HashMap<String,ArrayList<String>>();
		private Text outvalue = new Text();
		private Text maxV = new Text();
		
		// Method to get the max value from the configuration and build the Hashmap
		@Override
		public void setup(Context context) throws IOException{
			maxV = new Text(context.getConfiguration().get("Max_value"));	
			URI[] uris = context.getCacheFiles();
			if (uris == null || uris.length == 0) {
				throw new RuntimeException(
						"Edges file is not set in DistributedCache");
			}
			// Reading from cache file and adding to the hashmap
			Path p = new Path(uris[0].getPath());
				BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(new File(p.toString()))));
				String line;
				while((line=rdr.readLine())!=null) {
					if(Integer.parseInt((line.split(",")[0])) < Integer.parseInt(maxV.toString())  
							&& Integer.parseInt((line.split(",")[1])) < Integer.parseInt(maxV.toString()) ) {
					if(edgesMap.get(line.split(",")[1]) == null) {
						ArrayList<String> valueLines = new ArrayList<String>();
						valueLines.add(line);
						edgesMap.put(line.split(",")[1],valueLines);
					}else {
						edgesMap.get(line.split(",")[1]).add(line);
					}
					}
				}
			
		}
		
		@Override
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
			// Checking for max_Value
			if(Integer.parseInt(key.toString()) < Integer.parseInt(maxV.toString())  
					&& Integer.parseInt(value.toString()) < Integer.parseInt(maxV.toString()) ) {
				// Extracting records from the hashmap with key as the key
				ArrayList<String> linesFromCache = edgesMap.get(key.toString());
				if(linesFromCache!=null) {
					if(!linesFromCache.isEmpty()) {
						for(String str: linesFromCache) {
							String from = str.split(",")[0];
							// Checking for condition 1->2->1 and removing these paths
							if(value.compareTo(new Text(from))!=0) {
								// Finding if there is a closed triangle by extracting the records from the hashmap with from as the key
								ArrayList<String> CompleteTriangleLines = edgesMap.get(from.toString());
								if(CompleteTriangleLines!=null) {
									if(!CompleteTriangleLines.isEmpty()) {
										for(String st: CompleteTriangleLines) {
											String to = st.split(",")[0];
											// Incrementing count when a closing edge is found
											if(value.compareTo(new Text(to))==0) {
												context.getCounter(TriangleCount.Triangle_Count).increment(1);
											}
										}
									}
								}

							}
						}
					}
				}
			}
		}
	}
}