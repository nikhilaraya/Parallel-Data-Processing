package assign2.part1

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object RDDReduceBy{
  def main(args: Array[String]){
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if(args.length != 2){
      logger.error("Two arguments are required specifying the input and output directory")
      System.exit(1);
    }
    
    val conf = new SparkConf().setAppName(" Followers Count Reduce By")
    val sc = new SparkContext(conf)
    // Deleting the output directory
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try{
      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)),true) 
      }
    catch { case _: Throwable => {} }

    // Reads the input file from the directory as specified in the arguments
    val textFile = sc.textFile(args(0))

    // Creates a map by splitting each line by the character ',' and uses the second
    // part of the split which represents the user ID and creates a pair RDD (userId,1)
	  val mapCounts = textFile.map{ pair => val userID = pair.split(",")
                                       (userID(1), 1)}
    // groups by the userID and aggregates the values of the same key (userID)	
    val counts = mapCounts.reduceByKey(_ + _)
    
    counts.saveAsTextFile(args(1))
    
    // printing the toDebugString
     logger.info(" toDebugString Details "+ counts.toDebugString);
  }
}
