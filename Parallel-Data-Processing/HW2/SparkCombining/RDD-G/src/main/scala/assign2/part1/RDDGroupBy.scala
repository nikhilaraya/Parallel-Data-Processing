package assign2.part1

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object RDDGroupBy {
  def main(args: Array[String]){
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if(args.length != 2){
      logger.error("Two arguments are required specifying the input and output directory")
      System.exit(1);
    }
    
    val conf = new SparkConf().setAppName(" Followers Count by GroupBy")
    val sc = new SparkContext(conf)
    
    
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try{
      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)),true) 
      }
    catch { case _: Throwable => {} }
    
    // Reading the file from input directory
    val textFile = sc.textFile(args(0))
    
    // Constructing the RDD by splitting each record by the delimitor ',' and 
    // returning the userId with count 1
    val mapCounts = textFile.map{
      pair => val userID = pair.split(",")
     (userID(1),1)
    }
    
    // The groupByKey groups the RDD by key that is the userID
    val counts = mapCounts.groupByKey();
    
    //  aggregates the value count
     counts.map{ case (userId, followers) => (userId, followers.sum)}.saveAsTextFile(args(1))
     
     // printing the toDebugString
     logger.info(" toDebugString Details "+ counts.toDebugString);
  }
}