package assign2.part1

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object RDDAggregateBy {
  def main(args: Array[String]){
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if(args.length != 2){
      logger.error("Two arguments are required specifying the input and output directory")
      System.exit(1);
    }
    
    val conf = new SparkConf().setAppName(" Followers Count Aggregate By")
    val sc = new SparkContext(conf)
    
    // Deleting the output directory
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try{
      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)),true) 
      }
    catch { case _: Throwable => {} }
    
    
    // Constructing the RDD by splitting each record by the delimitor ',' and 
    // returning the userId with count 1
    val textFile = sc.textFile(args(0))
    val mapCounts = textFile.map{
      pair => val userID = pair.split(",")
     (userID(1),1)
    }
    
    // using aggregateByKey on userID and an aggregate function
    val counts = mapCounts.aggregateByKey(0)((k,v) => k+v,(v1,v2) => v1+v2)
    
    // Saving to the output file
    counts.saveAsTextFile(args(1))
    
     // printing the toDebugString
     logger.info(" toDebugString Details "+ counts.toDebugString);
  }
}