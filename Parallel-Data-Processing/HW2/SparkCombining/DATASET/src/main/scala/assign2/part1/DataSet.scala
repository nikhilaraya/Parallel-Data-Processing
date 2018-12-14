package assign2.part1

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object DataSet {
  
  
  import org.apache.spark.sql.SparkSession
  
  val session: SparkSession = SparkSession.builder().appName("DataSet Twitter Follower").config("spark.master","local").getOrCreate()
  import session.implicits._
  
  def main(args: Array[String]){
    
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
     if(args.length != 2){
      logger.error("Two arguments are required specifying the input and output directory")
      System.exit(1);
    }
    
    // Reading the input file and converting to a data set
    val twitterDataSet = session.read.option("inferSchema","true").csv(args(0))
    
    // Grouping by userID (which is column 1) and counting the number of counts
    val followersCount = twitterDataSet.groupBy("_c1").count();
    
    // printing the explain()
     logger.info("explain output"+followersCount.explain(true));
     
     // partitioning to one file and saving to the output directory
    followersCount.coalesce(1).write.format("csv").save(args(1))
    
   
  }
}