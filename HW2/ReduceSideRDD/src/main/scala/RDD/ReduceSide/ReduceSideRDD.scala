package RDD.ReduceSide

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ReduceSideRDD{
def main(args: Array[String]){
    val maxValue = 30000;
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if(args.length != 2){
      logger.error("Two arguments are required specifying the input and output directory")
      System.exit(1);
    }
    val conf = new SparkConf().setAppName("Triangles Count")
    val sc = new SparkContext(conf)
    
    // Deleting the output directory
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try{
      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)),true) 
      }
    catch { case _: Throwable => {} }
    
    // Read the input edges.csv file
    val edgesfile = sc.textFile(args(0))
    
    // Create a edgesRDD by splitting with delimiter',' each record(from,to) and mapping it to (to,from)
    val edgesRDD = edgesfile.map{line => val record = line.split(",")
                  if((record(1).toInt) < maxValue && (record(0).toInt) < maxValue){
                  (record(1),record(0))}
                  else("","")}
    
    // Create a selfJoinEdgesRDD by splitting with delimiter',' each record(from,to) and mapping it to (from,to)
    val selfJoinEdgesRDD = edgesfile.map{line => val record = line.split(",")
                  if((record(1).toInt) < maxValue && (record(0).toInt) < maxValue){
                  (record(0),record(1))}else("","")}
    
    // Joining the two RDD on key to get the path 2 and checking for 1->2->1 paths and eliminating them
    var joinedRDD = edgesRDD.join(selfJoinEdgesRDD)
                    .map{case(middle,(from,to)) => if(from!="" && from != to)(from,to)else("","")}
    joinedRDD.saveAsTextFile(args(1))
    // Performing another join on the joinedRDD and edges to find the closing path
    var closedRDD = joinedRDD.join(edgesRDD).map{case(middle,(from,to)) => if(from!="" && from == to) (from,to)else ("","")}
    
    //Filtering records which are empty and finding the count of triangles
    closedRDD = closedRDD.filter{case(from,to) => if(from!=""){true}else{false}}
    
    sc.parallelize(Seq ((closedRDD.count()/3))).saveAsTextFile(args(1))
  }
}