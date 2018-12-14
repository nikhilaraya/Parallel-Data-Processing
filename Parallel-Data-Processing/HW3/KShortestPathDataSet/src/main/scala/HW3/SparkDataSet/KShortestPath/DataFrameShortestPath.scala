package HW3.SparkDataSet.KShortestPath

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.{udf, col, lit, when,explode, struct}
import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.WrappedArray

object DataFrameShortestPath {
  
   import org.apache.spark.sql.SparkSession
   
   val session: SparkSession = SparkSession.builder().appName("DataSet Shortest Path")
                                           .config("spark.master","local")
                                           .getOrCreate()
   import session.implicits._
  
  val logger: org.apache.log4j.Logger = LogManager.getRootLogger
  
  def main(args: Array[String]){
   
     if(args.length != 2){
      logger.error("Two arguments are required specifying the input and output directory")
      System.exit(1);
    }
     
     
     
     
     var sourceNode = "2"
     // Reading the input file and returning a dataset with columns from and to
     val twitterDataSet = session.read.option("inferSchema","true").csv(args(0)).toDF("from","to")
     
      val graph = twitterDataSet.groupBy("from").agg(collect_list('to).as("set"))
                                .withColumn("activeflag", when($"from" === sourceNode,"T").otherwise("F"))
    
      var distances = graph.withColumn("distance", when($"activeflag" === "T",0.0).otherwise(Double.PositiveInfinity))
                          .select("from","distance").withColumnRenamed("from", "v")
      
      var iterate = true;
      var newdistances = null;
      var difference = 0.0;

      while(iterate){
    	  var distance1 = graph.join(distances,$"v" === $"from", joinType = "inner").rdd
    			  .flatMap(line => extractVertices(line)).reduceByKey(Math.min(_, _));
    	  iterate = false
    			  // distance1.toDF().show()
    			  // detecting changes to in the distances table to check if another iteration is required or not.
    			  difference = distances.join(distance1).map(line => ((line._2._1 != line._2._2),1)).filter(_._1 == true).count()

    			  if (count > 0.0) {
    				  iterate = true;
    				  distances = distance1;
    			  } else {
    				  iterate = false
    			  }
    	  iterate = false;
      }
}

   def extractVertices(line : org.apache.spark.sql.Row): ListBuffer[(String, Double)] ={
    val df = new ListBuffer[String]()
    val distance = line.get(3).toString().toDouble
    val v = line.get(0).toString()
    df += line.get(0).toString()
    for(i <- 0 to line.get(1).asInstanceOf[WrappedArray[String]].length-1){
    	df += line.get(1).asInstanceOf[WrappedArray[String]](i)
    }
    val ret = df.map(vertex => 
                  if (vertex.equals(v))
                    (vertex, distance)
                     else (vertex, distance + 1.0));
    return ret
  }
   
}