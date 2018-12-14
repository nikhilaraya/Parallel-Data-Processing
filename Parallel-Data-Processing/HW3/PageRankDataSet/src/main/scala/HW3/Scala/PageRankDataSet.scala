package HW3.Scala

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.functions._



object PageRankDataSet {
  
  import org.apache.spark.sql.SparkSession
  val session: SparkSession = SparkSession.builder().appName("DataSet  Join").config("spark.master","local").getOrCreate()
  import session.implicits._
  
  session.conf.set("spark.sql.crossJoin.enabled",true)
  
   def main(args: Array[String]){
   val logger: org.apache.log4j.Logger = LogManager.getRootLogger
        		if(args.length != 2){
        			logger.error("Two arguments are required specifying the k value and output directory")
        			System.exit(1);
        		}
  
   // Initializing k, graph and pageRank 
   var graph = Seq(("1","2")).toDF("v","v1")
   var pageRank = Seq(("0",0.0)).toDF("v","pr")
   
   var k = args(0).toInt

   // building the graph data set and page rank data set with initial values
   for(i <- 1 to (k*k)){
	   if(i!=1){
		   if((i % k) == 0)
			   graph =graph.union(Seq((""+i,"0")).toDF())
			   else
				   graph =graph.union(Seq((i+"",(i+1)+"")).toDF())
	   }
	   pageRank = pageRank.union(Seq((i+"",(1.0/(k*k)))).toDF())
   }
   
   pageRank.show()
   graph.show()
   for(i <- 1 to 10){
      
     // Performing a join operation on graph and PageRank and grouping by v1 and calculating the sum
	   val temp = graph.join(pageRank,"v").groupBy("v1").sum("PR")
	   // Extracting the page rank value of the dangling node
	   val delta = temp.filter($"v1"=== "0").collectAsList().get(0).get(1).toString().toDouble

	   // Constructing the new pageRank table by adding (delta/k*k) to the previous page ranks
	   pageRank = pageRank.join(temp, $"v" === $"v1", joinType = "left")
	                      .withColumn("PR", when($"sum(PR)"isNull,(delta/(k*k))).otherwise($"sum(PR)" + delta/(k*k)))
	                      .withColumn("PR", when($"v" === 0+"",0.0).otherwise($"PR"))
	                    	.select("v","PR")
  
	   logger.info("Page Rank Sum "+i+ "--------------"+pageRank.agg(sum("PR")).first.get(0))
   }
   
   logger.info("Page ranks "+pageRank.orderBy($"PR".desc).take(5).foreach(println))
}


}