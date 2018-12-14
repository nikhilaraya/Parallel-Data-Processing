
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object DataSetRedJoin {
  
  import org.apache.spark.sql.SparkSession
  
  val session: SparkSession = SparkSession.builder().appName("DataSet Reduce Join").config("spark.master","local").getOrCreate()
  import session.implicits._
  
  session.conf.set("spark.sql.crossJoin.enabled",true)
  val maxVal = 30000
    def main(args: Array[String]){
    
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
     if(args.length != 2){
      logger.error("Two arguments are required specifying the input and output directory")
      System.exit(1);
    }
    
     // Reading the input file and returning a dataset with columns from and to
     val twitterDataSet = session.read.option("inferSchema","true").csv(args(0)).toDF("from","to")
     
     // Filtering the records based on the maxVal
     val maxFilter = twitterDataSet.filter($"_c0" < maxVal && $"_c1" < maxVal)
     
     // Creating two data sets with columns
     val edgesDS = maxFilter.toDF("from1","middle1")
     val selfEdgesDS = maxFilter.toDF("middle2","to1")
     
     // Joining the two datasets and eliminating the case 1->2->1 path
     val nodePaths = edgesDS.join(selfEdgesDS,($"middle1" === $"middle2")&&($"to1" !== $"from1"))
                            .select($"from1",$"middle1", $"to1") 
    
     // Joining nodePaths and maxFilter to get the closed triangles   
     val closedTriangle = nodePaths.join(maxFilter,($"to1" === $"from")&&($"from1" === $"to"))
                                   .select($"from1", $"to1") 
                                   
     // Finding the count of triangles
     val triangleCount =  (closedTriangle.count()/3)
     logger.info("-----------triangle count---------"+ (closedTriangle.count())/3)
  }
}