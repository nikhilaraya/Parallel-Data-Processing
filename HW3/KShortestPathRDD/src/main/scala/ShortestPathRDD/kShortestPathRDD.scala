package ShortestPathRDD

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

object kShortestPathRDD {
  val logger: org.apache.log4j.Logger = LogManager.getRootLogger
  def main(args: Array[String]) {

    if (args.length != 3) {
      logger.error("Two arguments are required specifying the k value and output directory")
      System.exit(1);
    }
    val conf = new SparkConf().setAppName("K source shortest path").setMaster("local")
    val sc = new SparkContext(conf)
    var iterationCount = sc.accumulator(0);

    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try {
    //      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true)
    //    } catch { case _: Throwable => {} }
    
    val sourceNode = "2";
    // Read the input edges.csv file
    val edgesfile = sc.textFile(args(0))

    // Create edges RDD reading each line from the edges.csv file 
    val edgesRDD = edgesfile.map { line =>
      val record = line.split(",")
      (record(0), record(1))
    }

    // Create the adjacency list for each vertex in edgesRDD using group by and also check if the vertex is 
    // the source node or not and correspondingly set the flag to T or F
    val graphRdd = edgesRDD.groupBy(_._1).map {
      case (v1, v2) => if (v1.equals(sourceNode))
        (v1, (v2.map(_._2), "T"))
      else (v1, (v2.map(_._2), "F"))
    }

    // Creating distances RDD by setting the initial distances as 0 if the vertex is a source node else infinity
    //if the vertex is not a source node
    var distances = graphRdd.mapValues {
      case (e) => if (e._2.compareTo("T") == 0) 0.0
      else Double.PositiveInfinity
    };
    var oldDistances = distances;
    var iterate = true;
    var count = 0.toLong;
     while (iterate) {
       
      // join the distances and the graph rdd and for each vertex in the adjacency list update the distances
      distances = graphRdd.join(oldDistances).flatMap(e => extractVertices(e)).reduceByKey(Math.min(_, _));
      iterate = false
      
      // detecting changes to in the distances table to check if another iteration is required or not.
       count = oldDistances.join(distances).map(line => ((line._2._1 != line._2._2),1)).filter(_._1 == true).count()
       
      if (count > 0.0) {
        iterate = true;
        oldDistances = distances;
      } else {
        iterate = false
      }
    }
//    distances.saveAsTextFile("final1");
  }
  
  // Function extractVertices returns each vertex id m in n’s(key) adjacency list as (m, distance(n)+w)
  // where w is the weight of the edge (n,m) and also returns itself as (n, distance(n))
  def extractVertices(line: (String, ((Iterable[String], String), Double))): Iterable[(String, Double)] = {
    var returnlist = new ListBuffer[String]();
    for (i <- line._2._1._1) {
      returnlist += i;
    }
    returnlist += line._1;
    val distance = line._2._2
    return returnlist.map(vertex => if (vertex.equals(line._1)) (vertex, distance)
    else (vertex, distance + 1));
  }

}