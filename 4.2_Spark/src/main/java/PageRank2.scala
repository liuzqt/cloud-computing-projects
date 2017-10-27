import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

object PageRank2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("pageRank")
    
    conf.set("spark.yarn.executor.memoryOverhead", "1000m")
    val sc = new SparkContext(conf)

    val iterNum = 10
    val file = sc.textFile("hdfs:///input")

//    val file = sc.textFile("hdfs:///sample")
    val all = file.flatMap { line => line.split("\t") }.distinct()
    val contributor = file.map ( line => line.split("\t")(0)).distinct()
    val dangling = all.subtract(contributor)
    
    val graphDangling = dangling.cartesian(all).groupByKey()

    val graph0 = file.map(line => { val temp = line.split("\t"); (temp(0), temp(1)) }).distinct().groupByKey()
    val graph = graph0.union(graphDangling)
    graph.cache()
    
    var ranks = graph.mapValues { x => 1.0 }

    var i = 0

    for (i <- 1 to iterNum) {
      val contriReceive = graph.join(ranks).values.flatMap {
        case (followees, rank) =>
          {
            val size = followees.size;
            followees.map(followee => (followee, rank / size))
          }
      }
      ranks = contriReceive.reduceByKey(_ + _).mapValues { x => 0.15 + 0.85 * x }
    }

    val result = ranks.map{case(user,rank)=>user+"\t"+rank}

    result.saveAsTextFile("hdfs:///pagerank-output")

    sc.stop()
  }

}
