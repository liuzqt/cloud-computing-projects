import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

object PageRank {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("pageRank")
    conf.set("yarn.scheduler.maximum-allocation-mb","26000m")
    conf.set("spark.yarn.executor.memoryOverhead", "2000m")
    val sc = new SparkContext(conf)

    val iterNum = 10
//    val file = sc.textFile("hdfs:///input")

    val file = sc.textFile("hdfs:///sample")
    val all = file.flatMap { line => line.split("\t") }.distinct()
    all.cache()
    val contributor = file.map ( line => line.split("\t")(0)).distinct()
    val dangling = all.subtract(contributor).map { x => (x,1) }
    dangling.cache()
    
    val size = all.count()

    //not include dangling users
    val graph = file.map(line => { val temp = line.split("\t"); (temp(0), temp(1)) }).distinct().groupByKey()
    graph.cache()
    
    var ranks = all.map { x => (x,1.0) }

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
      
      val danglingDistribute = ranks.join(dangling).reduce({case((id1,(rank1,dummy1)),(id2,(rank2,dummy2)))=>("a",(rank1+rank2,1))})._2._1/size
      val allTemp = all.map { x => (x,danglingDistribute) }
      ranks = allTemp.union(ranks).reduceByKey(_+_)
    }

    val result = ranks.map{case(user,rank)=>user+"\t"+rank}

    result.saveAsTextFile("hdfs:///pagerank-output")

    sc.stop()
  }

}
