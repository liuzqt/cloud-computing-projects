import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Follower_scala {
  def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val file = sc.textFile("hdfs:///input")
//    val file = sc.textFile("hdfs:///sample")
    val counts = file.map(line => line.split("\t")(1))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map{case(user,followerNumber)=>user+"\t"+followerNumber}
      
    counts.saveAsTextFile("hdfs:///follower-output")
    sc.stop()
//    counts.saveAsTextFile("hdfs:///sample-output")
  }
}