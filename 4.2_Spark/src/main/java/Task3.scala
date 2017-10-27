import org.apache.spark._
import org.apache.spark.graphx._

import org.apache.spark.rdd.RDD

object Task3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("recommend")
    conf.set("spark.yarn.executor.memoryOverhead", "5000m")
    val sc = new SparkContext(conf)

    val pageRank = sc.textFile("hdfs:///pagerank-output").map { line => { val temp = line.split("\t"); val id: VertexId = temp(0).toLong; (id, temp(1).toDouble) } }
    val originGraph = GraphLoader.edgeListFile(sc, "hdfs:///input")
    val graph0 = originGraph.mapVertices({ case (id, attr) => 0.0 })
    val graph = graph0.joinVertices(pageRank)({ case (id, v, rank) => rank })

    val message = graph.aggregateMessages[(Long, Double)](triplet => {
      triplet.sendToSrc((triplet.dstId, triplet.dstAttr))
    },
      (a, b) => { if (a._2 >= b._2) { a } else { b } })

    val graph2 = graph.mapVertices({ case (id, attr) => (0L, 0.0) }).joinVertices(message)({ case (id, v, msg) => msg })

    val message2 = graph2.aggregateMessages[(Long, Double)](triplet => {
      triplet.sendToSrc(triplet.dstAttr)
    },
      (a, b) => { if (a._2 >= b._2) { a } else { b } })

    val result = message2.map[String]({ case (id, (rcmid, influ)) => id.toString + "\t" + rcmid.toString + "\t" + influ.toString() })
    result.saveAsTextFile("hdfs:///task3-output")
    sc.stop()
  }

}
