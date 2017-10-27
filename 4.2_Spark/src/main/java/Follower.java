import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;



public class Follower {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		JavaSparkContext jsc =new JavaSparkContext(conf);
	    JavaRDD<String> file = jsc.textFile("hdfs:///input");
	    JavaRDD<String> users = file.flatMap((FlatMapFunction<String, String>) arg0 -> {
            ArrayList<String> list = new ArrayList<>();
            list.add(arg0.split("\t")[1]);
            return list.iterator();
        });
	    JavaPairRDD<String, Integer> pairs = users.mapToPair((PairFunction<String, String, Integer>) arg0 -> new Tuple2<>(arg0, 1));
	    JavaPairRDD<String, Integer> counts = pairs.reduceByKey((Function2<Integer, Integer, Integer>) (arg0, arg1) -> arg0+arg1);
	    counts.saveAsTextFile("hdfs:///follower-output");
	    jsc.close();

	}

}
