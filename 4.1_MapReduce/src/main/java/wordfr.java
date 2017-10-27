import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


// just like a word count(1garm) program for XML
public class wordfr {
	private static int wordNum = 5;
	private static Admin hbaseAdmin;

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private Text columnValue = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] pair = value.toString().trim().split("\t");
			String w = pair[0];

			for (int i = 1; i < w.length(); i++) {
				word.set(w.substring(0, i));
				columnValue.set(w + " " + pair[1]);
				context.write(word, columnValue);
			}
		}
	}

	public static class posibilityReducer
			extends TableReducer<Text, Text, NullWritable> {

		public void reduce(Text key, Iterable<Text> values, Mapper.Context context) throws IOException, InterruptedException {
			int total = 0;
			ArrayList<String[]> valuePair = new ArrayList<String[]>();
			for (Text val : values) {
				String[] data = val.toString().trim().split(" ");
				valuePair.add(data);
				total += Integer.parseInt(data[1]);
			}
			Collections.sort(valuePair, (o1, o2) -> {
                int i1 = Integer.parseInt(o1[1]);
                int i2 = Integer.parseInt(o2[1]);
                return i2 - i1;
            });
			Put putrow = new Put(key.getBytes());
			List<String[]> temp;

			if (valuePair.size() > wordNum) {
				temp = valuePair.subList(0, wordNum);

				for (String[] pair : temp) {
					putrow.addColumn(Bytes.toBytes("part"), Bytes.toBytes(pair[0]),
							Bytes.toBytes(String.valueOf(Integer.parseInt(pair[1]) * 1.0 / total)));
				}
			} else {

				for (String[] pair : valuePair) {
					putrow.addColumn(Bytes.toBytes("part"), Bytes.toBytes(pair[0]),
							Bytes.toBytes(String.valueOf(Integer.parseInt(pair[1]) * 1.0 / total)));
				}
			}
			context.write(NullWritable.get(), putrow);
		}
	}
	private static String zkAddr = "172.31.28.125";
	public static void main(String[] args) throws Exception {
		TableOutputFormat
        Configuration conf = new Configuration();
        conf.set("hbase.master", zkAddr + ":16000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        conf.set(TableOutputFormat.OUTPUT_TABLE, "wordcomplete");
        Configuration hconf = HBaseConfiguration.create(conf);
        Connection conn = ConnectionFactory.createConnection(hconf);
        Job job = Job.getInstance(conf, "language model");
        job.setJarByClass(wordfr.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(posibilityReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        TableMapReduceUtil.initTableReducerJob("wordcomplete", posibilityReducer.class, job);
        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}