import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
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
import java.util.*;

public class phasefr {
	private static int wordNum=5;
	private static Admin hbaseAdmin;

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		
		private Text rowkey = new Text();
		private Text columnValue = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] kv =value.toString().trim().split("\t");
			String[] words = kv[0].split(" ");
			if ((words.length > 1)) {
				String[] keyWords = Arrays.copyOfRange(words, 0, words.length - 1);
				StringBuilder sb = new StringBuilder();
				for (String s : keyWords) {
					sb.append(s + " ");
				}
				String v = words[words.length - 1] + " " + kv[1];
				rowkey.set(sb.toString().trim());
				columnValue.set(v);
				context.write(rowkey, columnValue);
			}

		}
	}

	public static class posibilityReducer
			extends org.apache.hadoop.hbase.mapreduce.TableReducer<Text, Text, NullWritable> {
		


		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			ArrayList<String[]> valuePair = new ArrayList<String[]>();
			int total = 0;
			for (Text val : values) {
				String[] data = val.toString().trim().split(" ");
				valuePair.add(data);
				total += Integer.parseInt(data[1]);
			}
			Collections.sort(valuePair, new Comparator<String[]>() {

				@Override
				public int compare(String[] o1, String[] o2) {
					int i1 = Integer.parseInt(o1[1]);
					int i2 = Integer.parseInt(o2[1]);
					return i2 - i1;
				}

			});
			Put putrow = new Put(key.getBytes());
			List<String[]> temp;
			
			if (valuePair.size() > wordNum) {
				temp = valuePair.subList(0, wordNum);
				for (String[] pair : temp) {
					putrow.addColumn(Bytes.toBytes("word"), Bytes.toBytes(pair[0]), Bytes.toBytes(String.valueOf(Integer.parseInt(pair[1])*1.0/total)));
				}
			} else {
				for (String[] pair : valuePair) {
					putrow.addColumn(Bytes.toBytes("word"), Bytes.toBytes(pair[0]), Bytes.toBytes(String.valueOf(Integer.parseInt(pair[1])*1.0/total)));		
				}
				
			}
				context.write(NullWritable.get(), putrow);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "172.31.3.199");
		String tablename = "ngram";
		conf.set(TableOutputFormat.OUTPUT_TABLE, tablename);
		conf.set("hbase.master", "172.31.3.199:60000");
		Configuration hbaseConf = HBaseConfiguration.create(conf);

        try {
        	Connection connection = ConnectionFactory.createConnection(hbaseConf);
            hbaseAdmin = connection.getAdmin();
            System.out.println("HBase connection successfull");
        } catch (MasterNotRunningException e) {
        	System.out.println("HBase Master Exception " + e);
        } catch (ZooKeeperConnectionException e) {
        	System.out.println("Zookeeper Exception " + e);
        }

		wordNum = Integer.parseInt(args[1]);
		System.out.println("word num is "+wordNum);
		
		Job job = Job.getInstance(conf, "ngramtohbase");
		job.setJarByClass(phasefr.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		TableMapReduceUtil.initTableReducerJob(tablename, posibilityReducer.class, job);
		job.setOutputFormatClass(TableOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}