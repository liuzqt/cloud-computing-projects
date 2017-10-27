import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class ngram {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		static enum CountersEnum {
			INPUT_WORDS
		}

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().toLowerCase();
			try {
				Document document = (Document) DocumentHelper.parseText(line);
				Element textNode = document.getRootElement().element("revision").element("text");
				String text = textNode.getStringValue()
						.replaceAll("(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]", " ")
						.replaceAll("<ref[^>]*>", " ").replaceAll("</ref>", " ")
						.replaceAll("'(?![a-z])|(?<![a-z])'", " ");

				ArrayList<String> wordList = new ArrayList<String>();
				String[] words = text.split("[^a-z']+");
				for (String w : words) {
					if (!(w.isEmpty())) {
						wordList.add(w);
					}
				}
				words = new String[wordList.size()];
				wordList.toArray(words);

				String[] tempWords;
				StringBuilder sb = new StringBuilder();
				wordList = null;
				for (int i = 1; i <= 5; i++) {
					for (int j = 0; j < words.length - i + 1; j++) {
						tempWords = Arrays.copyOfRange(words, j, j + i);
						sb.setLength(0);
						for (String w : tempWords) {
							sb.append(w + " ");
						}
						word.set(sb.toString().trim());
						context.write(word, one);
						Counter counter = context.getCounter(CountersEnum.class.getName(),
								CountersEnum.INPUT_WORDS.toString());
						counter.increment(1);
					}
				}

			} catch (DocumentException e) {
				e.printStackTrace();
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if (sum > 2) {
				result.set(sum);
				context.write(key, result);
			}
		}
	}



	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "ngram");
		job.setJarByClass(ngram.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// job.setNumReduceTasks(8);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
