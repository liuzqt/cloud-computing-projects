import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.json.JSONObject;

public class DataProducer {

	public static void main(String[] args) {
		/*
		 * Task 1: In Task 1, you need to read the content in the tracefile we
		 * give to you, and create two streams, feed the messages in the
		 * tracefile to different streams based on the value of "type" field in
		 * the JSON string.
		 * 
		 * Please note that you're working on an ec2 instance, but the streams
		 * should be sent to your samza cluster. Make sure you can consume the
		 * topics on the master node of your samza cluster before make a
		 * submission.
		 */

		Properties props = new Properties();
		props.put("bootstrap.servers", "172.31.62.117:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<Integer, String> producer = new KafkaProducer<>(props);
		try(BufferedReader br = new BufferedReader(new FileReader("tracefile"))){
			String line="";
			int count=0;
			while((line = br.readLine())!=null){
				count++;
				JSONObject jsonObject = new JSONObject(line);
				int blockId = jsonObject.getInt("blockId");
				if (jsonObject.getString("type").equals("DRIVER_LOCATION")) {
					producer.send(new ProducerRecord<Integer, String>("driver-locations", blockId, jsonObject.toString()));
				} else {
					producer.send(new ProducerRecord<Integer, String>("events", blockId, jsonObject.toString()));
				}

				System.out.println(count);
				
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		producer.close();
	}
}
