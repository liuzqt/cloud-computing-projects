package com.cloudcomputing.samza.pitt_cabs;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {

	/* Define per task state here. (kv stores etc) */
	private final double MAX_MONEY = 100.0;
	private KeyValueStore<String, String> driver;
	private KeyValueStore<String, String> driverLocation;
	private final String AVAILABLE = "AVAILABLE";
	private final String ENTERING_BLOCK = "ENTERING_BLOCK";
	private final String LEAVING_BLOCK = "LEAVING_BLOCK";
	private final String RIDE_COMPLETE = "RIDE_COMPLETE";

	@Override
	@SuppressWarnings("unchecked")
	public void init(Config config, TaskContext context) throws Exception {
		driver = (KeyValueStore<String, String>) context.getStore("driver");
		driverLocation = (KeyValueStore<String, String>) context.getStore("driver-loc");
		System.out.println("version2");
	}

	@Override
	@SuppressWarnings("unchecked")
	public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
		// The main part of your code. Remember that all the messages for a
		// particular partition
		// come here (somewhat like MapReduce). So for task 1 messages for a
		// blockId will arrive
		// at one task only, thereby enabling you to do stateful stream
		// processing.
		String incomingStream = envelope.getSystemStreamPartition().getStream();

		if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
			// Handle Driver Location messages
			updateLocation((Map<String, Object>) envelope.getMessage());

		} else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
			// Handle Event messages
			processEvent((Map<String, Object>) envelope.getMessage(), collector);

		} else {
			throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
		}
	}

	private void updateLocation(Map<String, Object> message) {
		if (message.get("driverId") != null) {
			if (message.get("latitude") != null && message.get("longitude") != null) {
				String id = String.valueOf(message.get("driverId"));
				String location = String.valueOf(message.get("latitude")) + ":"
						+ String.valueOf(message.get("longitude"));
				driverLocation.put(id, location);
			}
		}

	}

	private void processEvent(Map<String, Object> message, MessageCollector collector) {

		String type = message.get("type").toString();
		String blockid = String.valueOf(message.get("blockId"));

		if (message.get("latitude") != null && message.get("longitude") != null) {
			double latitude = (double) message.get("latitude");
			double longitude = (double) message.get("longitude");
			if (message.get("driverId") != null) {
				String driverid = String.valueOf(message.get("driverId"));
				String gender = String.valueOf(message.get("gender"));
				String rating = String.valueOf(message.get("rating"));
				String salary = String.valueOf(message.get("salary"));
				String status = String.valueOf(message.get("status"));

				if (type.equals(ENTERING_BLOCK) && status.equals(AVAILABLE)) {
					driver.put(blockid + ":" + driverid, gender + ":" + rating + ":" + salary);
					driverLocation.put(driverid, latitude + ":" + longitude);
					// System.out.println(driverid + latitude + ":" +
					// longitude);
				} else if (type.equals(LEAVING_BLOCK)) {
					driver.delete(blockid + ":" + driverid);
				} else if (type.equals(RIDE_COMPLETE)) {
					driver.put(blockid + ":" + driverid, gender + ":" + rating + ":" + salary);
					driverLocation.put(driverid, latitude + ":" + longitude);

				}
			} else {
				String thisDriver = "";
				// try {
				int clientid = (int) message.get("clientId");
				String genderPre = message.get("gender_preference").toString();
				KeyValueIterator<String, String> iter = driver.range(blockid + ":", blockid + ";");
				Entry<String, String> entry = null;

				String currentDriver = "";
				double currentScore = 0.0;
				while (iter.hasNext()) {
					entry = iter.next();
					thisDriver = entry.getKey().split(":")[1];
					String[] info = entry.getValue().split(":"); // gender:rating:salary
					double rat = Double.parseDouble(info[1]);
					double sal = Double.parseDouble(info[2]);
					if (driverLocation.get(thisDriver) == null)
						continue;
					String[] loc = driverLocation.get(thisDriver).split(":"); // la:lo
					double lat = Double.parseDouble(loc[0]);
					double longt = Double.parseDouble(loc[1]);
					double distance = Math.sqrt(Math.pow(lat - latitude, 2) + Math.pow(longt - longitude, 2));
					double distance_score = Math.pow(Math.E, -1 * distance);
					double gender_score = genderPre.equals("N") || genderPre.equals(info[0]) ? 1.0 : 0.0;
					double score = distance_score * 0.4 + gender_score * 0.2 + 0.2 * rat / 5.0
							+ 0.2 * (1 - sal / MAX_MONEY);
					if (score > currentScore) {
						currentScore = score;
						currentDriver = thisDriver;
					}
				}

				iter.close();
				if (currentDriver != null) {
					driver.delete(currentDriver);
					HashMap<String, Integer> output = new HashMap<>();
					output.put("clientId", clientid);
					output.put("driverId", Integer.parseInt(currentDriver));
					collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, output));
				}
				// } catch (Exception e) {
				// PrintStream psPrintStream = new PrintStream(System.err);
				// psPrintStream.println("this:"+thisDriver);
				// psPrintStream.close();
				// }
			}
		}

	}

	@Override
	public void window(MessageCollector collector, TaskCoordinator coordinator) {
		// this function is called at regular intervals, not required for this
		// project
	}
}
