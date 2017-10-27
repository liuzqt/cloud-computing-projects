package com.cloudcomputing.samza.pitt_cabs;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.config.Config;
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
public class DriverMatchTask2 implements StreamTask, InitableTask, WindowableTask {

    /* Define per task state here. (kv stores etc) */
    private double MAX_MONEY = 100.0;
    // store driver information
    private KeyValueStore<String, String> driver_list;
    // store driver location
    private KeyValueStore<String, String> driver_location;
    
    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) throws Exception {
        // Initialize (maybe the kv stores?)
        driver_list = (KeyValueStore<String, String>) context.getStore("driver");
        driver_location = (KeyValueStore<String, String>) context.getStore("driver-loc");
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
            processMessageLocation((Map<String, Object>) envelope.getMessage());
        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
            // Handle Event messages
            processMessageEvent((Map<String, Object>) envelope.getMessage(), collector);
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }
    private void processMessageLocation(Map<String, Object> message) throws NullPointerException{
        String driverid = String.valueOf(message.get("driverId"));
        String position = String.valueOf(message.get("latitude")) + ":" + String.valueOf(message.get("longitude"));
        // update driver location
        driver_location.put(driverid, position);
    }
    @SuppressWarnings("unchecked")
    private void processMessageEvent(Map<String, Object> message, MessageCollector collector) {
        String type = (String) message.get("type");
        String blockid = String.valueOf(message.get("blockId"));
        // driver's position or client's position
        double latitude = (double) message.get("latitude");
        double longitude = (double) message.get("longitude");
        if (type.equals("DRIVER_LOCATION")) {
            throw new IllegalStateException("Unexpected event type on messages stream: " + message);
        }
        if(message.get("driverId") != null) {
            String driverid = String.valueOf(message.get("driverId"));
            String status = (String) message.get("status");
            if(type.equals("ENTERING_BLOCK") && status.equals("AVAILABLE")){
                String rating = String.valueOf(message.get("rating"));
                String salary = String.valueOf(message.get("salary"));
                String gender = (String) message.get("gender");
                // add driver's information
                driver_list.put(blockid + ":" + driverid, latitude + ":" + longitude + ":" + rating + ":" + salary + ":" + gender);
            }
            else if(type.equals("LEAVING_BLOCK")){
                // delete driver's information
                driver_list.delete(blockid + ":" + driverid);
            }
            else if(type.equals("RIDE_COMPLETE")){
                String rating = String.valueOf(message.get("rating"));
                String salary = String.valueOf(message.get("salary"));
                String gender = (String) message.get("gender");
                // add driver's information
                driver_list.put(blockid + ":" + driverid, latitude + ":" + longitude + ":" + rating + ":" + salary + ":" + gender);
            }
        }
        else {
            int clientid = (Integer) message.get("clientId");
            String gender_pre = (String) message.get("gender_preference");
            
            // search every driver in this block
            KeyValueIterator<String, String> driver = driver_list.range(blockid + ":", blockid + ";");
            org.apache.samza.storage.kv.Entry<String, String> entry = null;
            String bestdriver = null;
            double bestscore = 0;
            while(driver.hasNext()){
                entry = driver.next();
                String thisdriver = entry.getKey().split(":")[1];
                String[] data = entry.getValue().split(":");
                double la = Double.parseDouble(data[0]);
                double lo = Double.parseDouble(data[1]);
                double rating = Double.parseDouble(data[2]);
                double salary = Double.parseDouble(data[3]);
                String gender = data[4];
                
                // check whether there is updated location
                if(driver_location.get(thisdriver) != null){
                    String[] p = driver_location.get(thisdriver).split(":");
                    la = Double.parseDouble(p[0]);
                    lo = Double.parseDouble(p[1]);
                }
                double distance = Math.sqrt(Math.pow(la - latitude, 2) + Math.pow(lo - longitude, 2));
                double distance_score = Math.pow(Math.E, -1 * distance);
                double gender_score = 0.0;
                if(gender_pre != null && gender != null)
                    if(gender_pre.equals("N") || gender_pre.equals(gender))
                        gender_score = 1.0;
                
                // calculating score
                double score = distance_score * 0.4 + gender_score * 0.2 + rating/5.0 * 0.2 + (1 - salary/MAX_MONEY) * 0.2;
                if(score > bestscore){
                    bestscore = score;
                    bestdriver = thisdriver;
                }
            }
            driver.close();
            if(bestdriver != null){
                Map<String, Integer> result = new HashMap<String, Integer>();
                result.put("clientId", clientid);
                result.put("driverId", Integer.parseInt(bestdriver));
                collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, result));
            }
        }
    }
    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {
        // this function is called at regular intervals, not required for this
        // project
    }
}
