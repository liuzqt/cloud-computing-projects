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
public class DriverMatchTask3 implements StreamTask, InitableTask, WindowableTask {

    /* Define per task state here. (kv stores etc) */
    private double MAX_MONEY = 100.0;
    // store driver information
    private KeyValueStore<String, String> driver;
    // store driver location
    private KeyValueStore<String, String> driverLocation;
    
    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) throws Exception {
        // Initialize (maybe the kv stores?)
        driver = (KeyValueStore<String, String>) context.getStore("driver");
        driverLocation = (KeyValueStore<String, String>) context.getStore("driver-loc");
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
        String id = String.valueOf(message.get("driverId"));
        String location = String.valueOf(message.get("latitude")) + ":" + String.valueOf(message.get("longitude"));
        // update driver location
        driverLocation.put(id, location);
    }
    @SuppressWarnings("unchecked")
    private void processMessageEvent(Map<String, Object> message, MessageCollector collector) {
        String type = (String) message.get("type");
        String blockid = String.valueOf(message.get("blockId"));
        // driver's position or client's position
        double latitude = (double) message.get("latitude");
        double longitude = (double) message.get("longitude");

        if(message.get("driverId") != null) {
            String driverid = String.valueOf(message.get("driverId"));
            String status = (String) message.get("status");
            if(type.equals("ENTERING_BLOCK") && status.equals("AVAILABLE")){
                String rating = String.valueOf(message.get("rating"));
                String salary = String.valueOf(message.get("salary"));
                String gender = (String) message.get("gender");
                // add driver's information
                driver.put(blockid + ":" + driverid, latitude + ":" + longitude + ":" + rating + ":" + salary + ":" + gender);
            }
            else if(type.equals("LEAVING_BLOCK")){
                // delete driver's information
                driver.delete(blockid + ":" + driverid);
            }
            else if(type.equals("RIDE_COMPLETE")){
                String rating = String.valueOf(message.get("rating"));
                String salary = String.valueOf(message.get("salary"));
                String gender = (String) message.get("gender");
                // add driver's information
                driver.put(blockid + ":" + driverid, latitude + ":" + longitude + ":" + rating + ":" + salary + ":" + gender);
            }
        }
        else {
            int clientid = (Integer) message.get("clientId");
            String genderPre = (String) message.get("gender_preference");
            
            // search every driver in this block
            KeyValueIterator<String, String> iter = driver.range(blockid + ":", blockid + ";");
            org.apache.samza.storage.kv.Entry<String, String> entry = null;
            String bestdriver = null;
            double bestscore = 0;
            while(iter.hasNext()){
                entry = iter.next();
                String thisdriver = entry.getKey().split(":")[1];
                String[] data = entry.getValue().split(":");
                double la = Double.parseDouble(data[0]);
                double lo = Double.parseDouble(data[1]);
                double rating = Double.parseDouble(data[2]);
                double salary = Double.parseDouble(data[3]);
                String gender = data[4];
                
                // check whether there is updated location
                if(driverLocation.get(thisdriver) != null){
                    String[] p = driverLocation.get(thisdriver).split(":");
                    la = Double.parseDouble(p[0]);
                    lo = Double.parseDouble(p[1]);
                }
                double distance = Math.sqrt(Math.pow(la - latitude, 2) + Math.pow(lo - longitude, 2));
                double distance_score = Math.pow(Math.E, -1 * distance);
                double gender_score = 0.0;
                if(genderPre != null && gender != null)
                    if(genderPre.equals("N") || genderPre.equals(gender))
                        gender_score = 1.0;
                
                // calculating score
                double score = distance_score * 0.4 + gender_score * 0.2 + rating/5.0 * 0.2 + (1 - salary/MAX_MONEY) * 0.2;
                if(score > bestscore){
                    bestscore = score;
                    bestdriver = thisdriver;
                }
            }
            iter.close();
            if(bestdriver != null){
            	driver.delete(blockid + ":" + bestdriver);
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
