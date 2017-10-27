import java.io.IOException;
import java.util.HashMap;
import java.util.Queue;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;
import org.vertx.java.platform.impl.HAManager;

public class Coordinator extends Verticle {

	// This integer variable tells you what region you are in
	// 1 for US-E, 2 for US-W, 3 for Singapore
	private static int region = KeyValueLib.region;

	// Default mode: Strongly consistent
	// Options: strong, eventual
	private static String consistencyType = "strong";

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances. Be sure to match the regions with their DNS!
	 * Do the same for the 3 Coordinators as well.
	 */
	private static final String dataCenterUSE = "ec2-54-159-208-233.compute-1.amazonaws.com";
	private static final String dataCenterUSW = "ec2-54-167-17-237.compute-1.amazonaws.com";
	private static final String dataCenterSING = "ec2-54-166-247-239.compute-1.amazonaws.com";
	private static final String[] dataCenters = { "null",dataCenterUSE, dataCenterUSW, dataCenterSING };

	private static final String coordinatorUSE = "ec2-54-88-90-148.compute-1.amazonaws.com";
	private static final String coordinatorUSW = "ec2-54-172-20-85.compute-1.amazonaws.com";
	private static final String coordinatorSING = "ec2-54-237-217-99.compute-1.amazonaws.com";
	private static final String[] coordinators = { "null",coordinatorUSE, coordinatorUSW, coordinatorSING };

	@Override
	public void start() {
		KeyValueLib.dataCenters.put(dataCenterUSE, 1);
		KeyValueLib.dataCenters.put(dataCenterUSW, 2);
		KeyValueLib.dataCenters.put(dataCenterSING, 3);
		KeyValueLib.coordinators.put(coordinatorUSE, 1);
		KeyValueLib.coordinators.put(coordinatorUSW, 2);
		KeyValueLib.coordinators.put(coordinatorSING, 3);
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);

		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				List<Map.Entry<String,String>> list=map.entries();
				for(Map.Entry<String,String> e:list){
					System.out.println(e.getKey()+" "+e.getValue());
				}
				final String key = map.get("key");
				final String value = map.get("value");
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				final String forwarded = map.get("forward");
				final String forwardedRegion = map.get("region");
				Thread t = new Thread(new Runnable() {
					public void run() {
						if(consistencyType.equals("strong")){
							//not forwarded request
							if (forwarded==null) {
								//inform data center
							try {
								KeyValueLib.AHEAD(key, timestamp.toString());
								System.out.println("AHEAD request sent!!!!");
							} catch (IOException e) {
								e.printStackTrace();
							}
							int hashValue = myHash(key);
							//in the same region
							if (hashValue == region) {
								int[] r = { 1, 2, 3 };
								for (int i : r) {
									try {
										KeyValueLib.PUT(dataCenters[i], key, value, timestamp.toString(),
												consistencyType);
									} catch (IOException e) {
										e.printStackTrace();
									}
								}

							} 
							//not the same region, forward to other coordinator
							else{
								try {
									KeyValueLib.FORWARD(coordinators[hashValue], key, value, timestamp.toString());
									System.out.println("FORWARD "+hashValue);
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						} 
							//forward request, must be in the same region
							else  if(forwarded.equals("true")){
							int[] r = { 1, 2, 3 };
							for (int i : r) {
								try {
									KeyValueLib.PUT(dataCenters[i], key, value, timestamp.toString(),
											consistencyType);
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						}
						}
						//eventual consistency
						else {
							//not forwarded request
							if (forwarded==null) {
							int hashValue = myHash(key);
							//in the same region
							if (hashValue == region) {
								int[] r = { 1, 2, 3 };
								for (int i : r) {
									try {
										KeyValueLib.PUT(dataCenters[i], key, value, timestamp.toString(),
												consistencyType);
									} catch (IOException e) {
										e.printStackTrace();
									}
								}

							} 
							//not the same region, forward to other coordinator
							else{
								try {
									KeyValueLib.FORWARD(coordinators[hashValue], key, value, timestamp.toString());
									System.out.println("FORWARD "+hashValue);
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						} 
							//forward request, must be in the same region
							else  if(forwarded.equals("true")){
							int[] r = { 1, 2, 3 };
							for (int i : r) {
								try {
									KeyValueLib.PUT(dataCenters[i], key, value, timestamp.toString(),
											consistencyType);
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						}
						}

					}
				});
				t.start();
				req.response().end(); // Do not remove this
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				Thread t = new Thread(new Runnable() {
					public void run() {
						System.out.println("get" + timestamp + " " + key + " arrive");
						String response = "0";
						if(consistencyType.equals("strong")){
							try{
								response = KeyValueLib.GET(dataCenters[region], key, timestamp.toString(), consistencyType);
							}catch (IOException e) {
								e.printStackTrace();
							}
							if(response.equals("null"))
								response = "0";
						}
						// eventual consistency
						else{
							try{
								response = KeyValueLib.GET(dataCenters[region], key, timestamp.toString(), consistencyType);
							}catch (IOException e) {
								e.printStackTrace();
							}
							if(response.equals("null"))
								response = "0";
						}

						req.response().end(response);
					}
				});
				t.start();
			}
		});
		/*
		 * This endpoint is used by the grader to change the consistency level
		 */
		routeMatcher.get("/consistency", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				consistencyType = map.get("consistency");
				req.response().end();
			}
		});
		routeMatcher.noMatch(new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().putHeader("Content-Type", "text/html");
				String response = "Not found.";
				req.response().putHeader("Content-Length", String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
			}
		});
		server.requestHandler(routeMatcher);
		server.listen(8080);
	}

	public static int myHash(String s) {
		int i = (Math.abs(s.hashCode()) - 1) % 3 + 1;
		return i;
	}
}
