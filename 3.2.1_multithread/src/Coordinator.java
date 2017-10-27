import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

public class Coordinator extends Verticle {

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances
	 */
	private static final String dataCenter1 = "ec2-54-162-119-107.compute-1.amazonaws.com";
	private static final String dataCenter2 = "ec2-52-207-239-44.compute-1.amazonaws.com";
	private static final String dataCenter3 = "ec2-54-159-157-115.compute-1.amazonaws.com";
	private static ConcurrentHashMap<String, ArrayList<PriorityQueue<Long>>> currentKey = new ConcurrentHashMap<String, ArrayList<PriorityQueue<Long>>>();

	@Override
	public void start() {
		// DO NOT MODIFY THIS
		KeyValueLib.dataCenters.put(dataCenter1, 1);
		KeyValueLib.dataCenters.put(dataCenter2, 2);
		KeyValueLib.dataCenters.put(dataCenter3, 3);
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);

		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String value = map.get("value");
				// You may use the following timestamp for ordering requests
//				final String timestamp = new Timestamp(
//						System.currentTimeMillis() + TimeZone.getTimeZone("EST").getRawOffset()).toString();
//				System.out.println(timestamp);
				final long timestamp = System.currentTimeMillis() + TimeZone.getTimeZone("EST").getRawOffset();
				Thread t = new Thread(new Runnable() {
					public void run() {
						// insert (key,timestampQueue) into map, thread-safety
						// implemented.
						synchronized (currentKey) {
							if (!currentKey.containsKey(key)) {
								PriorityQueue<Long> PUTqueue = new PriorityQueue<Long>();
								PUTqueue.add(timestamp);
								PriorityQueue<Long> GETqueue = new PriorityQueue<Long>();
								ArrayList<PriorityQueue<Long>> list = new ArrayList<PriorityQueue<Long>>();
								list.add(GETqueue);
								list.add(PUTqueue);
								currentKey.put(key, list);
							} else {
								ArrayList<PriorityQueue<Long>> list = currentKey.get(key);
								PriorityQueue<Long> queue = list.get(1);
								queue.add(timestamp);
								list.set(1, queue);
								currentKey.replace(key, list);
							}
						}
						System.out.println("put"+timestamp+" "+key+" arrive");
						// check timeStampQueue, thread-safety implemented.
						long currentTs1 = 0;
						long currentTs2 = 0;
						do {
							wait_50ms();
							synchronized (currentKey) {
								if (currentKey.get(key).get(0).isEmpty()) {
									currentTs1 = timestamp + 1;
									currentTs2 = currentKey.get(key).get(1).peek();
								} else {
									currentTs1 = currentKey.get(key).get(0).peek();
									currentTs2 = currentKey.get(key).get(1).peek();
								}

							}

						} while (timestamp > currentTs1 || timestamp > currentTs2);

						String[] dataCenters = { dataCenter1, dataCenter2, dataCenter3 };

						System.out.println("put"+timestamp+" "+key+" begin");
						int complete = 0;
						while (complete != 3) {
							complete = 0;
							for (String s : dataCenters) {
								try {
									KeyValueLib.PUT(s, key, value);
									complete++;
								} catch (IOException e) {
									e.printStackTrace();
									break;
								}
							}

						}
						
						synchronized (currentKey) {
							ArrayList<PriorityQueue<Long>> list = currentKey.get(key);
							PriorityQueue<Long> queue = list.get(1);
							queue.remove();
							if (queue.isEmpty()) {
								if (list.get(0).isEmpty()) {
									currentKey.remove(key);
								}
							} else {
								list.set(1, queue);
								currentKey.replace(key, list);
							}
						}
						System.out.println("put"+timestamp+" "+key+" finish");

						// TODO: Write code for PUT operation here.
						// Each PUT operation is handled in a different thread.
						// Highly recommended that you make use of helper
						// functions.
					}
				});
				t.start();

				// Every important notice should be repeated for three times
				// Do not remove this
				// Do not remove this
				// Do not remove this
				req.response().end();

			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String loc = map.get("loc");
				// You may use the following timestamp for ordering requests
//				final String timestamp = new Timestamp(
//						System.currentTimeMillis() + TimeZone.getTimeZone("EST").getRawOffset()).toString();
				final long timestamp = System.currentTimeMillis() + TimeZone.getTimeZone("EST").getRawOffset();
				Thread t = new Thread(new Runnable() {
					public void run() {
						String returnValue="null";
						synchronized (currentKey) {
							if (!currentKey.containsKey(key)) {
								PriorityQueue<Long> PUTqueue = new PriorityQueue<Long>();
								PriorityQueue<Long> GETqueue = new PriorityQueue<Long>();
								GETqueue.add(timestamp);
								ArrayList<PriorityQueue<Long>> list = new ArrayList<PriorityQueue<Long>>();
								list.add(GETqueue);
								list.add(PUTqueue);
								currentKey.put(key, list);
							} else {
								ArrayList<PriorityQueue<Long>> list = currentKey.get(key);
								PriorityQueue<Long> queue = list.get(0);
								queue.add(timestamp);
								list.set(0, queue);
								currentKey.replace(key, list);
							}
						}
						System.out.println("get"+timestamp+" "+key+" arrive");
						// check timeStampQueue, thread-safety implemented.
						long currentTs2 = 0;
						do {
							wait_50ms();
							synchronized (currentKey) {
								if (currentKey.get(key).get(1).isEmpty()) {
									currentTs2 = timestamp + 1;
								} else {
									currentTs2 = currentKey.get(key).get(1).peek();
								}

							}

						} while (timestamp > currentTs2);
						System.out.println("get"+timestamp+" "+key+" begin");
						
						String[] dataCenters = { dataCenter1, dataCenter2, dataCenter3 };
						
						try {
							returnValue = KeyValueLib.GET(dataCenters[Integer.parseInt(loc) - 1], key);
							if(returnValue.equals("null"))
								returnValue = "0";
						} catch (IOException e) {
							e.printStackTrace();

						}

						synchronized (currentKey) {
							ArrayList<PriorityQueue<Long>> list = currentKey.get(key);
							PriorityQueue<Long> queue = list.get(0);
							queue.remove(timestamp);//might be problematic...
							if (queue.isEmpty()) {
								if (list.get(1).isEmpty()) {
									currentKey.remove(key);
								}
							} else {
								list.set(0, queue);
								currentKey.replace(key, list);
							}

						}
						System.out.println("get"+timestamp+" "+key+" finish");
						// TODO: Write code for GET operation here.
						// Each GET operation is handled in a different thread.
						// Highly recommended that you make use of helper
						// functions.
						req.response().end(returnValue);
					}
				});
				t.start();
			}
		});

        routeMatcher.get("/flush", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                //Flush all datacenters before each test.
                URL url = null;
                try {
                    url = new URL("http://" + dataCenter1 + ":8080/flush");
                    url.openConnection();
                    url = new URL("http://" + dataCenter2 + ":8080/flush");
                    url.openConnection();
                    url = new URL("http://" + dataCenter3 + ":8080/flush");
                    url.openConnection();
                } catch (Exception e) {

                }
                //This endpoint will be used by the auto-grader to flush your datacenter before tests
                //You can initialize/re-initialize the required data structures here
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

	private static void wait_50ms() {
		try {
			Thread.sleep(50);
		} catch (InterruptedException e) {
			return;
		}
	}
}
