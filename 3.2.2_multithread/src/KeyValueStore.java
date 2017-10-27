import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Queue;
import java.util.Set;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class KeyValueStore extends Verticle {
	private static ConcurrentHashMap<String, PriorityQueue<Long>> currentKey = new ConcurrentHashMap<String, PriorityQueue<Long>>();
	private static ConcurrentHashMap<String, String> store = new ConcurrentHashMap<String, String>();
	private static ConcurrentHashMap<String, PriorityQueue<Long>> latestOperation = new ConcurrentHashMap<String, PriorityQueue<Long>>();
	/* TODO: Add code to implement your backend storage */

	@Override
	public void start() {
		final KeyValueStore keyValueStore = new KeyValueStore();
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);
		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				String value = map.get("value");
				String consistency = map.get("consistency");
				Integer region = Integer.parseInt(map.get("region"));
				// System.out.println(consistency);
				Long timestamp = Long.parseLong(map.get("timestamp"));

				if (consistency.equals("strong")) {
					Thread t = new Thread(new Runnable() {
						public void run() {
							// check timeStampQueue, thread-safety implemented.
							long currentTs = 0;

							do {
								wait_50ms();
								synchronized (currentKey) {
									currentTs = currentKey.get(key).peek();
								}
							} while (timestamp > currentTs);

							System.out.println("put" + timestamp + " " + key + " begin");
							store.put(key, value);

							// operation done, remove key from Queue
							synchronized (currentKey) {
								PriorityQueue<Long> queue = currentKey.get(key);
								queue.remove();
								if (queue.isEmpty()) {
									if (queue.isEmpty()) {
										currentKey.remove(key);
									}
								} else {
									currentKey.replace(key, queue);
								}
							}
							System.out.println("put" + timestamp + " " + key + " finish");
						}
					});
					t.start();
				}
				// eventual consistency
				else {
					Thread t = new Thread(new Runnable() {
						public void run() {
							System.out.println("put" + timestamp + " " + key + " " + value + " arrive");
							boolean stale = false;
							// check whether stale
							synchronized (latestOperation) {
								if (!latestOperation.containsKey(key)) {
									PriorityQueue<Long> timeQueue = new PriorityQueue<Long>(3, new reverseComparator());
									timeQueue.add(timestamp);
									latestOperation.put(key, timeQueue);
								} else {
									if (latestOperation.get(key).peek() > timestamp) {
										stale = true;
									} else {
										PriorityQueue<Long> timeQueue = latestOperation.get(key);
										timeQueue.add(timestamp);
										latestOperation.replace(key, timeQueue);
									}
								}

							}
							if (stale) {
								System.out.println("Stale value, discarded!");
							} else {
								synchronized (currentKey) {
									if (!currentKey.containsKey(key)) {
										PriorityQueue<Long> queue = new PriorityQueue<Long>();
										queue.add(timestamp);
										currentKey.put(key, queue);
									} else {
										PriorityQueue<Long> queue = currentKey.get(key);
										queue.add(timestamp);
										currentKey.replace(key, queue);
									}
								}

								// check timeStampQueue, thread-safety
								// implemented.
								long currentTs = 0;

								do {
									wait_50ms();
									synchronized (currentKey) {
										currentTs = currentKey.get(key).peek();
									}
								} while (timestamp > currentTs);

								System.out.println("put" + timestamp + " " + key + " " + value + " begin");

								synchronized (store) {
									if (store.containsKey(key)) {
										store.replace(key, value);
									} else {
										store.put(key, value);
									}								
								}

								// operation done, remove key from Queue
								synchronized (currentKey) {
									PriorityQueue<Long> queue = currentKey.get(key);
									queue.remove();
									if (queue.isEmpty()) {
										if (queue.isEmpty()) {
											currentKey.remove(key);
										}
									} else {
										currentKey.replace(key, queue);
									}
								}
								System.out.println("put" + timestamp + " " + key + " " + value + " finish");
							}

						}
					});
					t.start();
				}

				String response = "stored";
				req.response().putHeader("Content-Type", "text/plain");
				req.response().putHeader("Content-Length", String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
			}
		});
		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				String consistency = map.get("consistency");
				final Long timestamp = Long.parseLong(map.get("timestamp"));

				if (consistency.equals("strong")) {
					Thread t = new Thread(new Runnable() {
						public void run() {
							String response = "";
							synchronized (currentKey) {
								if (!currentKey.containsKey(key)) {
									PriorityQueue<Long> queue = new PriorityQueue<Long>();
									queue.add(timestamp);
									currentKey.put(key, queue);
								} else {
									PriorityQueue<Long> queue = currentKey.get(key);
									queue.add(timestamp);
									currentKey.replace(key, queue);
								}
							}
							System.out.println("get" + timestamp + " " + key + " arrive");
							// check timeStampQueue, thread-safety implemented.
							long currentTs = 0;
							do {
								wait_50ms();
								synchronized (currentKey) {
									currentTs = currentKey.get(key).peek();
								}

							} while (timestamp > currentTs);
							System.out.println("get" + timestamp + " " + key + " begin");

							response = store.get(key);
							if (response == null) {
								System.out.println("key non-exist!");
								response = "0";
							}

							synchronized (currentKey) {
								PriorityQueue<Long> queue = currentKey.get(key);
								queue.remove();
								if (queue.isEmpty()) {
									currentKey.remove(key);
								} else {
									currentKey.replace(key, queue);
								}
							}
							System.out.println("get" + timestamp + " " + key + " finish");
							req.response().putHeader("Content-Type", "text/plain");
							if (response != null)
								req.response().putHeader("Content-Length", String.valueOf(response.length()));
							req.response().end(response);
							req.response().close();

						}
					});
					t.start();
				}
				// eventual consistency
				else {
					Thread t = new Thread(new Runnable() {
						public void run() {
							String response = "";
							
							System.out.println("get" + timestamp + " " + key + " arrive");
							System.out.println("get" + timestamp + " " + key + " begin");

							response = store.get(key);
							if (response == null) {
								System.out.println("key non-exist!");
								response = "0";
							}

							System.out.println("get" + timestamp + " " + key + " finish");
							req.response().putHeader("Content-Type", "text/plain");
							if (response != null)
								req.response().putHeader("Content-Length", String.valueOf(response.length()));
							req.response().end(response);
							req.response().close();

						}
					});
					t.start();
				}

			}
		});
		// Clears this stored keys. Do not change this
		routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				store.clear();
				latestOperation.clear();
				System.out.println("flush-----------------------------------------------------------");
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});
		// Handler for when the AHEAD is called
		routeMatcher.get("/ahead", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));

				Thread t = new Thread(new Runnable() {
					public void run() {
						synchronized (currentKey) {
							if (!currentKey.containsKey(key)) {
								PriorityQueue<Long> queue = new PriorityQueue<Long>();
								queue.add(timestamp);
								currentKey.put(key, queue);
							} else {
								PriorityQueue<Long> queue = currentKey.get(key);
								queue.add(timestamp);
								currentKey.replace(key, queue);
							}
						}
						System.out.println("put" + timestamp + " " + key + " arrive");
					}
				});
				t.start();
				/* TODO: Add code to handle the signal here if you wish */
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});
		// Handler for when the COMPLETE is called
		routeMatcher.get("/complete", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				/* TODO: Add code to handle the signal here if you wish */
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
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

class reverseComparator implements Comparator<Long> {
	@Override
	public int compare(Long o1, Long o2) {
		if (o1 > o2)
			return -1;
		else if (o1 < o2)
			return 1;
		else
			return 0;
	}
}