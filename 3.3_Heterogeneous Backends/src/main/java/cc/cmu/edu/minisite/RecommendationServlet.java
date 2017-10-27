package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

public class RecommendationServlet extends HttpServlet {

	// HBase configuration
	private static String zkAddr = System.getenv("HBASE_HOST");
	private static final String tableName = "linksdb";
	private static Table linksTable;
	private static org.apache.hadoop.hbase.client.Connection conn_Hbase;
	private static final Logger LOGGER = Logger.getRootLogger();

	// MYSQL configuration
	private static java.sql.Connection conn_MySQL;
	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_NAME = "liuzq12";
	private static final String HOST = System.getenv("MYSQL_HOST");
	private static final String URL = "jdbc:mysql://" + HOST + "/" + DB_NAME + "?useSSL=false";
	private static final String DB_USER = System.getenv("DB_USER");
	private static final String DB_PWD = System.getenv("DB_PWD");

	private static HashMap<String, Integer[]> candidates;// Integer[0]:rate,Integer[1]:uid

	private static PriorityQueue<String> queue;
	private static ConcurrentHashMap<String, String> followees;
	private static AtomicInteger count;

	public RecommendationServlet() throws Exception {
		try {
			initializeConnection_Hbase();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try {
			initializeConnection_MySQL();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	private static void initializeConnection_Hbase() throws IOException {
		System.out.println("initializing HBASE....");
		System.out.println(zkAddr);

		// Remember to set correct log level to avoid unnecessary output.
		LOGGER.setLevel(Level.ERROR);
		if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
			System.out.print("Malformed HBase IP address");
			System.exit(-1);
		}
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", zkAddr + ":60000");
		conf.set("hbase.zookeeper.quorum", zkAddr);
		conf.set("hbase.zookeeper.property.clientport", "2181");
		conn_Hbase = ConnectionFactory.createConnection(conf);
		System.out.println("connection initialized!");

	}

	private static void initializeConnection_MySQL() throws ClassNotFoundException, SQLException {
		System.out.println("initializing MYSQL connection....");
		Class.forName(JDBC_DRIVER);
		conn_MySQL = DriverManager.getConnection(URL, DB_USER, DB_PWD);
		System.out.println("MYSQL connection initialized!");
	}
	
    private static void cleanup() throws IOException, SQLException {
        if (linksTable != null) {
            linksTable.close();
        }
        if (conn_Hbase != null) {
            conn_Hbase.close();
        }
        if(conn_MySQL != null){
        	conn_MySQL.close();
        }

    }

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
			throws ServletException, IOException {
		linksTable = conn_Hbase.getTable(TableName.valueOf(tableName));
		System.out.println("table get!");

		JSONObject result = new JSONObject();
		final String id = request.getParameter("id");

		
		candidates = new HashMap<String, Integer[]>();// Integer[0]:rate,Integer[1]:uid
		followees = new ConcurrentHashMap<String,String>();

		JSONArray candidateArray = new JSONArray();
		Scan scan = new Scan();
		BinaryComparator comp1 = new BinaryComparator(Bytes.toBytes(id));
		RowFilter rf1 = new RowFilter(CompareFilter.CompareOp.EQUAL, comp1);
		final byte[] family1 = Bytes.toBytes("flee");

		scan.setFilter(rf1);
		scan.addFamily(family1);
		ResultScanner rs1 = linksTable.getScanner(scan);
		Result r1 = rs1.next();
		rs1.close();
		if (r1 != null) {
			Cell[] cells = r1.rawCells();

			// store followees in HashSet
			queue = new PriorityQueue<String>();
			for (Cell c : cells) {
				String s = Bytes.toString(c.getValueArray(), c.getValueOffset(), c.getValueLength());
				followees.put(s, "1");
				queue.add(s);
			}
			System.out.println("size:" + followees.size());
			System.out.println("size:" + candidates.size());
			System.out.println("size:" + followees.toString());


			ExecutorService fixedThreadPool = Executors.newFixedThreadPool(10);
			for (int i = 0; i < followees.size(); i++) {
				final int index = i;
				System.out.println(i);
				try {
					Thread.sleep(400);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				fixedThreadPool.execute(new Runnable() {
					public void run() {
						String s = new String();
						synchronized (queue) {
							s = queue.poll();
						}
						System.out.println("followee:" + s);
						try {
							Scan sub_scan = new Scan();
							BinaryComparator comp_sub = new BinaryComparator(Bytes.toBytes(s));
							RowFilter rf_sub = new RowFilter(CompareFilter.CompareOp.EQUAL, comp_sub);

							sub_scan.setFilter(rf_sub);
							sub_scan.addFamily(family1);
							ResultScanner rs_sub = linksTable.getScanner(sub_scan);
							Result r_sub = rs_sub.next();
							rs_sub.close();

							if (r_sub != null) {
								Cell[] cells_sub = r_sub.rawCells();
								for (Cell c_sub : cells_sub) {
									String s_sub = Bytes.toString(c_sub.getValueArray(), c_sub.getValueOffset(),
											c_sub.getValueLength());
//									System.out.println("sub followee" + s_sub);
									if (s_sub.equals(id)) {
										// System.out.println("equal");
										continue;
									}

									else {
								
											if (followees.containsKey(s_sub)) {
												// System.out.println("contains");
												continue;
											}

											else {
												// System.out.println("pass");
												synchronized (candidates) {
													if (candidates.containsKey(s_sub)) {
														Integer[] tempCandidate = candidates.get(s_sub);
														tempCandidate[0]++;
														candidates.replace(s_sub, tempCandidate);
													} else {
														Integer[] candidate = { 1, Integer.parseInt(s_sub) };
														candidates.put(s_sub, candidate);
													}
												}

											}
										

									}

								}
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
//						System.out.println(count.addAndGet(1)+"finished!");
					}
				});
			}

			fixedThreadPool.shutdown();
			while (!fixedThreadPool.isTerminated()) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

			System.out.println(candidates.size());
			ArrayList<Map.Entry<String, Integer[]>> candidateList = new ArrayList<Map.Entry<String, Integer[]>>(
					candidates.entrySet());

			System.out.println("begin sorting! " + candidateList.size());

			Collections.sort(candidateList, new candidateComparator());

			for (Map.Entry<String, Integer[]> e : candidateList) {
				System.out.println(e.getKey() + " " + e.getValue()[0] + " " + e.getValue()[1]);
			}
			System.out.println("----------");
			int length = 0;
			if (candidateList.size() >= 10)
				length = 10;
			else
				length = candidateList.size();
			for (int i = 0; i < length; i++) {

				String uid = String.valueOf(candidateList.get(i).getValue()[1]);
				String rate = String.valueOf(candidateList.get(i).getValue()[0]);
				System.out.println("candidate:" + rate + " " + uid);
				Statement stmt = null;
				try {
					stmt = conn_MySQL.createStatement();
					String sql = "SELECT `Name`,`Profile_Image_URL` from userinfo where UserID='" + uid + "'";
					ResultSet rs = stmt.executeQuery(sql);
					if (rs.next()) {
						String name = rs.getString(1);
						String url = rs.getString(2);
						JSONObject jo = new JSONObject();
						jo.put("name", name);
						jo.put("profile", url);
						candidateArray.put(jo);
					}

				} catch (SQLException e) {
					e.printStackTrace();
				} finally {
					if (stmt != null) {
						try {
							stmt.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
				}

			}
			result.put("recommendation", candidateArray);
		}
		linksTable.close();
//		try {
//			cleanup();
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		/**
		 * Bonus task:
		 * 
		 * Recommend at most 10 people to the given user with simple
		 * collaborative filtering.
		 * 
		 * Store your results in the result object in the following JSON format:
		 * recommendation: [ {name:<name_1>, profile:<profile_1>}
		 * {name:<name_2>, profile:<profile_2>} {name:<name_3>,
		 * profile:<profile_3>} ... {name:<name_10>, profile:<profile_10>} ]
		 * 
		 * Notice: make sure the input has no duplicate!
		 */

		PrintWriter writer = response.getWriter();
		writer.write(String.format("returnRes(%s)", result.toString()));
		writer.close();

	}

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request, response);
	}
}

class candidateComparator implements Comparator<Map.Entry<String, Integer[]>> {

	public int compare(Entry<String, Integer[]> o1, Entry<String, Integer[]> o2) {
		int temp = o2.getValue()[0] - o1.getValue()[0];
		if (temp != 0)
			return temp;
		else
			return o1.getValue()[1] - o2.getValue()[1];
	}

}
