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
import java.util.Date;
import java.util.List;

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
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
public class TimelineServlet extends HttpServlet {
	
	//HBase configuration
    private static String zkAddr = System.getenv("HBASE_HOST");
    private static final String tableName = "linksdb";
	private static Table linksTable;
	private static org.apache.hadoop.hbase.client.Connection conn_Hbase;
    private static final Logger LOGGER = Logger.getRootLogger();
    
    //MYSQL configuration
    private static java.sql.Connection conn_MySQL;
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "liuzq12";
    private static final String HOST = System.getenv("MYSQL_HOST");
    private static final String URL = "jdbc:mysql://"+ HOST +"/" + DB_NAME + "?useSSL=false";
    private static final String DB_USER = System.getenv("DB_USER");
    private static final String DB_PWD = System.getenv("DB_PWD");
	
    //Mongo configuration
	private static MongoClient mongoClient;
	private static final String MONGO_HOST = System.getenv("MONGO_HOST");
	private static final String DB = "postdb";
	private static final String CL = "posts";
	private static MongoCollection<Document> collection;
	
	private static List<JSONObject> temp; //used for post sorting

    public TimelineServlet() throws Exception {
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
    	
    	System.out.println("initializing mongoDB...");
    	System.out.println(MONGO_HOST);
    	initializeConnection_Mongo();
    	MongoDatabase db = mongoClient.getDatabase(DB);
    	collection = db.getCollection(CL);
    	System.out.println("mongoDB connection created!");
    }

    private static void initializeConnection_Mongo(){
    	mongoClient = new MongoClient(MONGO_HOST,27017);
    }
    
    private static void initializeConnection_Hbase() throws IOException {
    	System.out.println("initializing HBASE....");
    	System.out.println(zkAddr);
    	
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
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
    
    @Override
    protected void doGet(final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        JSONObject result = new JSONObject();
        String id = request.getParameter("id");
    	linksTable = conn_Hbase.getTable(TableName.valueOf(tableName));
    	System.out.println("table get!");
    	
    	//part1: profile
        String profile_name = "";
        String profile_url = "";
        Statement stmt1 = null;
        try {
            stmt1 = conn_MySQL.createStatement();
            String sql = "SELECT `Name`,`Profile_Image_URL` from userinfo where UserID='" + id + "'";

            ResultSet rs = stmt1.executeQuery(sql);
            if (rs.next()) {
                profile_name = rs.getString(1);
                profile_url = rs.getString(2);  
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt1 != null) {
                try {
                    stmt1.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        result.put("name", profile_name);
        result.put("profile", profile_url);

        
        //part2: followers
        Scan scan = new Scan();
        BinaryComparator comp = new BinaryComparator(Bytes.toBytes(id));       
        RowFilter rf = new RowFilter(CompareFilter.CompareOp.EQUAL, comp); 
        byte[] family = Bytes.toBytes("fler");
        
        scan.setFilter(rf);
        scan.addFamily(family);
        ResultScanner rs = linksTable.getScanner(scan);
        Result r = rs.next();
        rs.close();
        List<String[]> followerList = new ArrayList<String[]>();
		if (r != null) {
			String name = "";
			String url = "";
			Cell[] cells = r.rawCells();
			
			for (Cell c : cells) {
				String s = Bytes.toString(c.getValueArray(),c.getValueOffset(),c.getValueLength());
				System.out.println("follower:"+s);
				Statement stmt = null;
				try {
					stmt = conn_MySQL.createStatement();
					String sql = "SELECT `Name`,`Profile_Image_URL` from userinfo where UserID='" + s + "'";
					
					ResultSet rs1 = stmt.executeQuery(sql);
					if (rs1.next()) {
						name = rs1.getString(1);
						url = rs1.getString(2);
						System.out.println(name+"  "+url);
						String[] temp = {name,url};
						followerList.add(temp);
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

			JSONArray followerArray = new JSONArray();
			Collections.sort(followerList,new FollowerComparator());
			for(String[] s:followerList){
				JSONObject follower = new JSONObject();
				follower.put("name", s[0]);
				follower.put("profile", s[1]);
				followerArray.put(follower);
			}
			result.put("followers", followerArray);

		}
		
		//part3:followees' post
		JSONArray postArray = new JSONArray();
		scan = new Scan();
        BinaryComparator comp1 = new BinaryComparator(Bytes.toBytes(id));       
        RowFilter rf1 = new RowFilter(CompareFilter.CompareOp.EQUAL, comp1); 
        byte[] family1 = Bytes.toBytes("flee");
        
        scan.setFilter(rf1);
        scan.addFamily(family1);
        ResultScanner rs1 = linksTable.getScanner(scan);
        Result r1 = rs1.next();
        rs1.close();
		if (r1 != null) {
			Cell[] cells = r1.rawCells();
			
			// iterate every followee
			temp = new ArrayList<JSONObject>();
			for (Cell c : cells) {
				String s = Bytes.toString(c.getValueArray(),c.getValueOffset(),c.getValueLength());
				System.out.println("followee:"+s);
				try {
		
			        FindIterable<Document> find = collection.find(new BasicDBObject("uid", Integer.parseInt(s)));
//			        System.out.println("found!");
	      
			        find.forEach(new Block<Document>() {
						public void apply(Document arg0) {
							JSONObject jsObject = new JSONObject(arg0.toJson());
							temp.add(jsObject);
						}
					});		        

				} catch (Exception e) {
					e.printStackTrace();
				} 
			}

	        Collections.sort(temp, new postComparator_des());
	        int length=0;
	        if(temp.size()>=30)
	        	length = 30;
	        else
	        	length = temp.size();
	        
	        ArrayList<JSONObject> postList = new ArrayList<JSONObject>();
	        for(int i=0;i<length;i++){
	        	postList.add(temp.get(i));
	        }
	        Collections.sort(postList,new postComparator());
	        for(JSONObject j:postList){
	        	System.out.println(j.getString("timestamp"));
	        	postArray.put(j);
	        }
	        result.put("posts", postArray);
		}
		
		linksTable.close();
//		try {
//			cleanup();
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		
		
        /*
            Task 4 (1):
            Get the name and profile of the user as you did in Task 1
            Put them as fields in the result JSON object
        */

        /*
            Task 4 (2);
            Get the follower name and profiles as you did in Task 2
            Put them in the result JSON object as one array
        */

        /*
            Task 4 (3):
            Get the 30 LATEST followee posts and put them in the
            result JSON object as one array.

            The posts should be sorted:
            First in ascending timestamp order
            Then numerically in ascending order by their PID (PostID) 
	    if there is a tie on timestamp
        */
        
        PrintWriter out = response.getWriter();
        out.print(String.format("returnRes(%s)", result.toString()));
        out.close();
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }
    
}

class postComparator_des implements Comparator<JSONObject> {
	
	public int compare(JSONObject o1,JSONObject o2) {
		Date d1 = DateStringParser.parse(o1.getString("timestamp"));
		Date d2 = DateStringParser.parse(o2.getString("timestamp"));
		int temp = d2.compareTo(d1);
		if(temp!=0)
			return temp;
		else{
			int id1 = o1.getInt("pid");
			int id2 = o2.getInt("pid");
			return id1-id2;
		}
	}
}


