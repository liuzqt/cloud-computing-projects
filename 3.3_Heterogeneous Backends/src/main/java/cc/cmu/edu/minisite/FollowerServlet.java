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
import org.json.JSONArray;
import org.json.JSONObject;




public class FollowerServlet extends HttpServlet {

    private static String zkAddr = System.getenv("HBASE_HOST");
    private static final String tableName = "linksdb";
	private static Table linksTable;
	private static org.apache.hadoop.hbase.client.Connection conn_Hbase;
    private static final Logger LOGGER = Logger.getRootLogger();
    
    private static java.sql.Connection conn_MySQL;
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "liuzq12";
    private static final String HOST = System.getenv("MYSQL_HOST");
    private static final String URL = "jdbc:mysql://"+ HOST +"/" + DB_NAME + "?useSSL=false";
    private static final String DB_USER = System.getenv("DB_USER");
    private static final String DB_PWD = System.getenv("DB_PWD");

    public FollowerServlet(){
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
    	
        String id = request.getParameter("id");
        JSONObject result = new JSONObject();

        Scan scan = new Scan();
        BinaryComparator comp = new BinaryComparator(Bytes.toBytes(id));       
        RowFilter rf = new RowFilter(CompareFilter.CompareOp.EQUAL, comp); 
        byte[] family = Bytes.toBytes("fler");
        
        scan.setFilter(rf);
        scan.addFamily(family);
        ResultScanner rs = linksTable.getScanner(scan);
        linksTable.close();
        Result r = rs.next();
        rs.close();
        List<String[]> followerList = new ArrayList<String[]>();
		if (r != null) {
			String name = "";
			String url = "";
			Cell[] cells = r.rawCells();
			
			for (Cell c : cells) {
				String s = Bytes.toString(c.getValueArray(),c.getValueOffset(),c.getValueLength());
				System.out.println(s);
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
//		System.out.println(result.toString());
        /*
            Task 2:
            Implement your logic to retrive the followers of this user. 
            You need to send back the Name and Profile Image URL of his/her Followers.

            You should sort the followers alphabetically in ascending order by Name. 
            If there is a tie in the followers name, 
	    sort alphabetically by their Profile Image URL in ascending order. 
        */
//		try {
//			cleanup();
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
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

class FollowerComparator implements Comparator<String[]> {
	public int compare(String[] arg0, String[] arg1) {
		int temp = arg0[0].compareTo(arg1[0]);
		if (temp == 0)
			return (arg0[1].compareTo(arg1[1]));
		else
			return temp;
	}
}


