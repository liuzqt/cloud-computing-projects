package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

public class ProfileServlet extends HttpServlet {
	
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "liuzq12";
    private static final String HOST = System.getenv("MYSQL_HOST");
    private static final String URL = "jdbc:mysql://"+ HOST +"/" + DB_NAME + "?useSSL=false";
    private static final String DB_USER = System.getenv("DB_USER");
    private static final String DB_PWD = System.getenv("DB_PWD");

    private static Connection conn;

    public ProfileServlet() {
    	try {
			initializeConnection();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

        /*
            Your initialization code goes here
        */
    }
    private static void initializeConnection() throws ClassNotFoundException, SQLException {
    	System.out.println("host is "+HOST);
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
    }
    
    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        JSONObject result = new JSONObject();

        String id = request.getParameter("id");
        String pwd = request.getParameter("pwd");
        String name = "";
        String url = "";

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            String sql = "SELECT `Name`,`Profile_Image_URL` from userinfo where UserID='" + id + "' and `Password` = '" + pwd + "'";

            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                name = rs.getString(1);
                url = rs.getString(2);
               
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

        System.out.println("name is:"+name);
        if(name.equals("")){
        	name = "Unauthorized";
        	url = "#";
        }
        result.put("name", name);
        result.put("profile", url);
        System.out.println(result.toString());
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
