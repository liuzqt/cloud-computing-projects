package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class HomepageServlet extends HttpServlet {
	
	private static MongoClient mongoClient;
	private static final String MONGO_HOST = System.getenv("MONGO_HOST");
	private static final String DB = "postdb";
	private static final String CL = "posts";
	private static MongoCollection<Document> collection;

	
    public HomepageServlet() {
    	System.out.println("initializing mongoDB...");
    	System.out.println(MONGO_HOST);
    	initializeConnection_Mongo();
    	MongoDatabase db = mongoClient.getDatabase(DB);
    	collection = db.getCollection(CL);
    	System.out.println("mongoDB connection created!");
        /*
            Your initialization code goes here
        */
    }

    private static void initializeConnection_Mongo(){
    	mongoClient = new MongoClient(MONGO_HOST,27017);
    }
    
    private static void cleanup() throws IOException {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
    
    
    @Override
    protected void doGet(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {

        String id = request.getParameter("id");
        JSONObject result = new JSONObject();

        try{

        FindIterable<Document> find = collection.find(new BasicDBObject("uid", Integer.parseInt(id))).sort(new BasicDBObject("timestamp",1));
        System.out.println("found!");
//        MongoCursor<Document> iterator = find.iterator();
        
        final JSONArray posts = new JSONArray();

        find.forEach(new Block<Document>() {

			public void apply(Document arg0) {
				JSONObject jsObject = new JSONObject(arg0.toJson());
				posts.put(jsObject);
				
			}
		});

        result.put("posts", posts);
        System.out.println("finished!");
        }catch(Exception e){
        	e.printStackTrace();
        }
        
        /*
            Task 3:
            Implement your logic to return all the posts authored by this user.
            Return this posts as-is, but be cautious with the order.

            You will need to sort the posts by Timestamp in ascending order
	     (from the oldest to the latest one). 
        */
//        cleanup();
        PrintWriter writer = response.getWriter();           
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }
}
class DateStringParser {

	public static Date parse(String dateString) {
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			date = sdf.parse(dateString);
		}

		catch (ParseException e) {
			System.out.println(e.getMessage());
		}
		return date;
	}
	public static String format(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// HH:mm:ss
		String str;
		str = sdf.format(date);

		return str;
	}
}

class postComparator implements Comparator<JSONObject> {
	
	public int compare(JSONObject o1,JSONObject o2) {
		Date d1 = DateStringParser.parse(o1.getString("timestamp"));
		Date d2 = DateStringParser.parse(o2.getString("timestamp"));
		int temp = d1.compareTo(d2);
		if(temp!=0)
			return temp;
		else{
			System.out.println("fuck!");
			int id1 = o1.getInt("pid");
			int id2 = o2.getInt("pid");
			return id1-id2;
		}
	}
}



