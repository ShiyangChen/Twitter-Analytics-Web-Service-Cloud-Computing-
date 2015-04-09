package hitman.phase1;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class Q2MySQL extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private Connection connection = null;
	public static final String UTF8 = "UTF-8";
	public static String TEAM_INFO = "HitMan,2136-3324-0103,3798-5854-9461,8668-4729-2265";
	public static String MYSQL_SERVER = "jdbc:mysql://localhost:3306/test";
	public static String MYSQL_USER = "ubuntu";
	public static String MYSQL_PASSWORD = "ubuntu";
	
	public Q2MySQL() {
		super();
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}		
		
		// Connect to mysql
		try {
			connection = DriverManager.getConnection(
					MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWORD);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
    
	 protected void doGet(HttpServletRequest request,
             HttpServletResponse response) throws ServletException, IOException {
	     ArrayList<String> tweets = new ArrayList<String>();
	     // Get the parameters from URL
	     String userid = request.getParameter("userid");
	     String tweet_time = request.getParameter("tweet_time");
	     tweet_time = tweet_time.replaceFirst(" ", "+");
	
	     try {
	
	             Statement statement = connection.createStatement();
	             ResultSet resultSet = statement.executeQuery("select * from tweets where userid = " + userid + " and time = " + "\'" + tweet_time + "\'") ;
	             while (resultSet.next()) {
	            	 String content = resultSet.getString("text").replace(" fuck ",",");
	            	 content = content.replace(" ass ","\n");
	                 tweets.add(resultSet.getString("tweetid")+ ":" +resultSet.getString("score")+":" + content);
	             }
	             resultSet.close();
	             statement.close();
	     } catch (SQLException e) {
	             response.getWriter().println(e);
	     }
	
	     // Sort
	     Collections.sort(tweets);
	
	     response.setContentType("text/xml; charset=utf-8");
	     response.getWriter().println(TEAM_INFO);
	     for (String s : tweets) {
	             response.getWriter().println(s);
	     }
	 }
}
