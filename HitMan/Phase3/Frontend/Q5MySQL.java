package hitman.frontend;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.servlet.ServletConfig;
//import javax.servlet.annotation.WebServlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;

/**
 * Servlet implementation class q3
 */
//@WebServlet("/q5")
public class Q5MySQL extends HttpServlet {
	private static String TEAM_INFO = "HitMan,2136-3324-0103,3798-5854-9461,8668-4729-2265\n";
	/**
	 * 
	 */
	private static final long serialVersionUID = 7644554169405828628L;
	private Statement stmt = null;
	private ResultSet rs = null;
	private DataSource datasource = null;
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Q5MySQL() {
        super();
    }

	/**
	 * @see Servlet#init(ServletConfig)
	 */
	public void init(ServletConfig config) throws ServletException {
		
		PoolProperties p = new PoolProperties();
		p.setUrl("jdbc:mysql://localhost:3306/test");
		p.setDriverClassName("com.mysql.jdbc.Driver");
		p.setUsername("ubuntu");
		p.setPassword("ubuntu");
        p.setJmxEnabled(true);
		p.setTestOnBorrow(false);
		p.setValidationQuery("SELECT 1");
		p.setTestOnReturn(false);
		p.setValidationInterval(30000);
		p.setTimeBetweenEvictionRunsMillis(30000);
		p.setMaxActive(500);
		p.setInitialSize(100);
		p.setMaxWait(10000);
		p.setRemoveAbandonedTimeout(60);
		p.setMinEvictableIdleTimeMillis(50000);
		p.setMinIdle(20);
		p.setRemoveAbandoned(true);
		p.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;"
				+ "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer");
		datasource = new DataSource();
		datasource.setPoolProperties(p);
        
	}

	/**
	 * @see Servlet#destroy()
	 */
	public void destroy() {
		try {
			   if(rs != null) {
				   rs.close();
				   rs = null;
			   }
			   if(stmt != null) {
			    stmt.close();
			    stmt = null;
			   }
			  
			} catch(Exception e) {
				System.out.println("Database close error");
				e.printStackTrace();
			}
		super.destroy();
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		Connection conn = null;
		try {
			conn = datasource.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		    
		response.setContentType("text/plain");
	    String useridA = (String)request.getParameter("m");
	    String useridB = (String)request.getParameter("n");
	  
	    int AScore1=0, AScore2=0, AScore3=0, AScore=0;
	    int BScore1=0, BScore2=0, BScore3=0, BScore=0;

	    PrintWriter out = response.getWriter();
	    StringBuilder result = new StringBuilder();
	    
	    try {
	    	// get scores for A
			stmt = conn.createStatement();
			StringBuilder sql1 = new StringBuilder();
			sql1.append("SELECT * FROM q5tweets WHERE userid = ");
			sql1.append(useridA);
//			sql1.append("'");
			rs = stmt.executeQuery(sql1.toString());
			while(rs.next()) {
				AScore1 = rs.getInt("score1");
				AScore2 = rs.getInt("score2");
				AScore3 = rs.getInt("score3");
				AScore = AScore1+AScore2+AScore3;
			}
	    	// get scores for B
			stmt = conn.createStatement();
			StringBuilder sql2 = new StringBuilder();
			sql2.append("SELECT * FROM q5tweets WHERE userid = ");
			sql2.append(useridB);
//			sql2.append("'");
			rs = stmt.executeQuery(sql2.toString());
			while(rs.next()) {
				BScore1 = rs.getInt("score1");
				BScore2 = rs.getInt("score2");
				BScore3 = rs.getInt("score3");
				BScore = BScore1+BScore2+BScore3;
			}

		    result.append(TEAM_INFO);
		    
		    result.append(useridA);
		    result.append("\t");
		    result.append(useridB);
		    result.append("\t");
		    result.append("WINNER\n");
		    
		    result.append(AScore1);
		    result.append("\t");
		    result.append(BScore1);
		    result.append("\t");
		    result.append(AScore1>BScore1?useridA:(AScore1<BScore1?useridB:"X"));
		    result.append("\n");
		    
		    result.append(AScore2);
		    result.append("\t");
		    result.append(BScore2);
		    result.append("\t");
		    result.append(AScore2>BScore2?useridA:(AScore2<BScore2?useridB:"X"));
		    result.append("\n");
		    
		    result.append(AScore3);
		    result.append("\t");
		    result.append(BScore3);
		    result.append("\t");
		    result.append(AScore3>BScore3?useridA:(AScore3<BScore3?useridB:"X"));
		    result.append("\n");
		    
		    result.append(AScore);
		    result.append("\t");
		    result.append(BScore);
		    result.append("\t");
		    result.append(AScore>BScore?useridA:(AScore<BScore?useridB:"X"));
		    result.append("\n");
	    }
		catch (SQLException e) {
			System.out.println("Manupulating operations go wrong!");
			e.printStackTrace();
		}
	    
	    finally {
  	      if (conn != null) 
  	    	  try {
  	    		  conn.close();
  	    	  }
  	      catch (Exception ignore) {}
  	    }
	    
	    out.print(result.toString());
	}
}