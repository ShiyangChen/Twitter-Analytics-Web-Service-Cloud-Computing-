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
 * Servlet implementation class q4
 */
// @WebServlet("/q4")
public class Q4MySQL extends HttpServlet {
	private static String TEAM_INFO = "HitMan,2136-3324-0103,3798-5854-9461,8668-4729-2265\n";
	private static final long serialVersionUID = 1L;
	private Statement statement = null;
	private ResultSet resultSet = null;
	private DataSource datasource = null;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public Q4MySQL() {
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
			if (resultSet != null) {
				resultSet.close();
				resultSet = null;
			}
			if (statement != null) {
				statement.close();
				statement = null;
			}

		} catch (Exception e) {
			System.out.println("Database close error");
			e.printStackTrace();
		}
		super.destroy();
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		Connection conn = null;
		try {
			conn = datasource.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		String location = request.getParameter("location");
	    String tweet_time = request.getParameter("date");
	    String lowRank = request.getParameter("m");
	    String highRank = request.getParameter("n");
	    
	    response.setContentType("text/html; charset=UTF-8");
	    response.setCharacterEncoding("UTF-8");
	    PrintWriter out = response.getWriter();
	    
		StringBuilder result = new StringBuilder();

		result.append(TEAM_INFO);

		String value = "";
		try {
			// get scores for A
			statement = conn.createStatement();
			resultSet = statement
					.executeQuery("SELECT * FROM q4tweets WHERE dateLocation = \'" + tweet_time + location+
                            "\' and rank >= " + lowRank + " and rank <= " + highRank);
			
			while (resultSet.next()) {
				
				value = resultSet.getString("tagnID");
				result.append(value + "\n");
			}
			
		} catch (SQLException e) {
			System.out.println("Manupulating operations go wrong!");
			e.printStackTrace();
		}

		finally {
			if (conn != null)
				try {
					conn.close();
				} catch (Exception ignore) {
				}
		}
		out.print(result.toString());
	}
}