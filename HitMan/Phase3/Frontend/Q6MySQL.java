package hitman.frontend;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
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
 * Servlet implementation class q6
 */
// @WebServlet("/q6")
public class Q6MySQL extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static String TEAM_INFO = "HitMan,2136-3324-0103,3798-5854-9461,8668-4729-2265\n";
	private Statement statement1 = null;
	private Statement statement2 = null;
	private ResultSet resultSet1 = null;
	private ResultSet resultSet2 = null;
	private DataSource datasource = null;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public Q6MySQL() {
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
//		p.setMaxIdle(40);
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
			if (resultSet1 != null) {
				resultSet1.close();
				resultSet1 = null;
			}
			if (resultSet2 != null) {
				resultSet2.close();
				resultSet2 = null;
			}
			if (statement1 != null) {
				statement1.close();
				statement1 = null;
			}
			if (statement2 != null) {
				statement2.close();
				statement2 = null;
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

		// response.setContentType("text/plain");
		String m = request.getParameter("m");
		String n = request.getParameter("n");
		
		PrintWriter out = response.getWriter();
		StringBuilder result = new StringBuilder();
		Long answer = 0L;
		
		try {
			// get scores for A
			statement1 = conn.createStatement();
			statement2 = conn.createStatement();
			//Here in q6tweets table, accumulate column means the sum of the previous number of pictures
			//Excluding the pics of the current one 
			
			resultSet1 = statement1
					.executeQuery("SELECT sumpics FROM q6tweets WHERE userid >= " + m + " limit 1");
			
			resultSet2 = statement2
					.executeQuery("SELECT sumpics FROM q6tweets WHERE userid > "
							+ n + " limit 1");
			
			if (resultSet1.next() && resultSet2.next()) {
				answer = Long.parseLong(resultSet2.getString("sumpics")) - Long.parseLong(resultSet1.getString("sumpics"));
			}

		} catch (SQLException e) {
			result.append("Manupulating operations go wrong!");
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			result.append(sw.toString());
			
			e.printStackTrace();
		}

		finally {
			if (conn != null)
				try {
					conn.close();
				} catch (Exception ignore) {
				}
		}

		result.append(TEAM_INFO);
		result.append(answer);
		out.println(result.toString());
	}
}
