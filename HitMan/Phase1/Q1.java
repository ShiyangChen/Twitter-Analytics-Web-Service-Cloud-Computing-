package hitman.phase1;

import java.io.IOException;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class Q1 extends HttpServlet{
	public static BigInteger x = 
			new BigInteger("68767668323517653964963775344760500"+
	"029708574838152629184503558698500851"+
	"67053394672634315391224052153");
	public static String TEAM_INFO = "HitMan,2136-3324-0103,3798-5854-9461,8668-4729-2265";
	
	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String key = request.getParameter("key");
		if(key!=null) {
			BigInteger product = new BigInteger(key);
			BigInteger y = product.divide(x);
			response.getWriter().println(y.toString());
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date d = new Date();
			response.getWriter().println(TEAM_INFO);
			response.getWriter().println(df.format(d));
		}
		else {
			response.getWriter().println("expecting key");
		}
	}

}