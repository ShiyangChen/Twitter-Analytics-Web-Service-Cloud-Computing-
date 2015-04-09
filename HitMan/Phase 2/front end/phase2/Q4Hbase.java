/**
 * Q3Hbase
 * */
package hitman.phase2;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;

public class Q4Hbase extends HttpServlet {
	private static final long serialVersionUID = 1L;
	 public static final byte[] CF = Bytes.toBytes("r");
	private static Configuration config = HBaseConfiguration.create();
	private static final String TABLE = "q4tweets";
	private static String TEAM_INFO = "HitMan,2136-3324-0103,3798-5854-9461,8668-4729-2265";
	private static String MASTER_IP = "ec2-54-173-179-176.compute-1.amazonaws.com";
	public static final String UTF8 = "UTF-8";
	private static int HTABLE_POOL_SIZE = 15;
    private static HTablePool pool;
	
	public Q4Hbase() throws IOException {
		super();
		// configuration file
		config.set("fs.hdfs.impl", "emr.hbase.fs.BlockableFileSystem");
		config.set("hbase.regionserver.handler.count", "36");
		config.set("hbase.zookeeper.quorum", MASTER_IP);
		config.set("hbase.rootdir", "hdfs://"+MASTER_IP+":9000/hbase");
//		config.set("hbase.cluster.distributed", "true");
		config.set("hbase.tmp.dir", "/mnt/var/lib/hbase/tmp-data");
		// create a HTablePool
		pool=new HTablePool(config, HTABLE_POOL_SIZE);
		HTableInterface[] tables = new HTableInterface[HTABLE_POOL_SIZE];
		for (int n = 0; n < HTABLE_POOL_SIZE; n++) {
			tables[n] = pool.getTable(TABLE);
		}
		for (HTableInterface table : tables) {
			table.close();
		}
	}

	/**
	 * @throws IOException
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws IOException {
		response.setContentType("text/html; charset=UTF-8");
		HTableInterface table = pool.getTable(TABLE);
		// Get the parameters from URL
		String date = request.getParameter("date");
		String location = request.getParameter("location");
		int m = Integer.parseInt(request.getParameter("m"));
		int n = Integer.parseInt(request.getParameter("n"));

		try {
			response.getWriter().println(TEAM_INFO);
			Result rs;
			byte[] value;
			Get g = new Get(Bytes.toBytes(date+location));
			rs = table.get(g);
			for(int i=m; i<=n; i++){
				value = rs.getValue(CF, Bytes.toBytes(i));
				if(value != null) {
					response.getWriter().println(new String(value, UTF8));
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
}