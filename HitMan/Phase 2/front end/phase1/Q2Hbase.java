/****
 * Q2 hbase front end
 * */
package hitman.phase1;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

public class Q2Hbase extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static Configuration config = HBaseConfiguration.create();
	private static final String TABLE = "tweets";
	private static String TEAM_INFO = "HitMan,2136-3324-0103,3798-5854-9461,8668-4729-2265";
	private static String MASTER_IP = "54.165.12.128";
	public static final String UTF8 = "UTF-8";
	private static int HTABLE_POOL_SIZE = 15;
    private static HTablePool pool;
	
	public Q2Hbase() throws IOException{
		super();
		// configuration file
		config.set("fs.hdfs.impl", "emr.hbase.fs.BlockableFileSystem");
		config.set("hbase.regionserver.handler.count", "100");
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
		String userid = request.getParameter("userid");
		String tweet_time = request.getParameter("tweet_time");

		// Query
		try {
			Get g = new Get(Bytes.toBytes(tweet_time+userid));
			Result rs = table.get(g);
		
			response.getWriter().println(TEAM_INFO);
			for(KeyValue kv : rs.raw()){
				String value = new String(kv.getValue(), UTF8);
				response.getWriter().print(value);
			}

		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}
	}
}

