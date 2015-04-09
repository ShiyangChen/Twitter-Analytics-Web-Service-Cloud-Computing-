package hitman.phase1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;

public class Q2Hbase extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private HTable table = null;
	private Configuration config = HBaseConfiguration.create();
	public static final String TABLE = "tweets";
	public static final byte[] CF_TEXT = Bytes.toBytes("text");
	public static final byte[] CF_SCORE = Bytes.toBytes("score");
	public static final String UTF8 = "UTF-8";
	public static String TEAM_INFO = "HitMan,2136-3324-0103,3798-5854-9461,8668-4729-2265";
	public static String MASTER_IP = "172.31.43.140";
	
	public Q2Hbase() {
		super();
		
		// configuration file
		config.set("fs.hdfs.impl", "emr.hbase.fs.BlockableFileSystem");
		config.set("hbase.regionserver.handler.count", "100");
		config.set("hbase.zookeeper.quorum", MASTER_IP);
		config.set("hbase.rootdir", "hdfs://"+MASTER_IP+":9000/hbase");
		config.set("hbase.cluster.distributed", "true");
		config.set("hbase.tmp.dir", "/mnt/var/lib/hbase/tmp-data");
		
		// Connect to Hbase
		try {
			HBaseAdmin.checkHBaseAvailable(config);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			return;
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			return;
		} catch (Exception e) {
			e.printStackTrace();
			return;
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
		try {
			table = new HTable(config, TABLE);
		} catch (IOException e) {
			response.getWriter().println("table initialzed failed "+e);
			return;
		}
		// Get the parameters from URL
		String userid = request.getParameter("userid");
		String tweet_time = request.getParameter("tweet_time");
		if(userid == null || tweet_time==null) {
			response.getWriter().println("Invalid input");
			return;
		}
		tweet_time = tweet_time.replaceFirst(" ", "+");

		// Query
		try {
			List<String> results = new ArrayList<String>();
			Get g = new Get(Bytes.toBytes(tweet_time+userid));
			Result rs = table.get(g);
			response.getWriter().println(TEAM_INFO);
			for(KeyValue kv : rs.raw()){
				String tweet_id = new String(kv.getQualifier());
				String value = new String(kv.getValue(), UTF8);
//				value = value.replace(" ass ", "\n");
//                value = value.replace(" fuck ", ",");
				results.add(tweet_id+":"+value);
			}
			Collections.sort(results);
			for(String str: results) {
				response.getWriter().println(str);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}
		response.getWriter().flush();
	}
}