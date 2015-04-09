package hitman.etl.q6;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.TreeMap;

public class Q6ETLOrganizer {

	static final String FILEPATH = "/Users/Cambi/lins/output";
	public void organize(String directoryName) {
		try{

			BufferedReader bufferedReader = new BufferedReader(new FileReader("/Users/Cambi/lins/output/full_1/14/allinone.txt"));
			TreeMap<Long, Integer> treeMap = new TreeMap<Long, Integer>(); 
			
			String line;
			Long userid;
			Integer sumPics = 0;
			
			while((line = bufferedReader.readLine()) != null) {
				String[] splits = line.split("\t");
				userid = Long.parseLong(splits[0]);
				Integer numpic = Integer.parseInt(splits[1]);
				treeMap.put(userid, numpic);
			}
			System.out.println(treeMap.size());
			
			Long sum = 0L;
			Iterator<Long> it = treeMap.keySet().iterator();
			PrintWriter writerSum = new PrintWriter("/Users/Cambi/lins/output/full_1/14/allinone_accumulate");
			PrintWriter writer = new PrintWriter("/Users/Cambi/lins/output/full_1/14/allinone_sorted");
			while (it.hasNext()){
				Long anId = it.next();
				writerSum.println(anId.toString() + '\t' + sum.toString());
				writer.println(anId.toString() + '\t' + treeMap.get(anId).toString());
				sum = sum + treeMap.get(anId);
			}
			
			writerSum.println(((Long)Long.MAX_VALUE).toString() + '\t' + sum.toString());
			writerSum.close();
			writer.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		

	}
	
	public static void main(String[] args) throws IOException {
		new Q6ETLOrganizer().organize(FILEPATH);
	}

}
