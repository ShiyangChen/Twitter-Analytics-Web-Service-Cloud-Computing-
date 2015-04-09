package hitman.etl.q6;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Q6ETLReducer {

	
	public static final String TWEET_ID_TERM = ",";
	public void reducer() throws IOException {
		try{
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
//			BufferedReader bufferedReader = new BufferedReader(new FileReader("/Users/Cambi/lins/dataset/15619f14twitter-parta-aa-mapout.txt"));

			String line;
			String userid;
			String currentid = null;
			Integer sumPics = 0;
			Set<String> tweetidSet = new HashSet<String>();
			
			while((line = bufferedReader.readLine()) != null) {
				String[] splits = line.split("\t");
				userid = splits[0];
				Integer numpic = Integer.parseInt(splits[2]);
				if (!tweetidSet.contains(splits[1])) {
					tweetidSet.add(splits[1]);
					
					if (currentid != null && currentid.equals(userid)) {
						sumPics = sumPics + numpic;
					} else {
						if (currentid != null) {
							System.out.println(currentid + "\t" + sumPics);
						}
						currentid = userid;
						sumPics = numpic;
						tweetidSet = new HashSet<String>();
						tweetidSet.add(splits[1]);
					}
				}
			}
			if (currentid != null) {
				System.out.println(currentid + '\t' + sumPics);
			}
			bufferedReader.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws IOException {
		new Q6ETLReducer().reducer();
	}
}
