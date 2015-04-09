package hitman.etl.q5;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

public class Q5ETLReducer {
	public static final String TYPE_1 = "1";
	public static final String TYPR_2 = "2";
	
	public void reducer() throws IOException {
		BufferedReader bufferedReader = 
				new BufferedReader(new InputStreamReader(System.in));
//				new BufferedReader(new FileReader("/Users/zuoyougu/Downloads/15619f14twitter-partb-kc"));
		
		String line;
		String[] splits;
		String userid;
		String prevUserid = null;
		String value;
		// hashset can make sure tweetids are different
		HashSet<String> type1 = new HashSet<String>();
		HashSet<String> type2 = new HashSet<String>();
		while((line = bufferedReader.readLine()) != null) {
			// split the key and value
			splits = line.split("\t");
			userid = splits[0];
			
			// a new key came
			if(prevUserid != null && !prevUserid.equals(userid)) {
				streamOut(prevUserid, type1, type2);
				type1 = new HashSet<String>();
				type2 = new HashSet<String>();
			}
			prevUserid = userid;
			value = splits[1];
			
			splits = value.split(",");
			if(splits[0].equals(TYPE_1)) type1.add(splits[1]);
			else type2.add(splits[1]);
		}
		if(prevUserid != null) {
			streamOut(prevUserid, type1, type2);
		}
	}
	
	public void streamOut(String prevUserid, HashSet<String> type1, HashSet<String> type2) {
		int score1 = type1.size();
		int score2 = type2.size()*3;
		int score3 = getScore3(type2)*10;
		System.out.println(prevUserid+","+score1+","+score2+","+score3);
	}
	
	public int getScore3(HashSet<String> type2) {
		HashSet<String> userids = new HashSet<String>();
		String[] splits;
		for(String tweet: type2) {
			splits = tweet.split(";");
			userids.add(splits[1]);
		}
		
		return userids.size();
	}
	
	public static void main(String[] args) throws IOException {
		new Q5ETLReducer().reducer();
	}
}
