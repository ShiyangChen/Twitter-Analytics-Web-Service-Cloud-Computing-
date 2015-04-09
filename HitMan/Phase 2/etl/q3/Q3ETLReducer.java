/**
 * Q3ETLReducer
 * */
package hitman.etl.q3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;

public class Q3ETLReducer {
	public static final String RETWEET = "r";
	public static final String WASRETWEETED = "w";
	public static final int REWTWEET_LENGTH = 2;
	public static final String USER_ID_TERM = "\t";
	public void reducer() throws IOException {
		BufferedReader bufferedReader = 
				new BufferedReader(new InputStreamReader(System.in));
		
		String line;
		String[] splits;
		String currentUserid = null;
		String userid = null;
		String retweetUseridString = null;
		Long retweetUserid;
		Long[] wasRetweeted;
		HashSet<Long> wasRetweetedHS = new HashSet<Long>();
		// contains those userid I retweeted
		HashSet<Long> retweet = new HashSet<Long>();
		while((line = bufferedReader.readLine()) != null) {
			splits = line.split("\t");
			userid = splits[0];
			// when a new userid comes, stream out the result
			if(currentUserid!=null && !currentUserid.equals(userid)) {
				System.out.print(currentUserid+",");
				wasRetweeted = wasRetweetedHS.toArray(new Long[0]);
				Arrays.sort(wasRetweeted);
				for(Long s: wasRetweeted) {
					// if I also have retweeted the person
					if(retweet.contains(s))
						System.out.print("("+s+")"+USER_ID_TERM);
					else
						System.out.print(s+USER_ID_TERM);
				}
				System.out.println("shit");
				wasRetweetedHS = new HashSet<Long>();
				retweet = new HashSet<Long>();
			}
			currentUserid = userid;
			if(splits.length==REWTWEET_LENGTH) {
				retweetUseridString = splits[1];
				// add the retweetUserid either in the retweet or the retweetUserid
				retweetUserid = Long.parseLong(retweetUseridString.substring(1));
				if(retweetUseridString.startsWith(WASRETWEETED))
					wasRetweetedHS.add(retweetUserid);
				else
					retweet.add(retweetUserid);
			}
		}
		if(currentUserid!=null) {
			System.out.print(currentUserid+",");
			wasRetweeted = wasRetweetedHS.toArray(new Long[0]);
			Arrays.sort(wasRetweeted);
			for(Long s: wasRetweeted) {
				// if I also have retweeted the person
				if(retweet.contains(s))
					System.out.print("("+s+")"+USER_ID_TERM);
				else
					System.out.print(s+USER_ID_TERM);
			}
			System.out.println("shit");
		}
	}
	
	public static void main(String[] args) throws IOException {
		new Q3ETLReducer().reducer();
	}
}
