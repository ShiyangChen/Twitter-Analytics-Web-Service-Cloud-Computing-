package hitman.etl.q5;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Q5ETLMapper {
	public static final String KEY_USER = "user";
	public static final String KEY_USER_ID = "id";
	public static final String KEY_TWEET_ID = "id";
	private JSONParser jsonParser;
	
	public Q5ETLMapper() {
		jsonParser = new JSONParser();
	}

	public void mapper() throws IOException {
		BufferedReader bufferedReader = 
				new BufferedReader(new InputStreamReader(System.in));
//				new BufferedReader(new FileReader("/Users/zuoyougu/Downloads/15619f14twitter-partb-kc"));

		String line;
		JSONObject wholeLine;
		JSONObject user;
		String userid;
		String tweetid;
		JSONObject retweetStatus;
		JSONObject retweetUser;
		String retweetUserid;
		while((line = bufferedReader.readLine()) != null) {
			try {
				wholeLine = (JSONObject) jsonParser.parse(line);
			} catch (ParseException e) {
				continue;
			}
			
			user = (JSONObject) wholeLine.get(KEY_USER);
			userid = user.get(KEY_USER_ID).toString();
			tweetid = wholeLine.get(KEY_TWEET_ID).toString();
			System.out.println(userid+"\t1,"+tweetid);
			retweetStatus = (JSONObject) wholeLine.get("retweeted_status");
			if(retweetStatus != null) {
				retweetUser = (JSONObject) retweetStatus.get(KEY_USER);
				retweetUserid = retweetUser.get(KEY_USER_ID).toString();
				System.out.println(retweetUserid+"\t2,"+tweetid+";"+userid);
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
		new Q5ETLMapper().mapper();
	}
}
