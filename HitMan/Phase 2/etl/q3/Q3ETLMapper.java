/**
 * Q3ETLMapper
 * */
package hitman.etl.q3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Q3ETLMapper {
	public static final String KEY_USER = "user";
	public static final String KEY_USER_ID = "id";
	public static final String KEY_RETWEETED_COUNT = "retweet_count";
	public static final String KEY_RETWEETED_STATUS = "retweeted_status";
	public static final String RETWEET = "\tr";
	public static final String WASRETWEETED = "\tw";
	private JSONParser jsonParser;

	public Q3ETLMapper() {
		jsonParser = new JSONParser();
	}
	
	public void mappr() throws IOException {
		BufferedReader bufferedReader = 
				new BufferedReader(new InputStreamReader(System.in));
		String line;
		JSONObject wholeLine;
		JSONObject user;
		String userid;
		Object retweetStatus;
		JSONObject retweetedUser;
		String retweetedUserid;
		while((line = bufferedReader.readLine()) != null) {
			try {
				wholeLine = (JSONObject) jsonParser.parse(line);
			} catch (ParseException e) {
				continue;
			}
			user = (JSONObject) wholeLine.get(KEY_USER);
			userid = user.get(KEY_USER_ID).toString();
			retweetStatus = wholeLine.get(KEY_RETWEETED_STATUS);
			if(retweetStatus != null) {
				retweetedUser = (JSONObject) ((JSONObject)retweetStatus).get(KEY_USER);
				retweetedUserid = retweetedUser.get(KEY_USER_ID).toString();
				// I retweet someone
				System.out.println(userid+RETWEET+retweetedUserid);
				// someone was retweeted by me
				System.out.println(retweetedUserid+WASRETWEETED+userid);
			}
			else {
				System.out.println(userid);
            }
		}
	}
	
	public static void main(String[] args) throws IOException, ParseException {
		new Q3ETLMapper().mappr();
	}
}
