/**
 * Q4ETLReducer
 * */
package hitman.etl.q4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Q4ETLMapper {
	public static final String OLD_FORMAT = "E MMM dd HH:mm:ss ZZZZ yyyy";
	public static final String NEW_FORMAT = "yyyy-MM-dd";
	private JSONParser jsonParser;
	private SimpleDateFormat oldSDF;
	private SimpleDateFormat newSDF;
	
	private String newDateString = null;
	private String location = null;
	private String tweetid = null;
	
	public Q4ETLMapper() {
		jsonParser = new JSONParser();
		oldSDF = new SimpleDateFormat(OLD_FORMAT);
		oldSDF.setTimeZone(TimeZone.getTimeZone("UTC")); 
		newSDF = new SimpleDateFormat(NEW_FORMAT);
		newSDF.setTimeZone(TimeZone.getTimeZone("UTC")); 
	}
	
	@SuppressWarnings("unchecked")
	public void mappr() throws IOException, java.text.ParseException {
		BufferedReader bufferedReader = 
				new BufferedReader(new InputStreamReader(System.in, "UTF-8"));

		String line;
		JSONObject wholeLine;
		Date date;
		Object entities;
		
		while((line = bufferedReader.readLine()) != null) {
			try {
				wholeLine = (JSONObject) jsonParser.parse(line);
			} catch (ParseException e) {
				continue;
			}
			
			// get the date
			date = oldSDF.parse(wholeLine.get("created_at").toString());
			newDateString = newSDF.format(date);
			
			// get the location
			location = getFromPlace(wholeLine.get("place"));
			location = location==null?getFromTimeZone(wholeLine.get("user")):location;
			
			// location is null, ignore this line
			if(location == null) continue;
			
			tweetid = wholeLine.get("id_str").toString();
			
			entities = wholeLine.get("entities");
			printHashTag((List<JSONObject>)((JSONObject)entities).get("hashtags"));
		}
		bufferedReader.close();
	}
	
	public String getFromPlace(Object place) {
		if(place==null) return null;
		Object placeName = ((JSONObject)place).get("name");
		return placeName==null?null:(String)placeName;
	}
	
	// find the location from the time zone
	public String getFromTimeZone(Object user) {
		if(user==null) return null;
		Object timeZone = ((JSONObject)user).get("time_zone");
		// when time zone is null, return null
		if(timeZone == null) return null;
		
		String timeZoneString = timeZone.toString();
		// when time zone contains time
		if(timeZoneString.toLowerCase().contains(" time ")) return null;
		
		return timeZoneString;
	}
	
	@SuppressWarnings("unchecked")
	public void printHashTag(List<JSONObject> hashtags) {
		// remove the duplicate hashtags
		HashMap<String, JSONObject> map = new HashMap<String, JSONObject>();
		// print out the hashtags
		String key;
		List<Long> indices;
		List<Long> curIndices;
		for(JSONObject hashtag: hashtags) {
			key = hashtag.get("text").toString();
			if(!map.containsKey(key)) map.put(key, hashtag);
			else {
				// get the indices from the hashtag
				indices = (List<Long>) hashtag.get("indices");
				// the new hashtag is before the old one, update the hashtag
				curIndices = (List<Long>)map.get(key).get("indices");
				if(curIndices.get(0)>indices.get(0)) {
					map.put(key, hashtag);
				}
			}
		}
		Object text;
		JSONObject hashtag;
		for (Map.Entry<String, JSONObject> entry : map.entrySet()) {
			hashtag = entry.getValue();
			text = hashtag.get("text");
			indices = (List<Long>) hashtag.get("indices");
			if (text != null) {
				System.out.println(newDateString + location + "\t"
						+ text.toString() + ":" + tweetid + "."
						+ indices.get(0));
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ParseException, 
	java.text.ParseException {
		new Q4ETLMapper().mappr();
	}
}
