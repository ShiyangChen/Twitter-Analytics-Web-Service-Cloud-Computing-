/**
 * Q4ETLReducer
 * */
package hitman.etl.q4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Q4ETLReducer {
	public class Tweet {
		long tweetid;
		int start;
		
		public Tweet(String tweet) {
			String[] splits = tweet.split("\\.");
			tweetid = Long.parseLong(splits[0]);
			start = Integer.parseInt(splits[1]);
		}
		
		public int compare(Tweet t2) {
			// 2. compare tweetid first
			long diff = this.tweetid-t2.tweetid;
			if(diff < 0) return -1;
			if(diff > 0) return 1;
			
			// 3. compare the index
			int dif = this.start - t2.start;
			return dif;
		}
	}
	
	public class Element {
		String hashtag;
		List<Tweet> tweets;
		public Element(String hashtag, List<Tweet> tweets) {
			this.hashtag = hashtag;
			this.tweets = tweets;
		}
		
		public void print() {
			System.out.print(hashtag+":"+tweets.get(0).tweetid);
			
			Tweet t;
			for(int i=1; i<tweets.size(); i++) {
				t = tweets.get(i);
				System.out.print(","+t.tweetid);
			}
			// in case the lines are messup
			System.out.println(" shit ");
		}
	}
	
	// this is the comparator for the tweetids in a line
	public class TweetComparator implements Comparator<Tweet> {
		public int compare(Tweet t1, Tweet t2) {
			long diff = t1.tweetid-t2.tweetid;
			if(diff < 0) return -1;
			if(diff > 0) return 1;
			return 0;
		}
	}
	
	// this is the comparator for the Element
	public class ElementComparator implements Comparator<Element> {
		public int compare(Element e1, Element e2) {
			// 1. compare popularity
			int diff = e2.tweets.size() - e1.tweets.size();
			if(diff != 0) return diff;
			
			// 2. compare tweetid
			Tweet t1 = e1.tweets.get(0);
			Tweet t2 = e2.tweets.get(0);
			return t1.compare(t2);
		}
	}
	
	public static final String TWEET_ID_TERM = ",";
	public void reducer() throws IOException {
		BufferedReader bufferedReader = 
				new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
//		BufferedReader bufferedReader = new BufferedReader(new FileReader("/Users/zuoyougu/Downloads/q4mapper.txt"));

		String line;
		String[] splits;
		String dateAndLocation = null;
		String curDataAndLocation = null;
		// key:hashtag, value:tweetids+indices
		HashMap<String, HashMap<Long, Tweet>> map = new HashMap<String, HashMap<Long, Tweet>>();
		String value;
		String hashtag;
		Tweet tweet;
		HashMap<Long, Tweet> tweets;
		HashMap<Long, Tweet> list;
		
		while((line = bufferedReader.readLine()) != null) {
			splits = line.split("\t");
			dateAndLocation = splits[0];
			// when a new dateAndLocation comes, stream out the result
			if(curDataAndLocation!=null && !curDataAndLocation.equals(dateAndLocation)) {
				// add the data into list
				streamOut(map, curDataAndLocation);
				map = new HashMap<String, HashMap<Long, Tweet>>();
			}
			curDataAndLocation = dateAndLocation;
			value = splits[1];
			splits = value.split(":");
			hashtag = splits[0];
			tweet = new Tweet(splits[1]);
			if(map.containsKey(hashtag)) {
				list = map.get(hashtag);
				if(list.containsKey(tweet.tweetid)) {
					if(list.get(tweet.tweetid).start > tweet.start) {
						list.put(tweet.tweetid, tweet);
					}
				}
				else {
					map.get(hashtag).put(tweet.tweetid, tweet);
				}
			}
			else {
				tweets = new HashMap<Long, Tweet>();
				tweets.put(tweet.tweetid, tweet);
				map.put(hashtag, tweets);
			}
		}
		if(curDataAndLocation!=null) {
			streamOut(map, curDataAndLocation);
		}
	}
	
	public void streamOut(HashMap<String, HashMap<Long, Tweet>> map, String curDateAndLocation) {
		List<Element> list = new ArrayList<Element>();
		hashTagsMapToList(map, list);
		// list is sorted
		Element element;
		for(int i=0; i<list.size(); i++) {
			element = list.get(i);
			// Key: rank+date+location
			System.out.print(curDateAndLocation+"\t"+(i+1)+"\t");
			element.print();
		}
	}
	
	public void hashTagsMapToList(HashMap<String, HashMap<Long, Tweet>> map, List<Element> list) {
		HashMap<Long, Tweet> tweets;
		List<Tweet> tweetsList;
		for(Map.Entry<String, HashMap<Long, Tweet>> entry: map.entrySet()) {
			// sort the tweetids first
			tweets = entry.getValue();
			tweetsList = new ArrayList<Tweet>();
			tweetsMapToList(tweets, tweetsList);
//			Collections.sort(tweets, new TweetComparator());
			// this list contains all the hashtags for this date and location
			list.add(new Element(entry.getKey(), tweetsList));
		}
		// sort the list for this hashtag
		Collections.sort(list, new ElementComparator());
	}
	
	public void tweetsMapToList(HashMap<Long, Tweet> map, List<Tweet> list) {
		for(Map.Entry<Long, Tweet> entry: map.entrySet()) {
			list.add(entry.getValue());
		}
		// sort the tweet_ids in this line
		Collections.sort(list, new TweetComparator());
	}
	
	public static void main(String[] args) throws IOException {
		new Q4ETLReducer().reducer();
	}
}
