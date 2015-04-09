package hitman.etl.q6;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Q6ETLMapper {
	private JSONParser jsonParser;
	
	
	public Q6ETLMapper() {
		jsonParser = new JSONParser();
	}
	
	@SuppressWarnings("unchecked")
	public void mappr() throws IOException, java.text.ParseException {
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
//		BufferedReader bufferedReader = new BufferedReader(new FileReader("/Users/Cambi/lins/dataset/15619f14twitter-parta-aa"));
//	    Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/Cambi/lins/dataset/15619f14twitter-parta-aa-mapout.txt"), "utf-8"));
	    
		String line;
		JSONObject wholeLine;
		JSONObject entities;
		
		while((line = bufferedReader.readLine()) != null) {
			try {
				wholeLine = (JSONObject) jsonParser.parse(line);
			} catch (ParseException e) {
				continue;
			}
			
			
			String userid = ((JSONObject)wholeLine.get("user")).get("id_str").toString();
			String tweetid = wholeLine.get("id_str").toString();
			
			entities = (JSONObject)wholeLine.get("entities");
			List<JSONObject> mediaList = (List<JSONObject>)entities.get("media");
			if (mediaList != null) {
				int numPics = 0;
				for (JSONObject media : mediaList) {
					if (media.get("type").toString().equals("photo")) {
						numPics++;
					}
				}
			
				if (numPics > 0) {
//					writer.write(userid + "\t" + tweetid + "\t" + numPics + '\n');
					System.out.print(userid + "\t" + tweetid + "\t" + numPics + '\n');
				}
			}
		}
		
//		bufferedReader.close();
//		writer.close();
	}
	
	
	public static void main(String[] args) throws IOException, ParseException, 
	java.text.ParseException {
		new Q6ETLMapper().mappr();
	}
}
