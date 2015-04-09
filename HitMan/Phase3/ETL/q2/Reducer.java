package hitman.etl.q2;
/*
 * 
 * 1. give an array to store the views of each day. And the
 *    array should be renewed after reading each article.
 * 2. when turning to next article, print the needed article
 * 	  and data. 
 * 3. finally the last article should be considered and printed
 *    individually.
 * 
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
//import java.util.*;
public class Reducer {
	
	private final String fieldTerminate = " ass ";
	private final String slashNReplace= " fuck ";
	private final String lineTerminator = " shit \n";
	
	public class Value implements Comparator<Value> {
		private long tweetID;
		private String scoreText;
		public Value(){
			
		}
		public Value(String value){
			String[] parts = value.split(":", 2);
			tweetID = Long.parseLong(parts[0]);
			scoreText = parts[1];

		}
		@Override
		public int compare(Value v1, Value v2 ) {
			// TODO Auto-generated method stub
			long diff = v1.tweetID - v2.tweetID;
			if(diff < 0) return -1;
			if(diff > 0) return 1;
			return 0;

		}
		
		public String toString(){
			return (Long.toString(tweetID) + ":" + scoreText);
		}
	
	}
	
	private void reducer(){
		try{
//			BufferedReader br = new BufferedReader(
//					new FileReader("aftersort.txt"));
//			
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in,"UTF-8"));
			String input;
			String key = null;
			LinkedList<Value> value = new LinkedList<Value>();
			String currentKey = null;
			Value currentValue = null;
			String[] parts;

			while((input = br.readLine())!=null) {
				
					
					parts = input.split("\t");
					currentKey = parts[0];
					currentValue = new Value(parts[1] + slashNReplace);
					if(null == key){
						key = currentKey;
						value.add(currentValue);
					}else{
						if(currentKey.equals(key)){
							value.add(currentValue);
						}else{
							if (null != key)
								print(key,value);
							key = currentKey;
							value = new LinkedList<Value>();
							value.add(currentValue);
						}
					}
			}
			if (null != key)
				print(key,value);
			

		}catch (IOException io) {
			io.printStackTrace();
		}	
	}
	
	private void print(String key, LinkedList<Value> value){
		System.out.print(key + fieldTerminate);
		Collections.sort(value, new Value());
		for(int i = 0; i < value.size(); i++){
			System.out.print(value.get(i).toString());
		}
		System.out.print(lineTerminator);
		
	}
	
	public static void main(String[] args) {
			new Reducer().reducer();		
	}
	

}

