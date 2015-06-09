package feeder;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import common.feeder.utility.FileIOUtility;

import data.collection.entity.TweetTableObject;

public class TestFileIO {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		List<List<TweetTableObject>> tweetList =  null;
		
		
		tweetList = FileIOUtility
				.readTweetListFromFile("tweetList.txt");
		
		if(tweetList == null){
		 tweetList =  new ArrayList<List<TweetTableObject>>();
		
		List<TweetTableObject> arrayTableObject = new ArrayList<TweetTableObject>();
		
		// ****1*************
		TweetTableObject tableObject = new TweetTableObject();
		
		tableObject.setCreated_at(new Date());
		tableObject.setJsonObject("{\"hello1\":}");
		
		
		arrayTableObject.add(tableObject);
		
		// ****2*************
		tableObject = new TweetTableObject();
		
		tableObject.setCreated_at(new Date());
		tableObject.setJsonObject("{\"hello2\":}");
		
		arrayTableObject.add(tableObject);
		
		
		// ****3*************
		tableObject = new TweetTableObject();
		
		tableObject.setCreated_at(new Date());
		tableObject.setJsonObject("{\"hello2\":}");
		
		arrayTableObject.add(tableObject);
		
		tweetList.add(arrayTableObject);
		
		FileIOUtility.saveTweetListToFile(tweetList, "tweetList.txt");
		
		
		}
		
	
	}

}
