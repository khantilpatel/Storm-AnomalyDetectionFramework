package data.collection.entity;

import java.io.Serializable;

public enum TweetSentiment implements Serializable {
	
	NEGATIVE(0), NEUTRAL(2),POSITIVE(4);
	 
	private int sentimentCode;
 
	private TweetSentiment(int s) {
		sentimentCode = s;
	}
 
	public int getSentimentCode() {
		return sentimentCode;
	}	
	
}

