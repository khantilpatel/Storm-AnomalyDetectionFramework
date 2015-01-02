package algo.ad.feeder.dao;

public enum TweetSentiment {
	
	POSITIVE(0), NEUTRAL(2), NEGATIVE(4);
	 
	private int sentimentCode;
 
	private TweetSentiment(int s) {
		sentimentCode = s;
	}
 
	public int getSentimentCode() {
		return sentimentCode;
	}	
	
}

