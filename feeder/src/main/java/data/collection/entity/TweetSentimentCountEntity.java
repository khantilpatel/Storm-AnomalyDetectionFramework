package data.collection.entity;

public class TweetSentimentCountEntity {
	
	TweetSentiment sentiment;
	
	long count;

	public TweetSentiment getSentiment() {
		return sentiment;
	}

	public void setSentiment(TweetSentiment sentiment) {
		this.sentiment = sentiment;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}
	
	

}
