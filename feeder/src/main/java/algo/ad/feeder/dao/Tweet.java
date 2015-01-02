package algo.ad.feeder.dao;

import java.io.Serializable;
import java.util.Date;



public class Tweet implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7353435380699752176L;
	long tweet_id;
	Date created_at;
	TweetSentiment sentiment;
	public long getTweet_id() {
		return tweet_id;
	}
	public void setTweet_id(long tweet_id) {
		this.tweet_id = tweet_id;
	}
	public Date getCreated_at() {
		return created_at;
	}
	public void setCreated_at(Date created_at) {
		this.created_at = created_at;
	}
	public TweetSentiment getSentiment() {
		return sentiment;
	}
	public void setSentiment(TweetSentiment sentiment) {
		this.sentiment = sentiment;
	}		
}
