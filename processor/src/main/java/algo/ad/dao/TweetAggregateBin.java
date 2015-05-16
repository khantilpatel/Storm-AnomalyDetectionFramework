package algo.ad.dao;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import data.collection.entity.TweetTransferEntity;

public class TweetAggregateBin implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6606390869158746115L;
	
	List<TweetTransferEntity> tweetList = new ArrayList<TweetTransferEntity>(0);
	
	int counter;
	
	int sentiment_id;
	
	Date date;
	
	public int getCounter() {
		return counter;
	}

	public void setCounter(int counter) {
		this.counter = counter;
	}

	public int getSentiment_id() {
		return sentiment_id;
	}

	public void setSentiment_id(int sentiment_id) {
		this.sentiment_id = sentiment_id;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public List<TweetTransferEntity> getTweetList() {
		return tweetList;
	}

	public void setTweetList(List<TweetTransferEntity> tweetList) {
		this.tweetList = tweetList;
	}
	
}
