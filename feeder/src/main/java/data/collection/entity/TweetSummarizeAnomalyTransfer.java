package data.collection.entity;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

public class TweetSummarizeAnomalyTransfer implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6735470008801485734L;

	int is_negative_sentiment_anomalous;
	
	int is_neutral_sentiment_anomalous;
	
	int is_positive_sentiment_anomalous;
	
	int negative_sentiment_count;
	
	int positive_sentiment_count;
	
	int neutral_sentiment_count;
	
	List<TweetTransferEntity> neutral_sentiment_tweetlist;
	
	List<TweetTransferEntity> negative_sentiment_tweetlist;

	List<TweetTransferEntity> positive_sentiment_tweetlist;
	
	Date currentDate;
	
	public Date getCurrentDate() {
		return currentDate;
	}

	public void setCurrentDate(Date currentDate) {
		this.currentDate = currentDate;
	}

	public int getIs_negative_sentiment_anomalous() {
		return is_negative_sentiment_anomalous;
	}

	public void setIs_negative_sentiment_anomalous(
			int is_negative_sentiment_anomalous) {
		this.is_negative_sentiment_anomalous = is_negative_sentiment_anomalous;
	}

	public int getIs_neutral_sentiment_anomalous() {
		return is_neutral_sentiment_anomalous;
	}

	public void setIs_neutral_sentiment_anomalous(int is_neutral_sentiment_anomalous) {
		this.is_neutral_sentiment_anomalous = is_neutral_sentiment_anomalous;
	}

	public int getIs_positive_sentiment_anomalous() {
		return is_positive_sentiment_anomalous;
	}

	public void setIs_positive_sentiment_anomalous(
			int is_positive_sentiment_anomalous) {
		this.is_positive_sentiment_anomalous = is_positive_sentiment_anomalous;
	}

	public int getNegative_sentiment_count() {
		return negative_sentiment_count;
	}

	public void setNegative_sentiment_count(int negative_sentiment_count) {
		this.negative_sentiment_count = negative_sentiment_count;
	}

	public int getPositive_sentiment_count() {
		return positive_sentiment_count;
	}

	public void setPositive_sentiment_count(int positive_sentiment_count) {
		this.positive_sentiment_count = positive_sentiment_count;
	}

	public int getNeutral_sentiment_count() {
		return neutral_sentiment_count;
	}

	public void setNeutral_sentiment_count(int neutral_sentiment_count) {
		this.neutral_sentiment_count = neutral_sentiment_count;
	}

	public List<TweetTransferEntity> getNeutral_sentiment_tweetlist() {
		return neutral_sentiment_tweetlist;
	}

	public void setNeutral_sentiment_tweetlist(
			List<TweetTransferEntity> neutral_sentiment_tweetlist) {
		this.neutral_sentiment_tweetlist = neutral_sentiment_tweetlist;
	}

	public List<TweetTransferEntity> getNegative_sentiment_tweetlist() {
		return negative_sentiment_tweetlist;
	}

	public void setNegative_sentiment_tweetlist(
			List<TweetTransferEntity> negative_sentiment_tweetlist) {
		this.negative_sentiment_tweetlist = negative_sentiment_tweetlist;
	}

	public List<TweetTransferEntity> getPositive_sentiment_tweetlist() {
		return positive_sentiment_tweetlist;
	}

	public void setPositive_sentiment_tweetlist(
			List<TweetTransferEntity> positive_sentiment_tweetlist) {
		this.positive_sentiment_tweetlist = positive_sentiment_tweetlist;
	}
	
	
	
}
