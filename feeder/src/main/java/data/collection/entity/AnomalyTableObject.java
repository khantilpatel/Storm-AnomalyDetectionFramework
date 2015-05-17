package data.collection.entity;

public class AnomalyTableObject {

	int query_id;

	long tweet_id;

	int sentiment;

	long timestamp;

	long aggregation;

	long window_length;
	
	int value;

	String note;

	
	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public int getQuery_id() {
		return query_id;
	}

	public void setQuery_id(int query_id) {
		this.query_id = query_id;
	}

	public long getTweet_id() {
		return tweet_id;
	}

	public void setTweet_id(long tweet_id) {
		this.tweet_id = tweet_id;
	}

	public int getSentiment() {
		return sentiment;
	}

	public void setSentiment(int sentiment) {
		this.sentiment = sentiment;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getAggregation() {
		return aggregation;
	}

	public void setAggregation(long aggregation) {
		this.aggregation = aggregation;
	}

	public long getWindow_length() {
		return window_length;
	}

	public void setWindow_length(long window_length) {
		this.window_length = window_length;
	}

	public String getNote() {
		return note;
	}

	public void setNote(String note) {
		this.note = note;
	}

}
