package data.collection.entity;

import java.io.Serializable;

public class TweetTransferEntity implements Serializable{

	/**
	 * 	
	 */
	private static final long serialVersionUID = 421210472839905955L;

	Long timestamp;
	
	Long nextTimestamp;
	
	Integer sentiment;

	
	public Long getNextTimestamp() {
		return nextTimestamp;
	}

	public void setNextTimestamp(Long nextTimestamp) {
		this.nextTimestamp = nextTimestamp;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public Integer getSentiment() {
		return sentiment;
	}

	public void setSentiment(Integer sentiment) {
		this.sentiment = sentiment;
	}
	
	
}
