package data.collection.entity;

import java.util.Date;

public class TweetAggregatedBin {
	
	Date start_date_bin;
	
	Date end_date_bin;
	
	TweetSentiment sentiment;
	
	int value;

	public Date getStart_date_bin() {
		return start_date_bin;
	}

	public void setStart_date_bin(Date start_date_bin) {
		this.start_date_bin = start_date_bin;
	}

	public Date getEnd_date_bin() {
		return end_date_bin;
	}

	public void setEnd_date_bin(Date end_date_bin) {
		this.end_date_bin = end_date_bin;
	}

	public TweetSentiment getSentiment() {
		return sentiment;
	}

	public void setSentiment(TweetSentiment sentiment) {
		this.sentiment = sentiment;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}
	
	

}
