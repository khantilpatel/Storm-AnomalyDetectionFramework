package algo.ad.utility;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

enum AnomalyType
{
	NONE,
	LOCAL_ANOMALY,
	WINDOW_ANOMALY
};



public class Bin  {

	private int count;
	private Date date;
	private AnomalyType anomaly_type = AnomalyType.NONE;
	
	public AnomalyType getAnomaly_type() {
		return anomaly_type;
	}

	public void setAnomaly_type(AnomalyType anomaly_type) {
		this.anomaly_type = anomaly_type;
	}

	private List<Object> tweets ;
	
	public List<Object> getTweets() {
		return tweets;
	}

	public void addTweets(Object tweet) {
		this.tweets.add(tweet);
	}

	public Bin() {
		count = 0;
		tweets = new ArrayList<Object>();
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

}
