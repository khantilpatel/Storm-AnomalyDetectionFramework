package algo.ad.feeder.dao;

import java.io.Serializable;
import java.util.Date;

public class TweetTableObject implements Serializable {

	/**
	 * 
	 */
	// query_id
	// STRING tweet_id;
	// user_name
	// TEXT
	// INT YEAR
	// INT MONTH
	// INT DAY
	// INT HOUR
	// INT MIN
	// INT sec
	// BIGINT UNIX_TIMESTAMP
	// jsonObject BLOB
	// sentiment
	// sentiment_original
	// created_at

	private static final long serialVersionUID = -7353435380699752176L;
	long query_id;
	String tweet_id;
	String user_name;
	String text;
	int year;
	int month;
	int day;
	int hour;
	int min;
	int sec;
	long unix_timestamp;
	
	String jsonObject;
	int sentiment;
	int sentiment_original;
	Date created_at;
	String user_profile_image_url;
	String dbName;
	String query;
	public String getUser_profile_image_url() {
		return user_profile_image_url;
	}

	public void setUser_profile_image_url(String user_profile_image_url) {
		this.user_profile_image_url = user_profile_image_url;
	}

	public long getQuery_id() {
		return query_id;
	}

	public void setQuery_id(long query_id) {
		this.query_id = query_id;
	}

	public String getTweet_id() {
		return tweet_id;
	}

	public void setTweet_id(String tweet_id) {
		this.tweet_id = tweet_id;
	}

	public String getUser_name() {
		return user_name;
	}

	public void setUser_name(String user_name) {
		this.user_name = user_name;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getHour() {
		return hour;
	}

	public void setHour(int hour) {
		this.hour = hour;
	}

	public int getMin() {
		return min;
	}

	public void setMin(int min) {
		this.min = min;
	}

	public int getSec() {
		return sec;
	}

	public void setSec(int sec) {
		this.sec = sec;
	}

	public long getUnix_timestamp() {
		return unix_timestamp;
	}

	public void setUnix_timestamp(long unix_timestamp) {
		this.unix_timestamp = unix_timestamp;
	}

	public String getJsonObject() {
		return jsonObject;
	}

	public void setJsonObject(String jsonObject) {
		this.jsonObject = jsonObject;
	}

	public int getSentiment() {
		return sentiment;
	}

	public void setSentiment(int sentiment) {
		this.sentiment = sentiment;
	}

	public int getSentiment_original() {
		return sentiment_original;
	}

	public void setSentiment_original(int sentiment_original) {
		this.sentiment_original = sentiment_original;
	}

	public Date getCreated_at() {
		return created_at;
	}

	public void setCreated_at(Date created_at) {
		this.created_at = created_at;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

}
