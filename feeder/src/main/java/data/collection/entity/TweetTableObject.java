package data.collection.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import twitter4j.Status;

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
	
	boolean isRetweet;
	String text_retweeted;

	Status status;
	
	List<Queries> queries;
	
	// This is specially required for bucketing and bulk-insert
	// Because for the bulk insert, the queries list is unwind
	// and the result is that this Tweet object represent a single query.
	// and there will be multiple Tweet object representing query list.
	String dbName_for_insert;
	String query_for_insert;
	long query_id_forinsert;
	
	public TweetTableObject() {
		queries = new ArrayList<Queries>(0);
	}

	public String getUser_profile_image_url() {
		return user_profile_image_url;
	}

	public void setUser_profile_image_url(String user_profile_image_url) {
		this.user_profile_image_url = user_profile_image_url;
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

	
	public boolean isRetweet() {
		return isRetweet;
	}

	public void setRetweet(boolean isRetweet) {
		this.isRetweet = isRetweet;
	}

	public String getText_retweeted() {
		return text_retweeted;
	}

	public void setText_retweeted(String text_retweeted) {
		this.text_retweeted = text_retweeted;
	}


	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public boolean addToQueries(Queries query) {
		return queries.add(query);
	}

	public List<Queries> getQueries() {
		 return queries;
	}
	public void setQueries(List<Queries> queries) {
		this.queries = queries;
	}

	public String getDbName_for_insert() {
		return dbName_for_insert;
	}

	public void setDbName_for_insert(String dbName_for_insert) {
		this.dbName_for_insert = dbName_for_insert;
	}

	public String getQuery_for_insert() {
		return query_for_insert;
	}

	public void setQuery_for_insert(String query_for_insert) {
		this.query_for_insert = query_for_insert;
	}

	public long getQuery_id_forinsert() {
		return query_id_forinsert;
	}

	public void setQuery_id_forinsert(long query_id_forinsert) {
		this.query_id_forinsert = query_id_forinsert;
	}

	

//	public static Comparator<TweetTableObject> DBNameComparator = new Comparator<TweetTableObject>() {
//
//		public int compare(TweetTableObject obj1, TweetTableObject obj2) {
//
//			String dbName1 = obj1.getDbName().toUpperCase();
//			String dbName2 = obj2.getDbName().toUpperCase();
//
//			// ascending order
//			return dbName1.compareTo(dbName2);
//
//			// descending order
//			// return fruitName2.compareTo(fruitName1);
//		}
//
//	};

}
