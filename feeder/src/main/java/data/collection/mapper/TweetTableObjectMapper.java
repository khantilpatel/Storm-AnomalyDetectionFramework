package data.collection.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimeZone;

import org.springframework.jdbc.core.RowMapper;

import data.collection.entity.Tweet;
import data.collection.entity.TweetSentiment;
import data.collection.entity.TweetTableObject;

public class TweetTableObjectMapper implements RowMapper<TweetTableObject> {
	public TweetTableObject mapRow(ResultSet rs, int rowNum) throws SQLException {
		TweetTableObject tweet = new TweetTableObject();
		tweet.setTweet_id(String.valueOf(rs.getLong("tweet_id")));

		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		java.util.Date time = new java.util.Date((Long.parseLong(rs
				.getString("unix_timestamp")) * 1000));
		tweet.setUnix_timestamp(Long.parseLong(rs
				.getString("unix_timestamp")));
		
		tweet.setCreated_at(time);
		
		
		int temp_sentiment = rs.getInt("sentiment");

		if (temp_sentiment == TweetSentiment.POSITIVE.getSentimentCode()) {
			tweet.setSentiment(TweetSentiment.POSITIVE.getSentimentCode());
			
		} else if (temp_sentiment == TweetSentiment.NEUTRAL.getSentimentCode()) {
			tweet.setSentiment(TweetSentiment.NEUTRAL.getSentimentCode());
		} else if (temp_sentiment == TweetSentiment.NEGATIVE.getSentimentCode()) {
			tweet.setSentiment(TweetSentiment.NEGATIVE.getSentimentCode());
		}
		return tweet;
	}
}