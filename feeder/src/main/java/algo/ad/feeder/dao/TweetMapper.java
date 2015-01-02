package algo.ad.feeder.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimeZone;

import org.springframework.jdbc.core.RowMapper;

public class TweetMapper implements RowMapper<Tweet> {
	public Tweet mapRow(ResultSet rs, int rowNum) throws SQLException {
		Tweet tweet = new Tweet();
		tweet.setTweet_id(rs.getLong("tweet_id"));

		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		java.util.Date time = new java.util.Date((Long.parseLong(rs
				.getString("unix_timestamp")) * 1000));

		tweet.setCreated_at(time);

		int temp_sentiment = rs.getInt("sentiment");

		if (temp_sentiment == TweetSentiment.POSITIVE.getSentimentCode()) {
			tweet.setSentiment(TweetSentiment.POSITIVE);
		} else if (temp_sentiment == TweetSentiment.NEUTRAL.getSentimentCode()) {
			tweet.setSentiment(TweetSentiment.NEUTRAL);
		} else if (temp_sentiment == TweetSentiment.NEGATIVE.getSentimentCode()) {
			tweet.setSentiment(TweetSentiment.NEGATIVE);
		}
		return tweet;
	}
}