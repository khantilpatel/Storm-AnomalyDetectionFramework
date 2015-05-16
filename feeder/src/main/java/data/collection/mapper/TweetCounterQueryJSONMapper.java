package data.collection.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import data.collection.entity.TweetSentiment;
import data.collection.entity.TweetSentimentCountEntity;


public class TweetCounterQueryJSONMapper implements RowMapper<TweetSentimentCountEntity> {
	public TweetSentimentCountEntity mapRow(ResultSet rs, int rowNum) throws SQLException {

		TweetSentimentCountEntity tweetSentimentCountEntity = new TweetSentimentCountEntity();

		tweetSentimentCountEntity.setCount(rs.getLong("tweet_count"));
		
		int temp_sentiment = rs.getInt("sentiment");

		if (temp_sentiment == TweetSentiment.POSITIVE.getSentimentCode()) {
			tweetSentimentCountEntity.setSentiment(TweetSentiment.POSITIVE);
		} else if (temp_sentiment == TweetSentiment.NEUTRAL.getSentimentCode()) {
			tweetSentimentCountEntity.setSentiment(TweetSentiment.NEUTRAL);
		} else if (temp_sentiment == TweetSentiment.NEGATIVE.getSentimentCode()) {
			tweetSentimentCountEntity.setSentiment(TweetSentiment.NEGATIVE);
		}
		return tweetSentimentCountEntity;
	}
}