package data.collection.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.springframework.jdbc.core.RowMapper;

import data.collection.entity.TweetAggregatedBin;
import data.collection.entity.TweetSentiment;

public class TweetAggregateBinMapper implements RowMapper<TweetAggregatedBin> {
	
	public TweetAggregatedBin mapRow(ResultSet rs, int rowNum) throws SQLException {
		TweetAggregatedBin bin = new TweetAggregatedBin();
		
		String range = rs.getString("range_t");
		
		String [] range_values = range.split("-");
		
		Date start_date = new Date();
		start_date.setTime(Long.valueOf(range_values[0])*1000);
		
		Date end_date = new Date();
		end_date.setTime(Long.valueOf(range_values[1])*1000);
		
		bin.setStart_date_bin(start_date); //1
		bin.setEnd_date_bin(end_date); //2
		
		bin.setValue(rs.getInt("value")); //3
		
		int temp_sentiment = rs.getInt("sentiment"); //4

		if (temp_sentiment == TweetSentiment.POSITIVE.getSentimentCode()) {
			bin.setSentiment(TweetSentiment.POSITIVE);
		} else if (temp_sentiment == TweetSentiment.NEUTRAL.getSentimentCode()) {
			bin.setSentiment(TweetSentiment.NEUTRAL);
		} else if (temp_sentiment == TweetSentiment.NEGATIVE.getSentimentCode()) {
			bin.setSentiment(TweetSentiment.NEGATIVE);
		}
			
		return bin;
	}

}
