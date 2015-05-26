package common.feeder.utility;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.springframework.jdbc.core.RowCallbackHandler;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import data.collection.entity.Tweet;
import data.collection.mapper.TweetMapper;

class CustomRowCallbackHandler implements RowCallbackHandler {

	private StringBuilder sb = new StringBuilder();

	SpoutOutputCollector _collector;
	int AGGREGATION_FACTOR_MINUTES;
	long SLEEP_FACTOR_MILLI_SEC;
	TweetMapper mapper = new TweetMapper();
	Date nextAggregatedDate;

	public CustomRowCallbackHandler(SpoutOutputCollector _collector,
			int AGGREGATION_FACTOR_MINUTES, long SLEEP_FACTOR_MILLI_SEC) {
		super();
		this._collector = _collector;
		this.AGGREGATION_FACTOR_MINUTES = AGGREGATION_FACTOR_MINUTES;
		this.SLEEP_FACTOR_MILLI_SEC = SLEEP_FACTOR_MILLI_SEC;
	}

	@Override
	public void processRow(ResultSet rs) throws SQLException {
		// ///////////////////////////////
		//TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		//System.out.println("ATE:Default TimeZone:"+TimeZone.getDefault());
		Tweet tweet = mapper.mapRow(rs, 0);
		Date currentDate = tweet.getCreated_at();

		if (nextAggregatedDate == null) {
			
//			try {
//			
//				nextAggregatedDate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2013-06-20 18:00:00");
//					//	AggregateUtilityFunctions.addMinutesToDate(
//						//AGGREGATION_FACTOR_MINUTES, currentDate);
//			} catch (ParseException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			
			nextAggregatedDate = AggregateUtilityFunctions.addMinutesToDate(
					AGGREGATION_FACTOR_MINUTES, currentDate);
			
		}

		// while (!tweets.isEmpty()) {
		// While loop to emit the tuples with the aggregation condition
		if (currentDate.compareTo(nextAggregatedDate) <= 0) {
			_collector.emit(new Values(tweet, tweet
					.getSentiment().getSentimentCode(), tweet.getCreated_at(),
					nextAggregatedDate, 1));
			// tweet = tweets.remove(0);
			//currentDate = tweet.getCreated_at();
		} else {
			do{
			_collector.emit(new Values(null, 0, tweet
					.getCreated_at(), nextAggregatedDate, 0));

			_collector.emit(new Values(null, 2, tweet
					.getCreated_at(), nextAggregatedDate, 0));

			_collector.emit(new Values(null, 4, tweet
					.getCreated_at(), nextAggregatedDate, 0));

			//System.out.println("||Aggregate Date::" + nextAggregatedDate);
					
			// update for next aggregation range;
			nextAggregatedDate = AggregateUtilityFunctions.addMinutesToDate(
					AGGREGATION_FACTOR_MINUTES, nextAggregatedDate);
			
			}
			while(currentDate.compareTo(nextAggregatedDate) > 0);
				_collector.emit(new Values(tweet, tweet
						.getSentiment().getSentimentCode(), tweet.getCreated_at(),
						nextAggregatedDate, 1));
		}
		
		Utils.sleep(SLEEP_FACTOR_MILLI_SEC);
	}

}