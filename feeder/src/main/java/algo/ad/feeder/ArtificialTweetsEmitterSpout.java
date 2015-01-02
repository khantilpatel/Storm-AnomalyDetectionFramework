/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package algo.ad.feeder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.RowCallbackHandler;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import algo.ad.feeder.dao.Tweet;
import algo.ad.feeder.dao.TweetJDBCTemplate;
import algo.ad.feeder.dao.TweetMapper;
import algo.ad.feeder.utility.AggregateUtilityFunctions;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class ArtificialTweetsEmitterSpout extends BaseRichSpout {
	/**
	 * 
	 */
	
	//********Constants**************
	final static int SENTIMENT_LOG = 2;
	private static final int AGGREGATION_FACTOR_MINUTES = 15;//360; //15
	final long SLEEP_FACTOR_MILLI_SEC = (long) 0.5; // 1 sec = 1000 msec
	//*******************************
	
	
	private static final long serialVersionUID = 7616822882515961452L;
	// public static Logger LOG = LoggerFactory
	// .getLogger(ArtificialTweetsEmitterSpout.class);
	boolean _isDistributed;
	SpoutOutputCollector _collector;
	public static DriverManagerDataSource datasource;
	public static TweetJDBCTemplate tweetsJDBCTemplate;
	List<Tweet> tweets;

	// Algorithm Variables
	public ArtificialTweetsEmitterSpout() {
		this(true);
	}

	public ArtificialTweetsEmitterSpout(boolean isDistributed) {
		_isDistributed = isDistributed;

		datasource = new DriverManagerDataSource();
		datasource.setDriverClassName("com.mysql.jdbc.Driver");
		datasource.setUrl("jdbc:mysql://localhost:3306/test");// test-replica
		datasource.setUsername("root");
		datasource.setPassword("root");

		tweetsJDBCTemplate = new TweetJDBCTemplate();
		tweetsJDBCTemplate.setDataSource(datasource);

		//System.out.println("------Listing Multiple Records--------");

//		tweets = tweetsJDBCTemplate
//				.listTweets("select * from tweets where query_id = 2");// AND 91
//																		// sentiment
//																		// IN
//																		// ('2')");

	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
	}

	public void close() {

	}

	public void nextTuple() {
		
		tweetsJDBCTemplate.jdbcTemplateObject.query("select * from tweets where query_id = 2 ORDER BY UNIX_TIMESTAMP ASC",
				
				new CustomRowCallbackHandler(_collector, AGGREGATION_FACTOR_MINUTES, SLEEP_FACTOR_MILLI_SEC));
		
		tweetsJDBCTemplate = null;
		// /OLD
		// METHOD////////////////////////////////////////////////////////////////////
		/*
		 * Utils.sleep(20); if (!tweets.isEmpty()) { Tweet tweet =
		 * tweets.remove(0); _collector.emit(new
		 * Values(tweet.getTweet_id(),tweet
		 * .getSentiment().getSentimentCode(),tweet.getCreated_at())); }
		 */
		// ////////////////////////////////////////////////////////////////////////////////

		// /////////////////////////////////////////////////////////////////////////////////////
		// NEW METHOD WHICH EMITS TUPLES BASED ON AGGREGATION FACTOR
		// If the aggregation is 6 hours, and sleep is 1 sec then it will emit
		// one data point
		// every sec and the data point will have the aggregated tweets for 6
		// hours.
		// ////////////////////////////////////////////////////////////////////////////////////

		
//		 new RowCallbackHandler()
//	        {
//				int count =0;
//	            public void processRow(ResultSet resultSet) throws SQLException
//	            {
//	                final long i = count++;
//	                if (i % 500000 == 0) System.out.println("Iterated " + i + " rows");
//	            }
//	        });
		
		
		// Variables
//		Tweet tweet = tweets.remove(0);
//		Date currentDate = tweet.getCreated_at();
//		Date nextAggregatedDate = AggregateUtilityFunctions.addMinutesToDate(
//				AGGREGATION_FACTOR_MINUTES, currentDate);
//
//		while (!tweets.isEmpty()) {
//			// While loop to emit the tuples with the aggregation condition
//			while (currentDate.compareTo(nextAggregatedDate) <= 0) {
//				_collector.emit(new Values(tweet.getTweet_id(), tweet
//						.getSentiment().getSentimentCode(), tweet
//						.getCreated_at(), nextAggregatedDate, 1));
//				tweet = tweets.remove(0);
//				currentDate = tweet.getCreated_at();
//			}
//
//			_collector.emit(new Values(tweet.getTweet_id(), 0, tweet
//					.getCreated_at(), nextAggregatedDate, 0));
//
//			_collector.emit(new Values(tweet.getTweet_id(), 2, tweet
//					.getCreated_at(), nextAggregatedDate, 0));
//
//			_collector.emit(new Values(tweet.getTweet_id(), 4, tweet
//					.getCreated_at(), nextAggregatedDate, 0));
//
//			System.out.println("||Aggregate Date::" + nextAggregatedDate);
//			nextAggregatedDate = AggregateUtilityFunctions.addMinutesToDate(
//					AGGREGATION_FACTOR_MINUTES, nextAggregatedDate);
//			Utils.sleep(SLEEP_FACTOR_MILLI_SEC);
//		}
		// /////////////////////////////////////////////////////////////////////////////////

	}

	public void ack(Object msgId) {

	}

	public void fail(Object msgId) {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_id", "sentiment_id", "date_obj",
				"aggregate_date_obj", "tupleType")); // TupleType: 0.Tick,
														// 1.Regular
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		if (!_isDistributed) {
			Map<String, Object> ret = new HashMap<String, Object>();
			ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
			return ret;
		} else {
			return null;
		}
	}

}

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
			
			try {
			
				nextAggregatedDate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2013-06-20 18:00:00");
						
					//	AggregateUtilityFunctions.addMinutesToDate(
						//AGGREGATION_FACTOR_MINUTES, currentDate);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

		// while (!tweets.isEmpty()) {
		// While loop to emit the tuples with the aggregation condition
		if (currentDate.compareTo(nextAggregatedDate) <= 0) {
			_collector.emit(new Values(tweet.getTweet_id(), tweet
					.getSentiment().getSentimentCode(), tweet.getCreated_at(),
					nextAggregatedDate, 1));
			// tweet = tweets.remove(0);
			//currentDate = tweet.getCreated_at();
		} else {
			do{
			_collector.emit(new Values(tweet.getTweet_id(), 0, tweet
					.getCreated_at(), nextAggregatedDate, 0));

			_collector.emit(new Values(tweet.getTweet_id(), 2, tweet
					.getCreated_at(), nextAggregatedDate, 0));

			_collector.emit(new Values(tweet.getTweet_id(), 4, tweet
					.getCreated_at(), nextAggregatedDate, 0));

			//System.out.println("||Aggregate Date::" + nextAggregatedDate);
					
			// update for next aggregation range;
			nextAggregatedDate = AggregateUtilityFunctions.addMinutesToDate(
					AGGREGATION_FACTOR_MINUTES, nextAggregatedDate);
			
			}
			while(currentDate.compareTo(nextAggregatedDate) > 0);
			
			
				_collector.emit(new Values(tweet.getTweet_id(), tweet
						.getSentiment().getSentimentCode(), tweet.getCreated_at(),
						nextAggregatedDate, 1));
		}
		
		Utils.sleep(SLEEP_FACTOR_MILLI_SEC);
	}

}