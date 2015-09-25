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
package common.feeder.spouts;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowCallbackHandler;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import common.feeder.utility.AggregateUtilityFunctions;
import common.feeder.utility.ApplicationConfigurationFile;
import common.feeder.utility.TweetJDBCTemplate;
import common.feeder.utility.TweetJDBCTemplateConnectionPool;

import data.collection.entity.Tweet;
import data.collection.mapper.TweetMapper;

public class ArtificialTweetsEmitterSpout extends BaseRichSpout {
	/**
	 * 
	 */
	
	//********Constants**************
	final static int SENTIMENT_LOG = 2;
	private static final int AGGREGATION_FACTOR_MINUTES = 60;//360; //15
	final long SLEEP_FACTOR_MILLI_SEC = (long)0.5 ; // 1 sec = 1000 msec
	Date referenceNextAggregateDate;
	String DATA_BASE_NAME = "live_queries";	//"test";
	//*******************************
	
	
	private static final long serialVersionUID = 7616822882515961452L;
	// public static Logger LOG = LoggerFactory
	// .getLogger(ArtificialTweetsEmitterSpout.class);
	boolean _isDistributed;
	SpoutOutputCollector _collector;
	public static DriverManagerDataSource datasource;
	public static TweetJDBCTemplate tweetsJDBCTemplate;
	List<Tweet> tweets;
	ApplicationConfigurationFile configFile;
	
	public static Logger LOG = LoggerFactory
			.getLogger(ArtificialTweetsEmitterSpout.class);
	
	public ArtificialTweetsEmitterSpout(boolean isDistributed, 	ApplicationConfigurationFile _configFile) {
		_isDistributed = isDistributed;
		
		try {
			TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
			referenceNextAggregateDate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2015-09-06 02:00:00");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		datasource = new DriverManagerDataSource();
//		datasource.setDriverClassName("com.mysql.jdbc.Driver");
//		datasource.setUrl("jdbc:mysql://localhost:3307/test");// test-replica
//		datasource.setUsername("vistaroot");
//		datasource.setPassword("vista&mysql");
//
//		tweetsJDBCTemplate = new TweetJDBCTemplate();
//		tweetsJDBCTemplate.setDataSource(datasource);

		configFile = _configFile;
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
		
		TweetJDBCTemplateConnectionPool
		.getTweetJDBCTemplate(DATA_BASE_NAME,configFile).jdbcTemplateObject.query("select * from tweets where query_id"
				+ " = 115 ORDER BY UNIX_TIMESTAMP ASC",
				
				new CustomRowCallbackHandler(_collector, AGGREGATION_FACTOR_MINUTES, SLEEP_FACTOR_MILLI_SEC, LOG, referenceNextAggregateDate));
		
		configFile = null;
		
		Utils.sleep(5000000);
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

	int count;
	
	SpoutOutputCollector _collector;
	int AGGREGATION_FACTOR_MINUTES;
	long SLEEP_FACTOR_MILLI_SEC;
	TweetMapper mapper = new TweetMapper();
	Date nextAggregatedDate;
	Logger LOG;
	Date referenceNextAggregateDate;
	
	public CustomRowCallbackHandler(SpoutOutputCollector _collector,
			int AGGREGATION_FACTOR_MINUTES, long SLEEP_FACTOR_MILLI_SEC, Logger _LOG, Date _referenceNextAggregateDate) {
		super();
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		this._collector = _collector;
		this.AGGREGATION_FACTOR_MINUTES = AGGREGATION_FACTOR_MINUTES;
		this.SLEEP_FACTOR_MILLI_SEC = SLEEP_FACTOR_MILLI_SEC;
		referenceNextAggregateDate = _referenceNextAggregateDate;
		LOG = _LOG;
	}

	@Override
	public void processRow(ResultSet rs) throws SQLException {
		// ///////////////////////////////
		
		//System.out.println("ATE:Default TimeZone:"+TimeZone.getDefault());
		Tweet tweet = mapper.mapRow(rs, 0);
		Date currentDate = tweet.getCreated_at();

		if (nextAggregatedDate == null) {
			
				nextAggregatedDate = referenceNextAggregateDate; //new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2013-06-20 18:00:00");
						
					//	AggregateUtilityFunctions.addMinutesToDate(
						//AGGREGATION_FACTOR_MINUTES, currentDate);
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
				if( tweet.getCreated_at() == null)
				{
					System.out.println(" tweet.getCreated_at() is null");
				}
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
		count++;
		LOG.info("Current cout is:: "+count + " / 7498");
		Utils.sleep(SLEEP_FACTOR_MILLI_SEC);
	}

}


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


// new RowCallbackHandler()
//    {
//		int count =0;
//        public void processRow(ResultSet resultSet) throws SQLException
//        {
//            final long i = count++;
//            if (i % 500000 == 0) System.out.println("Iterated " + i + " rows");
//        }
//    });


// Variables
//Tweet tweet = tweets.remove(0);
//Date currentDate = tweet.getCreated_at();
//Date nextAggregatedDate = AggregateUtilityFunctions.addMinutesToDate(
//		AGGREGATION_FACTOR_MINUTES, currentDate);
//
//while (!tweets.isEmpty()) {
//	// While loop to emit the tuples with the aggregation condition
//	while (currentDate.compareTo(nextAggregatedDate) <= 0) {
//		_collector.emit(new Values(tweet.getTweet_id(), tweet
//				.getSentiment().getSentimentCode(), tweet
//				.getCreated_at(), nextAggregatedDate, 1));
//		tweet = tweets.remove(0);
//		currentDate = tweet.getCreated_at();
//	}
//
//	_collector.emit(new Values(tweet.getTweet_id(), 0, tweet
//			.getCreated_at(), nextAggregatedDate, 0));
//
//	_collector.emit(new Values(tweet.getTweet_id(), 2, tweet
//			.getCreated_at(), nextAggregatedDate, 0));
//
//	_collector.emit(new Values(tweet.getTweet_id(), 4, tweet
//			.getCreated_at(), nextAggregatedDate, 0));
//
//	System.out.println("||Aggregate Date::" + nextAggregatedDate);
//	nextAggregatedDate = AggregateUtilityFunctions.addMinutesToDate(
//			AGGREGATION_FACTOR_MINUTES, nextAggregatedDate);
//	Utils.sleep(SLEEP_FACTOR_MILLI_SEC);
//}
// /////////////////////////////////////////////////////////////////////////////////
