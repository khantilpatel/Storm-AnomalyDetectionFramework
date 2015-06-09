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

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import common.feeder.utility.FileIOUtility;
import common.feeder.utility.TweetJDBCTemplate;
import common.feeder.utility.TweetJDBCTemplateConnectionPool;
import data.collection.entity.QueriesForEmitter;
import data.collection.entity.Tweet;
import data.collection.entity.TweetTableObject;

public class MultipleQueriesTweetsEmitterSpout extends BaseRichSpout {

	/**
	 * ************Bunch of variables to declare*************************
	 */
	// ********Constants*************************
	final static int SENTIMENT_LOG = 2;
	private static final int AGGREGATION_FACTOR_MINUTES = 15;// 360; //15
	final long SLEEP_FACTOR_MILLI_SEC = (long) 0.5; // 1 sec = 1000 msec
	// *******************************

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
			.getLogger(MultipleQueriesTweetsEmitterSpout.class);

	List<List<TweetTableObject>> listOfQueryData = new ArrayList<List<TweetTableObject>>(
			0);

	List<Iterator<TweetTableObject>> iterators = new ArrayList<Iterator<TweetTableObject>>(
			0);

	Map<String, Date> mapList = new HashMap<String, Date>(0);

	/**
	 * *****************************************************************
	 */

	/**
	 * ***********Storm Functions**************************************
	 */
	public MultipleQueriesTweetsEmitterSpout(boolean isDistributed,
			ApplicationConfigurationFile _configFile) {
		_isDistributed = isDistributed;

		configFile = _configFile;

		listOfQueryData = initQueryData();
		for (List<TweetTableObject> tweetList : listOfQueryData) {
			iterators.add(tweetList.iterator());
		}
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
	}

	public void close() {

	}

	public void nextTuple() {

		// TweetJDBCTemplateConnectionPool
		// .getTweetJDBCTemplate("test", configFile).jdbcTemplateObject
		// .query("select * from tweets where query_id"
		// + " = 2 ORDER BY UNIX_TIMESTAMP ASC",
		//
		// new CustomRowCallbackHandler(_collector,
		// AGGREGATION_FACTOR_MINUTES,
		// SLEEP_FACTOR_MILLI_SEC, LOG));
		//
		// configFile = null;

		// Utils.sleep(500);

		Random randomizer = new Random();

		Iterator<TweetTableObject> iterator = iterators.get(randomizer
				.nextInt(iterators.size()));

		if (iterator.hasNext()) {
			System.out.println(iterator.next());
		}

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

	/**
	 ******************************************************************************
	 */

	/**
	 *********************** user defined functions********************************
	 */

	List<List<TweetTableObject>> initQueryData() {

		List<List<TweetTableObject>> listOfQueries = FileIOUtility
				.readTweetListFromFile("tweetList.txt");

		if (listOfQueries == null) {
			listOfQueries = new ArrayList<List<TweetTableObject>>(0);

			List<QueriesForEmitter> queries = new ArrayList<QueriesForEmitter>();

			// *******************************************************************************
			QueriesForEmitter query = new QueriesForEmitter();

			query.setDb_name("test");
			query.setQuery("#tdf");
			query.setQuery_id(2);
			try {
				query.setStart_date(new
						 SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2013-06-20 18:00:00"));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			queries.add(query);

			// *******************************************************************************
//			query = new QueriesForEmitter();
//
//			query.setDb_name("");
//			query.setQuery("");
//			query.setQuery_id(1);
//			query.setStart_date(new Date());
//
//			queries.add(query);
			// *******************************************************************************
//			query = new QueriesForEmitter();
//
//			query.setDb_name("");
//			query.setQuery("");
//			query.setQuery_id(1);
//			query.setStart_date(new Date());
//
//			queries.add(query);
			// *****************************************************************************
			for (QueriesForEmitter query1 : queries) {
				listOfQueries.add(fetchTweetsForQuery(query1));
			}
			
			FileIOUtility.saveTweetListToFile(listOfQueries, "tweetList.txt");

		}
		return listOfQueries;

	}

	List<TweetTableObject> fetchTweetsForQuery(QueriesForEmitter query) {

		List<TweetTableObject> tweets = TweetJDBCTemplateConnectionPool
				.getTweetJDBCTemplate(query.getDb_name(), configFile)
				.listTweetTableObjects(
						"select * from tweets where query_id ="
								+ query.getQuery_id()
								+ " ORDER BY UNIX_TIMESTAMP ASC");

		return tweets;

	}

	public void processRow(TweetTableObject tweet, QueriesForEmitter query)
			throws SQLException {
		// ///////////////////////////////
		// TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		// System.out.println("ATE:Default TimeZone:"+TimeZone.getDefault());
		Date currentDate = tweet.getCreated_at();

		Date nextAggregatedDate = mapList.get(query.getQuery());

		if (nextAggregatedDate == null) {
			// nextAggregatedDate = new
			// SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2013-06-20 18:00:00");
			nextAggregatedDate = query.getStart_date();
			// AggregateUtilityFunctions.addMinutesToDate(
			// AGGREGATION_FACTOR_MINUTES, currentDate);

		}

		// while (!tweets.isEmpty()) {
		// While loop to emit the tuples with the aggregation condition
		if (currentDate.compareTo(nextAggregatedDate) <= 0) {
			_collector.emit(new Values(tweet, tweet.getSentiment(), tweet
					.getCreated_at(), nextAggregatedDate, 1));
			// tweet = tweets.remove(0);
			// currentDate = tweet.getCreated_at();
		} else {
			do {
				_collector.emit(new Values(null, 0, tweet.getCreated_at(),
						nextAggregatedDate, 0));

				_collector.emit(new Values(null, 2, tweet.getCreated_at(),
						nextAggregatedDate, 0));

				_collector.emit(new Values(null, 4, tweet.getCreated_at(),
						nextAggregatedDate, 0));

				// System.out.println("||Aggregate Date::" +
				// nextAggregatedDate);

				// update for next aggregation range;
				nextAggregatedDate = AggregateUtilityFunctions
						.addMinutesToDate(AGGREGATION_FACTOR_MINUTES,
								nextAggregatedDate);

			} while (currentDate.compareTo(nextAggregatedDate) > 0);
			_collector.emit(new Values(tweet, tweet.getSentiment(), tweet
					.getCreated_at(), nextAggregatedDate, 1));
		}

		// count++;

		// LOG.info("Current cout is:: "+count + " / 449077");
		Utils.sleep(SLEEP_FACTOR_MILLI_SEC);
	}

	/**
	 *******************************************************************************
	 */

}