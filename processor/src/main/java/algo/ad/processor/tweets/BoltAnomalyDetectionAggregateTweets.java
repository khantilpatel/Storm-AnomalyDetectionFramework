package algo.ad.processor.tweets;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import algo.ad.dao.TweetAggregateBin;
import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

import common.feeder.utility.AggregateUtilityFunctions;
import common.feeder.utility.ApplicationConfigurationFile;
import common.feeder.utility.DatabaseHelper;
import common.feeder.utility.TupleHelpers;
import common.feeder.utility.TweetJDBCTemplateConnectionPool;
import data.collection.entity.Queries;
import data.collection.entity.TweetTableObject;

public class BoltAnomalyDetectionAggregateTweets extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8961875108592383706L;

	// *******Custom Ticks****************
	long keywordRefreshInterval = 120000;// 60 sec //300000; // 5min //900000;
											// //15 minute
	long startTime = 0;
	// ***********************************
	// *********DB Related****************
	String DB_NAME_META = "meta_db";
	// ***********************************
	int MAX_NUMBER_OF_QUERIES = 200;

	boolean isDebug = false;

	int totalSavedTweets = 0;
	int totalProcessedTweets = 0;
	int totalReceivedTweets = 0;

	ApplicationConfigurationFile configFile;
	public static Logger LOG = LoggerFactory
			.getLogger(BoltAnomalyDetectionAggregateTweets.class);

	Map<String, Date> map_nextAggregationDate = new HashMap<String, Date>();

	Map<String, Integer> map_queryCounter = null;

	Map<String, Queries> map_queriesList = null;

	Map<String, List<TweetTableObject>> map_tweetList = new HashMap<String, List<TweetTableObject>>();

	// *****Data Structures*************************
	// List<Queries> queriesList;
	HashMap<Integer, Queries> activeQueries;

	// *********************************************

	public BoltAnomalyDetectionAggregateTweets(
			ApplicationConfigurationFile _config, boolean _isDebug) {

		configFile = _config;
		isDebug = _isDebug;
		// listOfTweets = new ArrayList<TweetTableObject>(0);
		startTime = System.currentTimeMillis();
	}

	/**
	 * REST API call to sentiment 140
	 */

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		long elapsedTime = System.currentTimeMillis() - startTime;

		// ****************For Updating the QueryList
		// Periodically*********************
		if (elapsedTime > keywordRefreshInterval || map_queriesList == null) {
			startTime = System.currentTimeMillis();

			if (map_queriesList == null) {
				Map<String, Queries> map_queriesList_temp = new HashMap<String, Queries>();
				Map<String, Integer> map_queryCounter_temp = new HashMap<String, Integer>();
				Map<String, Date> map_nextAggregationDateTemp = new HashMap<String, Date>();

				List<Queries> queriesList_new = DatabaseHelper
						.getKeywordsFromDBwithAnomalies(
								TweetJDBCTemplateConnectionPool
										.getTweetJDBCTemplate(DB_NAME_META,
												configFile),
								MAX_NUMBER_OF_QUERIES, configFile);
				
				for (Queries queries : queriesList_new) {
					long query_id = queries.getQuery_id();

					String user_id = queries.getDb_name();

					String key_nextAggregation = user_id + query_id;

					String key_queryCounter_positive = queries.getDb_name()
							+ queries.getQuery_id() + 0;

					String key_queryCounter_negative = queries.getDb_name()
							+ queries.getQuery_id() + 4;

					String key_queryCounter_neutral = queries.getDb_name()
							+ queries.getQuery_id() + 2;
					
					//if(map)
				}
				
			} else {

				List<Queries> queriesList_new = DatabaseHelper
						.getKeywordsFromDBwithAnomalies(
								TweetJDBCTemplateConnectionPool
										.getTweetJDBCTemplate(DB_NAME_META,
												configFile),
								MAX_NUMBER_OF_QUERIES, configFile);

			}
			LOG.info("***********Original-isTickTuple****************TICKKKKKKKKKK");
		}
		// ****************************************************************************

		// ****************For checking the
		// nextAggregationDate in case no tweets is
		// arrived***********************
		if (TupleHelpers.isTickTuple(tuple)) {
			Iterator<String> keySetIterator = map_queriesList.keySet()
					.iterator();

			Date currentDate = new Date();

			int i = 0;
			while (keySetIterator.hasNext()) {
				String key = keySetIterator.next();
				Queries query = map_queriesList.get(key);

				String key_nextAggregation = query.getDb_name()
						+ query.getQuery_id();

				Date nextAggregationDate = map_nextAggregationDate
						.get(key_nextAggregation);

				if (currentDate.compareTo(nextAggregationDate) > 1) {

					emitAggregatedTuple(collector, query);
				}
			}

		} else {

			// ****************************************************************************

			// ****************For checking normal tuple
			// processing*********************

			// get the query_id, sentiment_id, tweetObject,

			List<Object> otherFields = Lists.newArrayList(tuple.getValues());
			TweetTableObject tweetObject = (TweetTableObject) otherFields
					.get(0);

			List<Queries> queries_for_current_tweets = tweetObject.getQueries();

			for (Queries queries : queries_for_current_tweets) {

				long query_id = queries.getQuery_id();

				String user_id = queries.getDb_name();

				String key_nextAggregation = user_id + query_id;

				String key_queryCounter = user_id + query_id
						+ tweetObject.getSentiment();

				Date nextDate = map_nextAggregationDate
						.get(key_nextAggregation);

				if (tweetObject.getCreated_at().compareTo(nextDate) > 1) {

					emitAggregatedTuple(collector, queries);

				} else {
					// increament the count
					map_queryCounter.put(key_queryCounter,
							map_queryCounter.get(key_queryCounter) + 1);

					List<TweetTableObject> tweets = map_tweetList
							.get(key_queryCounter);

					tweets.add(tweetObject);

					map_tweetList.put(key_queryCounter, tweets);
				}
			}
		}

	}

	private void emitAggregatedTuple(BasicOutputCollector collector,
			Queries queries) {

		long query_id = queries.getQuery_id();

		String user_id = queries.getDb_name();

		String key_nextAggregation = user_id + query_id;

		String key_queryCounter_positive = queries.getDb_name()
				+ queries.getQuery_id() + 0;

		String key_queryCounter_negative = queries.getDb_name()
				+ queries.getQuery_id() + 4;

		String key_queryCounter_neutral = queries.getDb_name()
				+ queries.getQuery_id() + 2;

		Date nextDate = map_nextAggregationDate.get(key_nextAggregation);

		// 1. Emit for Sentiment Positve

		TweetAggregateBin bin_positive = new TweetAggregateBin();
		//bin_positive.setTweetList(map_tweetList.get(key_queryCounter_positive));
		bin_positive
				.setCounter(map_queryCounter.get(key_queryCounter_positive));
		bin_positive.setDate(nextDate);
		bin_positive
				.setSentiment_id(Integer.valueOf(key_queryCounter_positive));

		collector.emit(new Values(bin_positive)); // Counter,
													// query_id,
													// Sentiment_id
													// TweetList
		map_queryCounter.put(key_queryCounter_positive, 0);

		// 2. Emit for Sentiment Neutral

		TweetAggregateBin bin_neutral = new TweetAggregateBin();
		//bin_positive.setTweetList(map_tweetList.get(key_queryCounter_neutral));
		bin_positive.setCounter(map_queryCounter.get(key_queryCounter_neutral));
		bin_positive.setDate(nextDate);
		bin_positive.setSentiment_id(Integer.valueOf(key_queryCounter_neutral));

		collector.emit(new Values(bin_neutral));

		map_queryCounter.put(key_queryCounter_neutral, 0);

		// 3. Emit for sentiment Negative
		TweetAggregateBin bin_negative = new TweetAggregateBin();
		//bin_positive.setTweetList(map_tweetList.get(key_queryCounter_negative));
		bin_positive
				.setCounter(map_queryCounter.get(key_queryCounter_negative));
		bin_positive.setDate(nextDate);
		bin_positive
				.setSentiment_id(Integer.valueOf(key_queryCounter_negative));

		collector.emit(new Values(bin_negative));

		map_queryCounter.put(key_queryCounter_negative, 0);

		// increment the aggregation date
		map_nextAggregationDate.put(key_nextAggregation,
				AggregateUtilityFunctions.addMinutesToDate(22, nextDate));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		int tickFrequencyInSeconds = 10;
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
		return conf;
	}

	// void processListToBuckets() {
	//
	// Map<String, List<TweetTableObject>> bucketsOfTweets = new HashMap<String,
	// List<TweetTableObject>>();
	//
	// for (TweetTableObject tweetTableObject : listOfTweets) {
	//
	// List<Queries> current_queries = tweetTableObject.getQueries();
	// for (Queries query : current_queries) {
	//
	// String currentKey = query.getDb_name();
	// if (currentKey != null) {
	// if (bucketsOfTweets.containsKey(currentKey)) {
	// tweetTableObject.setQuery_id_forinsert(query
	// .getQuery_id());
	// tweetTableObject.setDbName_for_insert(query
	// .getDb_name());
	// tweetTableObject.setQuery_for_insert(query.getQuery());
	//
	// bucketsOfTweets.get(currentKey).add(tweetTableObject);
	// } else {
	// List<TweetTableObject> list = new ArrayList<TweetTableObject>(
	// 0);
	//
	// tweetTableObject.setQuery_id_forinsert(query
	// .getQuery_id());
	// tweetTableObject.setDbName_for_insert(query
	// .getDb_name());
	// tweetTableObject.setQuery_for_insert(query.getQuery());
	//
	// list.add(tweetTableObject);
	//
	// bucketsOfTweets.put(currentKey, list);
	// }
	// }
	// }
	//
	// }
	//
	// datasource = new DriverManagerDataSource();
	// datasource.setDriverClassName("com.mysql.jdbc.Driver");
	// datasource.setUrl(DB_BASE_URL + DB_NAME);// test-replica
	// datasource.setUsername(USERNAME);
	// datasource.setPassword(PASSWORD);
	//
	// tweetsJDBCTemplate = new TweetJDBCTemplate();
	// tweetsJDBCTemplate.setDataSource(datasource);
	//
	// if (tweetsJDBCTemplate == null) {
	// LOG.error("tweetsJDBCTemplate found to be null");
	// }
	//
	// Iterator<String> keySetIterator = bucketsOfTweets.keySet().iterator();
	//
	// int i = 0;
	// while (keySetIterator.hasNext()) {
	//
	// String key = keySetIterator.next();
	// if(isDebug){
	// // LOG.info("********* " + i + ". " + key + " *************");
	// }
	// datasource.setUrl(DB_BASE_URL + key); // test-replica
	// tweetsJDBCTemplate.setDataSource(datasource);
	// List<TweetTableObject> objects = bucketsOfTweets.get(key);
	// // for (TweetTableObject tweetTableObject : objects) {
	// // LOG.info("query: " + tweetTableObject.getQuery_for_insert()
	// // + " || query_id: "
	// // + tweetTableObject.getQuery_id_forinsert()
	// // + " || Text::" + tweetTableObject.getText());
	// // }
	//
	//
	// tweetsJDBCTemplate.insertBulkTweetGeoLocationFeature(objects);
	// tweetsJDBCTemplate.insertBulkRetweetFeature(objects);
	//
	// // LOG.info("********* " + i + ". END *************");
	// i++;
	// totalSavedTweets +=objects.size();
	// }
	//
	// }

	// /Insert Into Database
}
