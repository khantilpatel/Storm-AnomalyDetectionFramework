package algo.ad.processor.tweets;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import common.feeder.utility.ApplicationConfigurationFile;
import common.feeder.utility.DatabaseHelper;
import common.feeder.utility.TupleHelpers;
import common.feeder.utility.TweetJDBCTemplate;
import common.feeder.utility.TweetJDBCTemplateConnectionPool;

import data.collection.entity.Queries;

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

	public static DriverManagerDataSource datasource;
	public static TweetJDBCTemplate tweetsJDBCTemplate;
	ApplicationConfigurationFile configFile;	
	public static Logger LOG = LoggerFactory
			.getLogger(BoltAnomalyDetectionAggregateTweets.class);

	//*****Data Structures*************************
	//List<Queries> queriesList;
	HashMap<Integer,Queries> activeQueries;
	//*********************************************

	public BoltAnomalyDetectionAggregateTweets(
			ApplicationConfigurationFile _config, boolean _isDebug) {

		configFile = _config;
		isDebug = _isDebug;
		//listOfTweets = new ArrayList<TweetTableObject>(0);
		startTime = System.currentTimeMillis();
	}

	/**
	 * REST API call to sentiment 140
	 */

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		long elapsedTime = System.currentTimeMillis() - startTime;

		if (TupleHelpers.isTickTuple(tuple)) {
			LOG.info("***********Original-isTickTuple****************TICKKKKKKKKKK");
		}

		if (elapsedTime > keywordRefreshInterval) {
			startTime = System.currentTimeMillis();
			List<Queries> queriesList;
			queriesList = DatabaseHelper.getKeywordsFromDBwithAnomalies(
					TweetJDBCTemplateConnectionPool
							.getTweetJDBCTemplate(DB_NAME_META,configFile),
					MAX_NUMBER_OF_QUERIES, configFile);
			
		//	activeQueries
			
		//	queries = tweetJDBCTemplate.listQueries("select * from queries");
			
			// processListToBuckets();
//			if (isDebug) {
//				// LOG.info("***************************TICKKKKKKKKKK");
//				LOG.info("statics Received::" + totalReceivedTweets
//						+ " Processed::" + totalProcessedTweets
//						+ " Saved to DB::" + totalSavedTweets + " Missed::"
//						+ (totalProcessedTweets - totalSavedTweets));
//			}
		} else {
			
		}
		// List<Object> otherFields = Lists.newArrayList(tuple.getValues());
		// TweetTableObject tweetObject = (TweetTableObject) otherFields
		// .get(0);
		// totalReceivedTweets++;
		// if (tweetObject != null) {
		// totalProcessedTweets++;
		// listOfTweets.add(tweetObject);
		// //LOG.info("Recieved tweetList::");
		// }
		//
		// // System.out.println(tweetObject.getJsonObject());
		// }
		// LOG.info("*************elapsedTime:" + elapsedTime + "||"
		// + " list_count::" + listOfTweets.size());
		// } catch (Exception e) {
		// collector.reportError(e);
		// }
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
