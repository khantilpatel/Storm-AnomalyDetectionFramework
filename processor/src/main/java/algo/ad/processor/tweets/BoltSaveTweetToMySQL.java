package algo.ad.processor.tweets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.Lists;
import common.feeder.utility.ApplicationConfigurationFile;
import common.feeder.utility.TweetJDBCTemplate;
import common.feeder.utility.TweetJDBCTemplateConnectionPool;

import data.collection.entity.Queries;
import data.collection.entity.TweetTableObject;

public class BoltSaveTweetToMySQL extends BaseBasicBolt {

	private static final long serialVersionUID = 6460656994454296875L;


	// *******Custom Ticks****************
	long keywordRefreshInterval = 120000;// 60 sec //300000; // 5min //900000;
											// //15 minute
	long startTime = 0;
	// ***********************************

	boolean isDebug = false;

	int totalSavedTweets = 0;
	int totalProcessedTweets = 0;
	int totalReceivedTweets = 0;

	ApplicationConfigurationFile configFile;
	
	List<TweetTableObject> listOfTweets;
	public static Logger LOG = LoggerFactory
			.getLogger(BoltSaveTweetToMySQL.class);

	public BoltSaveTweetToMySQL(ApplicationConfigurationFile _configFile, boolean _isDebug) {

		configFile = _configFile;

		isDebug = _isDebug;
		listOfTweets = new ArrayList<TweetTableObject>(0);
		startTime = System.currentTimeMillis();
	}

	/**
	 * REST API call to sentiment 140
	 */

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		long elapsedTime = System.currentTimeMillis() - startTime;

		if (elapsedTime > keywordRefreshInterval) {
			startTime = System.currentTimeMillis();
	
			processListToBuckets();
			listOfTweets.clear();
			if(isDebug){
			LOG.info("***************************TICKKKKKKKKKK");
			LOG.info("statics Received::" + totalReceivedTweets + "Processed::"
					+ totalProcessedTweets + " Saved to DB::"
					+ totalSavedTweets + " Missed::"
					+ (totalProcessedTweets - totalSavedTweets));
			}
		} else {
			List<Object> otherFields = Lists.newArrayList(tuple.getValues());
			TweetTableObject tweetObject = (TweetTableObject) otherFields
					.get(0);
			totalReceivedTweets++;
			if (tweetObject != null) {
				totalProcessedTweets++;
				listOfTweets.add(tweetObject);
				// LOG.info("Recieved tweetList::");
			}

			// System.out.println(tweetObject.getJsonObject());
		}
		// LOG.info("*************elapsedTime:" + elapsedTime + "||"
		// + " list_count::" + listOfTweets.size());
		// } catch (Exception e) {
		// collector.reportError(e);
		// }
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
	}

	void processListToBuckets() {

		Map<String, List<TweetTableObject>> bucketsOfTweets = new HashMap<String, List<TweetTableObject>>();

		for (TweetTableObject tweetTableObject : listOfTweets) {

			List<Queries> current_queries = tweetTableObject.getQueries();
			for (Queries query : current_queries) {

				String currentKey = query.getDb_name();
				if (currentKey != null) {
					if (bucketsOfTweets.containsKey(currentKey)) {
						tweetTableObject.setQuery_id_forinsert(query
								.getQuery_id());
						tweetTableObject.setDbName_for_insert(query
								.getDb_name());
						tweetTableObject.setQuery_for_insert(query.getQuery());

						bucketsOfTweets.get(currentKey).add(tweetTableObject);
					} else {
						List<TweetTableObject> list = new ArrayList<TweetTableObject>(
								0);

						tweetTableObject.setQuery_id_forinsert(query
								.getQuery_id());
						tweetTableObject.setDbName_for_insert(query
								.getDb_name());
						tweetTableObject.setQuery_for_insert(query.getQuery());

						list.add(tweetTableObject);

						bucketsOfTweets.put(currentKey, list);
					}
				}
			}

		}
		
		Iterator<String> keySetIterator = bucketsOfTweets.keySet().iterator();

		int i = 0;
		while (keySetIterator.hasNext()) {

			String key = keySetIterator.next();
			if (isDebug) {
			//	LOG.info("********* " + i + ". " + key + " *************");
			}
			
			TweetJDBCTemplate tweetsJDBCTemplate = TweetJDBCTemplateConnectionPool
					.getTweetJDBCTemplate(key, configFile);
					
					
			List<TweetTableObject> objects = bucketsOfTweets.get(key);
			// for (TweetTableObject tweetTableObject : objects) {
			// LOG.info("query: " + tweetTableObject.getQuery_for_insert()
			// + " || query_id: "
			// + tweetTableObject.getQuery_id_forinsert()
			// + " || Text::" + tweetTableObject.getText());
			// }

			tweetsJDBCTemplate.insertBulkTweet(objects);
			tweetsJDBCTemplate.insertBulkUserDetails(objects);
				
			// LOG.info("********* " + i + ". END *************");
			i++;
			totalSavedTweets += objects.size();
		}
	}

	// /Insert Into Database
}
