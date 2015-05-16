package algo.ad.processor.tweets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import common.feeder.utility.TweetJDBCTemplate;
import data.collection.entity.Queries;
import data.collection.entity.TweetTableObject;

public class BoltSaveToMongoDB extends BaseBasicBolt {

	private static final long serialVersionUID = 6460656994454296875L;

	String MONGODB_BASE_URL;
	String MONGODB_USERNAME;
	String MONGODB_PASSWORD;
	String MONGODB_PORT;
	boolean isDebug = false;
	String MONGODB_TWEET_COLLECTION = "tweetData";

	public static DriverManagerDataSource datasource;
	public static TweetJDBCTemplate tweetsJDBCTemplate;

	// *******Custom Ticks****************
	long keywordRefreshInterval = 120000;// 60 sec //300000; // 5min //900000;
											// //15 minute
	long startTime = 0;
	// ***********************************

	List<TweetTableObject> listOfTweets;
	
	int totalSavedTweets = 0;
	int totalProcessedTweets = 0;
	int totalReceivedTweets = 0;
	
	public static Logger LOG = LoggerFactory.getLogger(BoltSaveToMongoDB.class);

	public BoltSaveToMongoDB(String mongoDBURL, String mongoDBPort,
			String username, String password, boolean _isDebug) {
		super();
		MONGODB_USERNAME = username;
		MONGODB_PASSWORD = password;
		MONGODB_BASE_URL = mongoDBURL;
		MONGODB_PORT = mongoDBPort;
		isDebug = _isDebug ;
		listOfTweets = new ArrayList<TweetTableObject>(0);
		startTime = System.currentTimeMillis();
	}

	/**
	 * REST API call to sentiment 140
	 */

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		try {
			long elapsedTime = System.currentTimeMillis() - startTime;

			if (elapsedTime > keywordRefreshInterval) {
				startTime = System.currentTimeMillis();
				processListToBuckets();
				listOfTweets.clear();
				if(isDebug){
				LOG.info("***************************TICKKKKKKKKKK");
				LOG.info("statics Received::"+ totalReceivedTweets + "Processed::"+ totalProcessedTweets+
						" Saved to DB::"+totalSavedTweets+" Missed::"+ (totalProcessedTweets-totalSavedTweets));
				}
			} else {
				List<Object> otherFields = Lists.newArrayList(tuple.getValues());
				TweetTableObject tweetObject = (TweetTableObject) otherFields
						.get(0);
				totalReceivedTweets++;
				if (tweetObject != null) {
					totalProcessedTweets++;
					listOfTweets.add(tweetObject);
					//LOG.info("Recieved tweetList::");
				}

			}
//			LOG.info("*************elapsedTime:" + elapsedTime + "||"
//					+ " list_count::" + listOfTweets.size());
		} catch (Exception e) {
			collector.reportError(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
	}

	
	void processListToBuckets() {
		// **********************************
		// Task 1 :: Get List of active queries from DB
		// Task 2 :: Tag Tweets with queries (TODO: Check Multi-tagging)
		// Task 3 :: Do sentiment Analysis
		// Task 4 :: Save to respective user Database
		// Task 5 :: Save to MongoDB
		// **********************************
		
		//******************************************
		//  	Task 1 : Get the Bucketed Map
		//******************************************
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

		//*****************************************************************
		//  	Task 2 : Insert Into MongoDB for each Key or Collection
		//*****************************************************************
		
		MongoClient mongoClient = new MongoClient(MONGODB_BASE_URL,
				Integer.valueOf(MONGODB_PORT));

		Iterator<String> keySetIterator = bucketsOfTweets.keySet().iterator();

		int i = 0;
		while (keySetIterator.hasNext()) {

			String key = keySetIterator.next();
			@SuppressWarnings("deprecation")
			DB db = mongoClient.getDB(key);
			if(isDebug){
			//	LOG.info("********* " + i + ". " + key + " *************");
			}
			List<TweetTableObject> objects = bucketsOfTweets.get(key);

			DBCollection table = getCollectionWithIndexes(db);
				
				BulkWriteOperation  bulkWriteOperation = table.initializeUnorderedBulkOperation();
				
				for (TweetTableObject tweetTableObject : objects) {
					BasicDBObject document = new BasicDBObject();
					document.put("query_id", String.valueOf(tweetTableObject.getQuery_id_forinsert()));
					document.put("created_at", tweetTableObject.getCreated_at());
					document.put("timestamp", tweetTableObject.getUnix_timestamp());
					document.put("sentiment_id",
							String.valueOf(tweetTableObject.getSentiment()));
	
					DBObject tweetJSONObject = (DBObject) JSON.parse(tweetTableObject
							.getJsonObject());
	
					tweetJSONObject.put("created_at", tweetTableObject.getCreated_at());
	
					document.put("tweet", tweetJSONObject);
					bulkWriteOperation.insert(document);
					
//					LOG.info("query: " + tweetTableObject.getQuery_for_insert()
//							+ " || query_id: "
//							+ tweetTableObject.getQuery_id_forinsert()
//							+ " || Text::" + tweetTableObject.getText());
				}
				
					bulkWriteOperation.execute();
		
//			LOG.info("********* " + i + ". END *************");
			i++;
			totalSavedTweets +=objects.size();
		}
		mongoClient.close();
	}
	
	/**
	 *  Helper method for MongoDB
	 *  TODO:: Move code to a separate MongoDB Helper Class
	 * @param db
	 * @return
	 */
	DBCollection getCollectionWithIndexes(DB db)
	{
		DBCollection table = null;
		if (!db.collectionExists(MONGODB_TWEET_COLLECTION)) {
			table = db.getCollection(MONGODB_TWEET_COLLECTION);

			BasicDBObject document;

			// 1. Text Index
			document = new BasicDBObject();
			document.put("sentiment_id", 1);
			document.put("query_id", 1);
			document.put("tweet.text", "text");
			document.put("created_at", 1);

			table.createIndex(document, "Text_index");
			
			// 2. Hashtags Index
			document = new BasicDBObject();
			document.put("sentiment_id", 1);
			document.put("query_id", 1);
			document.put("tweet.entities.hashtags.text", 1);
			document.put("created_at", 1);

			table.createIndex(document, "Hashtags_Index");

			// 3. user_mentions.screen_name Index
			document = new BasicDBObject();
			document.put("sentiment_id", 1);
			document.put("query_id", 1);
			document.put("tweet.entities.user_mentions.screen_name", 1);
			document.put("created_at", 1);

			table.createIndex(document, "user_mentions_screen_name_Index");

			// 4. tweet.user.screen_name Index
			document = new BasicDBObject();
			document.put("sentiment_id", 1);
			document.put("query_id", 1);
			document.put("tweet.user.screen_name", 1);
			document.put("created_at", 1);

			table.createIndex(document, "user.screen_name_Index");
		} else {
			table = db.getCollection(MONGODB_TWEET_COLLECTION);
		}
		
		return table;
	}
	

	// /Insert Into Database
}
