package algo.ad.processor.tweets;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.HashtagEntity;
import twitter4j.UserMentionEntity;
import algo.ad.utility.StatusToTweetTableTransform;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

import data.collection.entity.Queries;
import data.collection.entity.RawTweet;
import data.collection.entity.TweetTableObject;

/**
 * 
 * @author Khantil Patel
 * 
 *         TagTweetsWithQueryBolt will process the RAW Tweet and tag them with a
 *         query_id it may belong to. The list of queries is passed from parent
 *         Spout
 *
 *         <<--parallelism Note: We intend to execute 3-executor (3-Tasks by
 *         default)-->>
 */
public class BoltTagTweetsWithQuery extends BaseBasicBolt {

	private static final long serialVersionUID = 6460656994454296875L;
	String DB_NAME = "meta_db";
	// int MAX_NUMBER_OF_QUERIES = 200;
	 long keywordRefreshInterval = 60000;// 60 sec //300000; // 5min //900000;
	// //15 minute
	// ArrayList<RawTweet> statusList;
	 long startTime = 0;

	List<Queries> queries;

	static int totalProcessedTweets = 0;
	static int totalEmitedTweets = 0;
	boolean isDebug = false;
	public static Logger LOG = LoggerFactory
			.getLogger(BoltTagTweetsWithQuery.class);

	public BoltTagTweetsWithQuery(boolean _isDebug) {
		super();
		// TODO Auto-generated constructor stub
		// startTime = System.currentTimeMillis();
		// statusList = new ArrayList<RawTweet>();
		queries = new ArrayList<Queries>(0);
		startTime = System.currentTimeMillis();
		isDebug = _isDebug;
	}

	/**
	 * REST API call to sentiment 140
	 */

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		totalProcessedTweets++;
		// try {
		List<Object> otherFields = Lists.newArrayList(tuple.getValues());
		RawTweet rawtweetObject = (RawTweet) otherFields.get(0);
		// if(otherFields.get(1) != null){
		queries = (List<Queries>) otherFields.get(1);
		// }
		
		TweetTableObject tweetObjectWithQuery_Id = tagCollectionOfTweets(rawtweetObject);
		// statusList.add(tweetObject);
		// System.out.println(rawtweetObject.getJsonObjectString());

		if (!tweetObjectWithQuery_Id.getQueries().isEmpty()) {
			totalEmitedTweets++;
			collector.emit(new Values(tweetObjectWithQuery_Id));
		}
		
		long elapsedTime = System.currentTimeMillis() - startTime;
		// LOG.info("*************elapsedTime:" + elapsedTime);
		if (elapsedTime > keywordRefreshInterval) {
			startTime = System.currentTimeMillis();
			if(isDebug){
				LOG.info("statics Processed::"+ totalProcessedTweets+
						" Emitted::"+totalEmitedTweets+" Missed::"+ (totalProcessedTweets-totalEmitedTweets));
				}
		}

		// } catch (Exception e) {
		// collector.reportError(e);
		// }
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declare(new Fields("tweetObjectWithQuery_Id"));
	}

	/**
	 * Take Raw Tweet Object, convert to TweetTableObject and tag with query
	 * 
	 * @param rawtweetObject
	 */
	TweetTableObject tagCollectionOfTweets(RawTweet rawtweetObject) {

		// **********************************
		// Task 1 :: Get List of active queries from DB
		// Task 2 :: Tag Tweets with queries (TODO: Check Multi-tagging)
		// Task 3 :: Do sentiment Analysis
		// Task 4 :: Save to respective user Database

		// **********************************
		TweetTableObject tweetObject = StatusToTweetTableTransform
				.transformStatusToTweet(rawtweetObject);

		for (Queries query : queries) {

			if ((query.getQuery() != null || query.getQuery() != "") || 
					(query.getDb_name() != null || query.getDb_name() != "")) {
				// String Matcher = "(?i).*" + query.getQuery().toLowerCase() +
				// ".*";
				if (query.getDb_name() == null)
					LOG.info("Null DBNAME **************");

				// Check if the current Query is a Hashtag
				if (query.getQuery().startsWith("#")) {
					HashtagEntity[] hashtags = tweetObject.getStatus()
							.getHashtagEntities();
					for (HashtagEntity hashtagEntity : hashtags) {
						if (query.getQuery()
								.toLowerCase()
								.contains(hashtagEntity.getText().toLowerCase()))// .matches(Matcher))
						{

							tweetObject.addToQueries(query);
							break;
						}
					}
				}else if (query.getQuery().startsWith("@")) {
					// Check if the current Query is a @useraccount
					UserMentionEntity[] userMentions = tweetObject.getStatus()
							.getUserMentionEntities();
					for (UserMentionEntity userMentionEntity : userMentions) {
						if ((query.getQuery().toLowerCase().contains(
								userMentionEntity.getScreenName().toLowerCase()))
									|| (query.getQuery().toLowerCase()
										.contains(userMentionEntity.getName().toLowerCase())))
						{
							tweetObject.addToQueries(query);
							break;
						}
					}
				}else if (tweetObject.getText().toLowerCase()
						.contains(query.getQuery().toLowerCase()))// .matches(Matcher))
				{
					tweetObject.addToQueries(query);

				} else if (tweetObject.isRetweet()) {
					if (tweetObject.getText_retweeted().toLowerCase()
							.contains(query.getQuery().toLowerCase()))// .matches(Matcher))
					{
						tweetObject.addToQueries(query);
					}
				}
			}
		}
		if (tweetObject.getQueries().isEmpty()) {

			LOG.error("No Match Found for Tweet:: " + tweetObject.getText());
		}

		return tweetObject;
	}
}