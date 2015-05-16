package algo.ad.processor.tweets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import algo.ad.dao.Sentiment140Request;
import algo.ad.dao.Sentiment140RequestDAO;
import algo.ad.dao.Sentiment140Response;
import algo.ad.dao.Sentiment140ResponseDAO;
import algo.ad.utility.HTMLEscapeUtil;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

import common.feeder.utility.TupleHelpers;
import data.collection.entity.Queries;
import data.collection.entity.TweetTableObject;

public class BoltSentiment140 extends BaseBasicBolt {

	private static final long serialVersionUID = 6460656994454296875L;
	String DB_NAME = "meta_db";
	int MAX_NUMBER_OF_QUERIES = 200;
	String SENTIMENT140_URL;
	// *******Custom Ticks****************
	long keywordRefreshInterval = 60000;// 60 sec //300000; // 5min //900000;
										// //15 minute
	long startTime = 0;
	
	int totalProcessedTweets = 0;
	int totalEmitedTweets = 0;
	// ***********************************

	boolean isDebug = false;
	 
	public static Logger LOG = LoggerFactory.getLogger(BoltSentiment140.class);

	ArrayList<TweetTableObject> tweetList = new ArrayList<TweetTableObject>(0);

	/**
	 * Constructor
	 * 
	 * @param jdbcURL
	 * @param jdbcUsername
	 * @param jdbcpassword
	 */
	public BoltSentiment140(String sentiment140_url, boolean _isdebug) {
		super();
		// TODO Auto-generated constructor stub
		startTime = System.currentTimeMillis();
		SENTIMENT140_URL = sentiment140_url;
		isDebug = _isdebug;
	}

	/**
	 * REST API call to sentiment 140
	 */
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		// try {
		if (TupleHelpers.isTickTuple(tuple)) {
			LOG.info("***********Original-isTickTuple****************TICKKKKKKKKKK");
		}
		long elapsedTime = System.currentTimeMillis() - startTime;
		// LOG.info("*************elapsedTime:" + elapsedTime);
		if (elapsedTime > keywordRefreshInterval) {
			startTime = System.currentTimeMillis();
			//LOG.info("***************************TICKKKKKKKKKK");

			processCollectionOfTweets();
			if (tweetList.size() > 0) {
				//LOG.info("Emitting tweetList size::" + tweetList.size());

				for (TweetTableObject tweetTableObject : tweetList) {
					collector.emit(new Values(tweetTableObject));
					totalEmitedTweets++;
				}
			}
			tweetList.clear();
			if(isDebug){
				LOG.info("Sentiment140Bolt statics Processed::"+ totalProcessedTweets+
						" Emitted::"+totalEmitedTweets+" Missed::"+ (totalProcessedTweets-totalEmitedTweets));
				}
		} else {
			List<Object> otherFields = Lists.newArrayList(tuple.getValues());
			TweetTableObject tweetObject = (TweetTableObject) otherFields
					.get(0);
			tweetList.add(tweetObject);
			totalProcessedTweets++;
			
			// System.out.println(tweetObject.getJsonObject());
		}

		// } catch (Exception e) {
		// collector.reportError(e);
		// }
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declare(new Fields("tweetObjectListWithSentiment"));
	}

	void processCollectionOfTweets() {

		// **********************************
		// Task 1 :: Get List of active queries from DB
		// Task 2 :: Tag Tweets with queries (TODO: Check Multi-tagging)
		// Task 3 :: Do sentiment Analysis
		// Task 4 :: Save to respective user Database

		// **********************************

		ArrayList<Sentiment140Request> sentiment140RequestList = new ArrayList<Sentiment140Request>(
				0);
		for (TweetTableObject tweet : tweetList) {
			Sentiment140Request sentiment140Request = new Sentiment140Request();
			
			sentiment140Request.setId(tweet.getTweet_id());
			
			// Add query parameter for accurate Sentiment140 analysis 
			// Multiple query would be like >> "war OR obama OR election"
			String query_str = "";
			for (Queries queryObject : tweet.getQueries()) {
				// Step 1:: remove any # or @ from query
				String add_query = queryObject.getQuery();
				if (queryObject.getQuery().startsWith("#")) {
					add_query = queryObject.getQuery().replace("#", "");
				} else if (queryObject.getQuery().startsWith("@")) {
					add_query = queryObject.getQuery().replace("@", "");
				}
				if (query_str == "") {
					query_str = add_query;
				} else {
					query_str += query_str + " OR " + add_query;
				}
			}
			sentiment140Request.setQuery(query_str);
			
			// String text = HTMLEscapeUtil.escape(tweet.getText());
			String text = HTMLEscapeUtil.escapeTextArea(tweet.getText());
			// text = HTMLEscapeUtil.escapeSpecial(tweet.getText());

			sentiment140Request.setText(text);

			sentiment140RequestList.add(sentiment140Request);
		}

		// Sort the Tweets collection with the DBNAME
		// Collections.sort(tweetList, TweetTableObject.DBNameComparator);

		Sentiment140RequestDAO requestObject = new Sentiment140RequestDAO();
		requestObject.setData(sentiment140RequestList);
		RestTemplate restTemplate = new RestTemplate();

		HttpHeaders requestHeaders = new HttpHeaders();
		final Map<String, String> parameterMap = new HashMap<String, String>(4);
		parameterMap.put("charset", "utf-8");
		requestHeaders.setContentType(new MediaType("application", "json",
				parameterMap));

		HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);

		Sentiment140ResponseDAO response = restTemplate.postForObject(
				SENTIMENT140_URL, requestObject, Sentiment140ResponseDAO.class,
				requestEntity);

		ArrayList<Sentiment140Response> responseData = response.getData();

		int i = 0;
		for (Sentiment140Response sentiment140Response : responseData) {

			if (tweetList.get(i).getTweet_id()
							.equals(sentiment140Response.getId())) {

				// _datasource.setUrl(DB_BASE_URL +
				// tweetList.get(i).getDbName());// test-replica
				// _tweetsJDBCTemplate.setDataSource(_datasource);

				tweetList.get(i).setSentiment_original(
						Integer.valueOf(sentiment140Response.getPolarity()));

				tweetList.get(i).setSentiment(
						Integer.valueOf(sentiment140Response.getPolarity()));

				// _tweetsJDBCTemplate.insertTweet(tweetList.get(i));

			} else {
				LOG.error("After Sentiment No query_id Match Found for Tweet:: ");
			}
			// System.out.println(sentiment140Response.getPolarity());
			i++;
		}

		// /Insert Into Database
	}

}