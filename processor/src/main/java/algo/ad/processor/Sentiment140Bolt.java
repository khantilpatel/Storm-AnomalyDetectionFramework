package algo.ad.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.web.client.RestTemplate;

import twitter4j.Status;
import algo.ad.dao.Sentiment140Request;
import algo.ad.dao.Sentiment140RequestDAO;
import algo.ad.dao.Sentiment140Response;
import algo.ad.dao.Sentiment140ResponseDAO;
import algo.ad.feeder.dao.Queries;
import algo.ad.feeder.dao.TweetJDBCTemplate;
import algo.ad.feeder.dao.TweetTableObject;
import algo.ad.feeder.utility.DatabaseHelper;
import algo.ad.utility.HTMLEscapeUtil;
import algo.ad.utility.StatusToTweetTableTransform;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.Lists;

public class Sentiment140Bolt extends BaseBasicBolt {

	private static final long serialVersionUID = 6460656994454296875L;
	String DB_BASE_URL = "jdbc:mysql://localhost:3307/";
	String DB_NAME = "meta_db";
	int MAX_NUMBER_OF_QUERIES = 200;
	long keywordRefreshInterval = 60000;// 60 sec //300000; // 5min //900000; //15 minute
	ArrayList<Status> statusList;
	long startTime = 0;
	
	public Sentiment140Bolt() {
		super();
		// TODO Auto-generated constructor stub
		startTime = System.currentTimeMillis();
		statusList = new ArrayList<Status>();
	}

	/**
	 * REST API call to sentiment 140
	 */

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		try {
			//if (TupleHelpers.isTickTuple(tuple)) {
			long elapsedTime = System.currentTimeMillis() - startTime;
			System.out.println("*************elapsedTime:" + elapsedTime);
			if (elapsedTime > keywordRefreshInterval) {
				 startTime =  System.currentTimeMillis();
				System.out.println("***************************TICKKKKKKKKKK");
				 processCollectionOfTweets();
				
				
			} else {
				List<Object> otherFields = Lists
						.newArrayList(tuple.getValues());
				Status tweetObject = (Status) otherFields.get(0);
				statusList.add(tweetObject);
				System.out.println(tweetObject.getText());
			}

			// do your bolt stuff
		} catch (Exception e) {
			collector.reportError(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
	}
	
	
	void processCollectionOfTweets()
	{
		ArrayList<TweetTableObject> tweetList = StatusToTweetTableTransform.transformStatusToTweet(statusList);
		statusList.clear();
		
		TweetJDBCTemplate tweetsJDBCTemplate = new TweetJDBCTemplate();

		DriverManagerDataSource _datasource = new DriverManagerDataSource();
		_datasource.setDriverClassName("com.mysql.jdbc.Driver");
		_datasource.setUrl(DB_BASE_URL + DB_NAME);// test-replica
		_datasource.setUsername("vistaroot");
		_datasource.setPassword("vista&mysql");
		tweetsJDBCTemplate.setDataSource(_datasource);
		
		 List<Queries> queries = DatabaseHelper.getKeywordsFromDBwithDetails(tweetsJDBCTemplate, DB_BASE_URL, DB_NAME, MAX_NUMBER_OF_QUERIES);
		
		 ArrayList<Sentiment140Request> sentiment140RequestList = new ArrayList<Sentiment140Request>(0);
		for (TweetTableObject tweet : tweetList) {
			
			for (Queries query : queries) {
				String Matcher = "(?i).*" + query.getQuery() + ".*";
				
				if(tweet.getText().matches(Matcher))
				{
					tweet.setQuery_id(query.getQuery_id());
					tweet.setDbName(query.getDb_name());
					tweet.setQuery(query.getQuery());
				}
			
			}
			
			Sentiment140Request sentiment140Request = new Sentiment140Request();
			sentiment140Request.setId(tweet.getTweet_id());
			sentiment140Request.setQuery(tweet.getQuery());
			//String text = HTMLEscapeUtil.escape(tweet.getText());
			String text = HTMLEscapeUtil.escapeTextArea(tweet.getText());
		//	text = HTMLEscapeUtil.escapeSpecial(tweet.getText());
			
			sentiment140Request.setText(text);
			
			sentiment140RequestList.add(sentiment140Request);
		}
		
		Sentiment140RequestDAO requestObject = new Sentiment140RequestDAO();
		requestObject.setData(sentiment140RequestList);
		RestTemplate restTemplate = new RestTemplate();
		
		HttpHeaders requestHeaders = new HttpHeaders();
		final Map<String, String> parameterMap = new HashMap<String, String>(4);
		parameterMap.put("charset", "utf-8");
		requestHeaders.setContentType(
		    new MediaType("application","json", parameterMap));
		
		HttpEntity<?> requestEntity = new HttpEntity<Object>(requestHeaders);
		
		Sentiment140ResponseDAO response = restTemplate
				.postForObject(
						"http://www.sentiment140.com/api/bulkClassifyJson?appid=usra2013twitter@gmail.com",
						requestObject, Sentiment140ResponseDAO.class, requestEntity);
		
		ArrayList<Sentiment140Response> responseData = response.getData();
		
		
		for (Sentiment140Response sentiment140Response : responseData) {
			System.out.println(sentiment140Response.getPolarity());
		}
		
		///Insert Into Database
	}

}