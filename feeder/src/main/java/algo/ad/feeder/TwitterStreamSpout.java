package algo.ad.feeder;

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

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.springframework.jdbc.datasource.DriverManagerDataSource;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import algo.ad.feeder.dao.Queries;
import algo.ad.feeder.dao.TweetJDBCTemplate;
import algo.ad.feeder.utility.DatabaseHelper;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

@SuppressWarnings("serial")
public class TwitterStreamSpout extends BaseRichSpout {

	SpoutOutputCollector _collector;
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream _twitterStream;
	String consumerKey;
	String consumerSecret;
	String accessToken;
	String accessTokenSecret;
	String[] keyWords;
	String[] keyWords1;
	long startTime = 0;
	long keywordRefreshInterval = 300000; // 5min //900000; //15 minute
	int MAX_NUMBER_OF_QUERIES = 200;
	// ************Database related instances
	String DB_BASE_URL = "jdbc:mysql://localhost:3307/";
	String DB_NAME = "meta_db";
	public static DriverManagerDataSource datasource;
	public static TweetJDBCTemplate tweetsJDBCTemplate;
	List<Queries> queriesList;

	public TwitterStreamSpout(String consumerKey, String consumerSecret,
			String accessToken, String accessTokenSecret, String[] keyWords) {
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
		this.keyWords = keyWords;
		String[] keyWords1 = { "#ThingsMyDoctorSays" };
		this.keyWords1 = keyWords1;

		// ********DB related stuff

		datasource = new DriverManagerDataSource();
		datasource.setDriverClassName("com.mysql.jdbc.Driver");
		datasource.setUrl(DB_BASE_URL + DB_NAME);// test-replica
		datasource.setUsername("vistaroot");
		datasource.setPassword("vista&mysql");

		tweetsJDBCTemplate = new TweetJDBCTemplate();
		tweetsJDBCTemplate.setDataSource(datasource);
	}

	public TwitterStreamSpout() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		_collector = collector;
		startTime = System.currentTimeMillis();
		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
				long elapsedTime = System.currentTimeMillis() - startTime;
				//System.out.println("*************elapsedTime:" + elapsedTime);
				if (elapsedTime > keywordRefreshInterval) {

				///	System.out.println("*************Changing Query:");

					// Update the keyword list////////////////////
					String[] temp_keywords = DatabaseHelper.getKeywordsFromDB(
							tweetsJDBCTemplate, DB_BASE_URL, DB_NAME,
							MAX_NUMBER_OF_QUERIES);
					if (!keyWords.equals(temp_keywords)) {
						keyWords = temp_keywords;
						FilterQuery query = new FilterQuery().track(keyWords);
						_twitterStream.filter(query);
					}
					// /////////////////////////////////////
					startTime = System.currentTimeMillis();
				}
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onException(Exception ex) {
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub

			}

		};

		_twitterStream = new TwitterStreamFactory(new ConfigurationBuilder()
				.setJSONStoreEnabled(true).build()).getInstance();

		_twitterStream.addListener(listener);
		_twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		AccessToken token = new AccessToken(accessToken, accessTokenSecret);
		_twitterStream.setOAuthAccessToken(token);

		keyWords = DatabaseHelper.getKeywordsFromDB(tweetsJDBCTemplate,
				DB_BASE_URL, DB_NAME, MAX_NUMBER_OF_QUERIES);

		if (keyWords.length == 0) {

			_twitterStream.sample();
		}

		else {

			FilterQuery query = new FilterQuery().track(keyWords);

			_twitterStream.filter(query);
		}

	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
		} else {
			_collector.emit(new Values(ret));

		}
	}

	@Override
	public void close() {
		_twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		ret.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 30);
		return ret;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
