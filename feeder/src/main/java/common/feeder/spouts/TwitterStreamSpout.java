package common.feeder.spouts;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import common.feeder.utility.ApplicationConfigurationFile;
import common.feeder.utility.DatabaseHelper;
import common.feeder.utility.TweetJDBCTemplateConnectionPool;

import data.collection.entity.Queries;
import data.collection.entity.RawTweet;

@SuppressWarnings("serial")
public class TwitterStreamSpout extends BaseRichSpout {

	SpoutOutputCollector _collector;
	LinkedBlockingQueue<RawTweet> queue = null;
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
	ApplicationConfigurationFile configFile;
	// ************Database related instances

	String DB_NAME_META = "meta_db";
	List<Queries> queriesList;

	public static Logger LOG = LoggerFactory
			.getLogger(TwitterStreamSpout.class);

	public TwitterStreamSpout(ApplicationConfigurationFile _configFile) {
		configFile = _configFile;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		LOG.info("In Open()::");
		queue = new LinkedBlockingQueue<RawTweet>(1000);
		_collector = collector;
		startTime = System.currentTimeMillis();
		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
				// LOG.info("executed onStatus()::");
				RawTweet rawTweet = new RawTweet();

				String strJson = DataObjectFactory.getRawJSON(status);
				rawTweet.setStatus(status);
				rawTweet.setJsonObjectString(strJson);
				// System.out.println(strJson);

				queue.offer(rawTweet);
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
		LOG.info("**********initialize TwitterStreamFactory::");
		_twitterStream = new TwitterStreamFactory(new ConfigurationBuilder()
				.setJSONStoreEnabled(true).build()).getInstance();

		_twitterStream.addListener(listener);

		LOG.info("consumerKey::" + configFile.getTwitter_consumerKey());
		LOG.info("consumerSecret::" + configFile.getTwitter_consumerSecret());
		LOG.info("accessToken::" + configFile.getTwitter_accessToken());
		LOG.info("accessTokenSecret::"
				+ configFile.getTwitter_accessTokenSecret());

		_twitterStream.setOAuthConsumer(configFile.getTwitter_consumerKey(),
				configFile.getTwitter_consumerSecret());
		AccessToken token = new AccessToken(
				configFile.getTwitter_accessToken(),
				configFile.getTwitter_accessTokenSecret());
		_twitterStream.setOAuthAccessToken(token);

		// Update the keyword list////////////////////
		queriesList = DatabaseHelper.getKeywordsFromDBwithDetails(
				TweetJDBCTemplateConnectionPool
						.getTweetJDBCTemplate(DB_NAME_META,configFile),
				MAX_NUMBER_OF_QUERIES, configFile);
		int index = 0;
		keyWords = new String[queriesList.size()];
		LOG.info("**************QueryList::***************");
		for (Queries query : queriesList) {
			keyWords[index] = query.getQuery();
			index++;
			LOG.info("<" + index + "> query:::" + query.getQuery());
		}

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
		RawTweet rawTweet = queue.poll();
		boolean isTimeTosendQueriesList = false;
		long elapsedTime = System.currentTimeMillis() - startTime;
		// System.out.println("*************elapsedTime:" +
		// elapsedTime);
		// LOG.info("*************elapsedTime:" + elapsedTime);
		if (elapsedTime > keywordRefreshInterval) {
			LOG.info("***************************TICKKKKKKKKKK");
			// / System.out.println("*************Changing Query:");

			// Update the keyword list////////////////////
			String[] temp_keywords = null;

			queriesList = DatabaseHelper.getKeywordsFromDBwithDetails(
					TweetJDBCTemplateConnectionPool
							.getTweetJDBCTemplate(DB_NAME_META, configFile),
					MAX_NUMBER_OF_QUERIES, configFile);
			int index = 0;
			temp_keywords = new String[queriesList.size()];
			LOG.info("**************QueryList::***************");
			for (Queries query : queriesList) {
				temp_keywords[index] = query.getQuery();
				index++;
				LOG.info("<" + index + "> query:::" + query.getQuery());
			}

			if (!keyWords.equals(temp_keywords)) {
				keyWords = temp_keywords;
				FilterQuery query = new FilterQuery().track(keyWords);
				_twitterStream.filter(query);
			}
			// /////////////////////////////////////
			startTime = System.currentTimeMillis();
			isTimeTosendQueriesList = true;
		}

		if (rawTweet == null) {
			Utils.sleep(50);
		} else {
			if (isTimeTosendQueriesList)
				_collector.emit(new Values(rawTweet, queriesList));
			else
				_collector.emit(new Values(rawTweet, queriesList));
		}
	}

	@Override
	public void close() {
		_twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		// ret.setMaxTaskParallelism(1);
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
		declarer.declare(new Fields("tweet", "querylist"));
	}

}
