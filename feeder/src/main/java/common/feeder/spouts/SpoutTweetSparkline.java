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
public class SpoutTweetSparkline extends BaseRichSpout {

	SpoutOutputCollector _collector;
	LinkedBlockingQueue<RawTweet> queue = null;

	long queryRefreshInterval ; // 5min //900000; //15 minute
	int MAX_NUMBER_OF_QUERIES = 200;

	// ************Database related instances
	String DB_BASE_URL;
	String USERNAME;
	String PASSWORD;
	String DB_NAME = "meta_db";

	List<Queries> queriesList;

	public static Logger LOG = LoggerFactory
			.getLogger(SpoutTweetSparkline.class);

	ApplicationConfigurationFile configFile;
	
	boolean isDebug = false;

	public SpoutTweetSparkline(ApplicationConfigurationFile _configFile, boolean _isDebug) {

		// this.keyWords = keyWords;

		configFile = _configFile;

		isDebug = _isDebug;
		
		queryRefreshInterval= configFile.getSparkline_refresh_interval();

	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		LOG.info("In Open()::");
		queue = new LinkedBlockingQueue<RawTweet>(1000);
		_collector = collector;

		LOG.info("**********initialize TwitterStreamFactory::");

	}

	@Override
	public void nextTuple() {

		// Update the keyword list////////////////////
		queriesList = DatabaseHelper.getKeywordsFromDBwithDetails(
				TweetJDBCTemplateConnectionPool.getTweetJDBCTemplate(DB_NAME, configFile),
				MAX_NUMBER_OF_QUERIES,configFile);
		int index = 0;

		LOG.info("**************QueryList::***************");
		for (Queries query : queriesList) {
			index++;
			LOG.info("<" + index + "> query:::" + query.getQuery());
			_collector.emit(new Values(query));
		}
		
		Utils.sleep(queryRefreshInterval);

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		// ret.setMaxTaskParallelism(1);
		// ret.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 30);
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
		declarer.declare(new Fields("queryObject"));
	}

}
