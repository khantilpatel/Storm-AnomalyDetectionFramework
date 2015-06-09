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
package common.feeder.spouts;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import common.feeder.utility.AggregateUtilityFunctions;
import common.feeder.utility.ApplicationConfigurationFile;
import common.feeder.utility.DatabaseHelper;
import common.feeder.utility.TweetJDBCTemplate;
import common.feeder.utility.TweetJDBCTemplateConnectionPool;

import data.collection.entity.Queries;
import data.collection.entity.QueriesForEmitter;
import data.collection.entity.Tweet;
import data.collection.entity.TweetTableObject;
import data.collection.json.JsonDashboardSparkline;

public class MultipleQueriesTweetsEmitterSpout extends BaseRichSpout {

	/**
	 * ************Bunch of variables to declare*************************
	 */
	// ********Constants*************************
	final static int SENTIMENT_LOG = 2;
	private static final int AGGREGATION_FACTOR_MINUTES = 15;// 360; //15
	final long SLEEP_FACTOR_MILLI_SEC = (long) 0.5; // 1 sec = 1000 msec
	// *******************************

	private static final long serialVersionUID = 7616822882515961452L;
	// public static Logger LOG = LoggerFactory
	// .getLogger(ArtificialTweetsEmitterSpout.class);
	boolean _isDistributed;
	SpoutOutputCollector _collector;
	public static DriverManagerDataSource datasource;
	
	List<Tweet> tweets;
	ApplicationConfigurationFile configFile;

	public static Logger LOG = LoggerFactory
			.getLogger(MultipleQueriesTweetsEmitterSpout.class);

	List<JsonDashboardSparkline> listOfQueryData = new ArrayList<JsonDashboardSparkline>(
			0);

	Map<String, Date> mapList = new HashMap<String, Date>(0);

	/**
	 * *****************************************************************
	 */

	/**
	 * ***********Storm Functions**************************************
	 */
	public MultipleQueriesTweetsEmitterSpout(boolean isDistributed,
			ApplicationConfigurationFile _configFile) {
		_isDistributed = isDistributed;

		configFile = _configFile;

		listOfQueryData = initQueryData();

	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
	}

	public void close() {

	}

	public void nextTuple() {

		Random randomizer = new Random();

		int rand = randomizer.nextInt(listOfQueryData.size());

		JsonDashboardSparkline jsonDashboardSparkline = listOfQueryData
				.get(rand);

	}

	public void ack(Object msgId) {

	}

	public void fail(Object msgId) {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_id", "sentiment_id", "date_obj",
				"aggregate_date_obj", "tupleType")); // TupleType: 0.Tick,
														// 1.Regular
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		if (!_isDistributed) {
			Map<String, Object> ret = new HashMap<String, Object>();
			ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
			return ret;
		} else {
			return null;
		}
	}

	/**
	 ******************************************************************************
	 */

	/**
	 *********************** user defined functions********************************
	 */

	List<JsonDashboardSparkline> initQueryData() {

		List<JsonDashboardSparkline> listOfQueries = new ArrayList<JsonDashboardSparkline>(
				0);

		List<Queries> queries = new ArrayList<Queries>();

		// *******************************************************************************
		Queries query = new Queries();

		query.setDb_name("test");
		query.setQuery("#tdf");
		query.setQuery_id(2);

		queries.add(query);

		// *******************************************************************************
		 query = new Queries();
		
		 query.setDb_name("db_khantilpatel");
		 query.setQuery("#FinalFour");
		 query.setQuery_id(9);
		
		 queries.add(query);
		// *******************************************************************************
		 query = new Queries();
		
		 query.setDb_name("db_khantilpatel");
		 query.setQuery("#WhyImNotVotingForHillary");
		 query.setQuery_id(33);
		
		 queries.add(query);
		// *****************************************************************************
		for (Queries query1 : queries) {

			TweetJDBCTemplate tweetsJDBCTemplate =TweetJDBCTemplateConnectionPool
					.getTweetJDBCTemplate(query.getDb_name(), configFile); 
			
			JsonDashboardSparkline jsonSparklineObject = DatabaseHelper
					.getAggregatedTweetData(tweetsJDBCTemplate, query1,
							configFile.getSparkline_aggregation_period(),
							configFile.getSparkline_window_period(), LOG);
			
			listOfQueries.add(jsonSparklineObject);

		}

		return listOfQueries;

	}

	
	/**
	 *******************************************************************************
	 */

}