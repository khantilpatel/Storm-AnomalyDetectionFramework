package common.feeder.utility;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import data.collection.entity.Queries;
import data.collection.entity.TweetAggregatedBin;
import data.collection.entity.TweetSentiment;
import data.collection.entity.Users;
import data.collection.json.JsonBin;
import data.collection.json.JsonDashboardSparkline;
import data.collection.json.JsonSparklineObject;

public class DatabaseHelper {

	static final int ZERO = 0;

	static final int ONE = 1;

	public static String[] getKeywordsFromDB(
			TweetJDBCTemplate tweetsJDBCTemplateMain, String DB_BASE_URL,
			String DB_NAME, int MAX_NUMBER_OF_QUERIES, Logger LOG) {

		List<Queries> listofAllQueries = new ArrayList<Queries>(0);
		List<Users> listofAllUsers = new ArrayList<Users>(0);

		LOG.info("calling ... tweetsJDBCTemplateMain.listUsers(\"Select * from users\");");
		listofAllUsers = tweetsJDBCTemplateMain
				.listUsers("Select * from users");

		TweetJDBCTemplate _tweetsJDBCTemplate = new TweetJDBCTemplate();

		DriverManagerDataSource _datasource = new DriverManagerDataSource();
		_datasource.setDriverClassName("com.mysql.jdbc.Driver");
		_datasource.setUrl(DB_BASE_URL + DB_NAME);// test-replica
		_datasource.setUsername("vistaroot");
		_datasource.setPassword("vista&mysql");

		for (Users user : listofAllUsers) {
			List<Queries> listofQueries = new ArrayList<Queries>(0);
			String db_name = user.getDatabase_name();

			_datasource.setUrl(DB_BASE_URL + db_name);// test-replica

			_tweetsJDBCTemplate.setDataSource(_datasource);

			listofQueries = _tweetsJDBCTemplate
					.listQueries("select * from queries where querystatus = 1");

			for (Queries queries : listofQueries) {
				queries.setDb_name(db_name);
			}
			listofAllQueries.addAll(listofQueries);
		}
		String[] _keywords = null;

		if (listofAllQueries.size() < MAX_NUMBER_OF_QUERIES) {
			_keywords = new String[listofAllQueries.size()];
			int index = 0;
			for (Queries query : listofAllQueries) {
				_keywords[index] = query.getQuery();
				index++;
			}
		} else {
			_keywords = new String[MAX_NUMBER_OF_QUERIES];

			for (int i = 0; i <= MAX_NUMBER_OF_QUERIES; i++) {

				_keywords[i] = listofAllQueries.get(i).getQuery();
			}
		}

		return _keywords;
	}

	public static List<Queries> getKeywordsFromDBwithDetails(
			TweetJDBCTemplate tweetsJDBCTemplateMain,
			int MAX_NUMBER_OF_QUERIES, ApplicationConfigurationFile configFile) {

		List<Queries> listofAllQueries = new ArrayList<Queries>(0);
		List<Users> listofAllUsers = new ArrayList<Users>(0);

		listofAllUsers = tweetsJDBCTemplateMain
				.listUsers("Select * from users");

		for (Users user : listofAllUsers) {
			List<Queries> listofQueries = new ArrayList<Queries>(0);
			String db_name = user.getDatabase_name();

			listofQueries = TweetJDBCTemplateConnectionPool
					.getTweetJDBCTemplate(db_name, configFile).listQueries(
							"select * from queries where querystatus = 1");

			for (Queries queries : listofQueries) {
				queries.setDb_name(db_name);
			}

			listofAllQueries.addAll(listofQueries);
		}

		List<Queries> return_listofAllQueries = new ArrayList<Queries>(0);

		if (listofAllQueries.size() < MAX_NUMBER_OF_QUERIES) {

			return_listofAllQueries.addAll(listofAllQueries);

		} else {
			// _keywords = new String[MAX_NUMBER_OF_QUERIES];

			for (int i = 0; i <= MAX_NUMBER_OF_QUERIES; i++) {

				return_listofAllQueries.add(listofAllQueries.get(i));
			}
		}

		return return_listofAllQueries;
	}

	public static List<Queries> getKeywordsFromDBwithAnomalies(
			TweetJDBCTemplate tweetsJDBCTemplateMain,
			int MAX_NUMBER_OF_QUERIES, ApplicationConfigurationFile configFile) {

		List<Queries> listofAllQueries = new ArrayList<Queries>(0);
		List<Users> listofAllUsers = new ArrayList<Users>(0);

		listofAllUsers = tweetsJDBCTemplateMain
				.listUsers("Select * from users");

		for (Users user : listofAllUsers) {
			List<Queries> listofQueries = new ArrayList<Queries>(0);
			String db_name = user.getDatabase_name();

			listofQueries = TweetJDBCTemplateConnectionPool
					.getTweetJDBCTemplate(db_name, configFile)
					.listQueries(
							"select * from queries where querystatus = 1 && anomalystatus = 1");

			for (Queries queries : listofQueries) {
				queries.setDb_name(db_name);
			}

			listofAllQueries.addAll(listofQueries);
		}

		List<Queries> return_listofAllQueries = new ArrayList<Queries>(0);

		if (listofAllQueries.size() < MAX_NUMBER_OF_QUERIES) {

			return_listofAllQueries.addAll(listofAllQueries);

		} else {
			// _keywords = new String[MAX_NUMBER_OF_QUERIES];

			for (int i = 0; i <= MAX_NUMBER_OF_QUERIES; i++) {

				return_listofAllQueries.add(listofAllQueries.get(i));
			}
		}

		return return_listofAllQueries;
	}

	public static JsonDashboardSparkline getAggregatedTweetData(
			TweetJDBCTemplate tweetsJDBCTemplateMain, String DB_BASE_URL,
			String DB_NAME, Queries query, int AGGREGATION_FACTOR_MINUTES,
			int WINDOW_PERIOD, Logger LOG) {
		int aggregate_factor_seconds_sql = AGGREGATION_FACTOR_MINUTES * 60; // *
																			// Seconds

		int aggregate_factor_milliseconds_sql = AGGREGATION_FACTOR_MINUTES * 60 * 1000; // *
																						// Seconds

		int aggregate_window_duration = WINDOW_PERIOD; // Days * hours * minutes

		long time_now = new Date().getTime();
		LOG.info("Date divided" + (time_now)
				/ (aggregate_factor_milliseconds_sql));
		Long floored_timestamp = (long) Math.floor((time_now)
				/ (aggregate_factor_milliseconds_sql));
		LOG.info("Floor Date::" + floored_timestamp);

		long query_id = query.getQuery_id();

		Date window_end_date = new Date();

		window_end_date.setTime(aggregate_factor_milliseconds_sql
				* floored_timestamp);

		Date window_start_date = AggregateUtilityFunctions.minusMinutesToDate(
				aggregate_window_duration, window_end_date);

		Long floored_end_timestamp = (long) Math.floor((window_start_date
				.getTime()) / (aggregate_factor_milliseconds_sql));
		LOG.info("Floor End Date::" + floored_end_timestamp);

		window_start_date.setTime(aggregate_factor_milliseconds_sql
				* floored_end_timestamp);

		LOG.info("**********Executing STEP 1 for query '" + query.getQuery()
				+ "' ***************");
		// SELECT CONCAT(21600*FLOOR(a.unix_timestamp/
		// 21600), '-', 21600*FLOOR(a.unix_timestamp/21600) + 21600) AS range_t,
		// COUNT(*) AS 'value',
		// a.sentiment FROM tweets a WHERE a.query_id='2' AND
		// a.unix_timestamp>='1371741012' AND
		// a.unix_timestamp<='1408000989' GROUP BY range_t,a.sentiment ORDER BY
		// range_t;

		// Step 1 :: Get the Aggregated DB query result in an array
		String sql = "SELECT CONCAT(" + aggregate_factor_seconds_sql
				+ "*FLOOR(a.unix_timestamp/ " + aggregate_factor_seconds_sql
				+ "), '-', " + aggregate_factor_seconds_sql
				+ "*FLOOR(a.unix_timestamp/" + aggregate_factor_seconds_sql
				+ ") + " + aggregate_factor_seconds_sql + ") AS range_t,"
				+ " COUNT(*) AS 'value',"
				+ " a.sentiment FROM tweets a WHERE a.query_id='" + query_id
				+ "' AND a.unix_timestamp>='" + window_start_date.getTime()
				/ 1000 + "' AND " + " a.unix_timestamp<='"
				+ window_end_date.getTime() / 1000
				+ "' GROUP BY range_t,a.sentiment " + "ORDER BY range_t;";

		LOG.info(sql);
		List<TweetAggregatedBin> bins = tweetsJDBCTemplateMain
				.listAggregatedTweets(sql);

		Date nextAggregatedDate = AggregateUtilityFunctions.addMinutesToDate(
				AGGREGATION_FACTOR_MINUTES, window_start_date);
		LOG.info("**********Done with STEP 1 for query '" + query.getQuery()
				+ "' ***************");

		LOG.info("**********Executing with STEP 2 for query '"
				+ query.getQuery() + "' ***************");
		// Step 2 :: Write the algorithm to generate the final List with
		// filling the missing interval in data in-between

		JsonDashboardSparkline dashboardSparkline = new JsonDashboardSparkline();
		int i = 0;
		for (TweetAggregatedBin tweetAggregatedBin : bins) {
			LOG.info(i + "::" + " | Sent::" + tweetAggregatedBin.getSentiment()
					+ " | Start::" + tweetAggregatedBin.getStart_date_bin()
					+ " | End::" + tweetAggregatedBin.getEnd_date_bin()
					+ " | Value::" + tweetAggregatedBin.getValue());

			i++;
		}

		int index = 0;
		while (nextAggregatedDate.compareTo(window_end_date) < 0
				&& index < bins.size()) {
			// A bin could be any sentiment
			TweetAggregatedBin bin_i = bins.get(index);
			TweetAggregatedBin bin_i_1 = null;

			if (index + 1 < bins.size()) {
				bin_i_1 = bins.get(index + 1);
			}

			TweetAggregatedBin bin_i_2 = null;

			if (index + 2 < bins.size()) {
				bin_i_2 = bins.get(index + 2);
			}

			nextAggregatedDate = bin_i.getEnd_date_bin();
			// This check only for the first actual bin
			if (index == 0
					&& (bin_i.getStart_date_bin().compareTo(window_start_date) > 0)) {

				// STEP 1 : Add the start point so that we don't miss on the
				// Window Length
				Date first_bin_date = AggregateUtilityFunctions
						.addMinutesToDate(AGGREGATION_FACTOR_MINUTES,
								window_start_date);

				JsonBin bin = new JsonBin();
				bin.setDate(first_bin_date);
				bin.setValue(ZERO); // set to ZERO

				dashboardSparkline.getLine()
						.get(TweetSentiment.POSITIVE.getSentimentCode())
						.addToData(bin);

				dashboardSparkline.getLine()
						.get(TweetSentiment.NEUTRAL.getSentimentCode())
						.addToData(bin);

				dashboardSparkline.getLine()
						.get(TweetSentiment.NEGATIVE.getSentimentCode())
						.addToData(bin);

				// STEP 2 :: Add the point before the actual start bin so that
				// to avoid a slop line from
				// start of the window to the actuall start bin.

				// get the bin before the actual start bin if it is not same as
				// window_start_date

				Date bin_before_start = AggregateUtilityFunctions
						.minusMinutesToDate(AGGREGATION_FACTOR_MINUTES,
								bin_i.getEnd_date_bin());

				if (bin_before_start.compareTo(window_start_date) != ZERO) {
					bin = new JsonBin();
					bin.setDate(bin_before_start);
					bin.setValue(ZERO); // set to ZERO

					dashboardSparkline.getLine()
							.get(TweetSentiment.POSITIVE.getSentimentCode())
							.addToData(bin);

					dashboardSparkline.getLine()
							.get(TweetSentiment.NEUTRAL.getSentimentCode())
							.addToData(bin);

					dashboardSparkline.getLine()
							.get(TweetSentiment.NEGATIVE.getSentimentCode())
							.addToData(bin);
				}

			}

			// This check is for each while loop, to make sure to add a ZERO
			// point if there is
			// a gap in the data timeline.
			if (index > ZERO
					&& (bins.get(index - ONE).getEnd_date_bin()
							.compareTo(bin_i.getStart_date_bin()) < ZERO)) {
				// STEP 1 : Add the start point so that we don't miss on the
				// Window Length

				Date bin_before_current = AggregateUtilityFunctions
						.minusMinutesToDate(AGGREGATION_FACTOR_MINUTES,
								bin_i.getEnd_date_bin());

				JsonBin bin = new JsonBin();
				bin.setDate(bin_before_current);
				bin.setValue(ZERO); // set to ZERO

				dashboardSparkline.getLine()
						.get(TweetSentiment.POSITIVE.getSentimentCode())
						.addToData(bin);

				dashboardSparkline.getLine()
						.get(TweetSentiment.NEUTRAL.getSentimentCode())
						.addToData(bin);

				dashboardSparkline.getLine()
						.get(TweetSentiment.NEGATIVE.getSentimentCode())
						.addToData(bin);
			}

			// Check for all the three sentiment and increment "i" accordingly
			if (bin_i.getSentiment() == TweetSentiment.POSITIVE
					|| bin_i.getSentiment() == TweetSentiment.NEUTRAL
					|| bin_i.getSentiment() == TweetSentiment.NEGATIVE) {

				JsonBin bin = new JsonBin();
				bin.setDate(bin_i.getEnd_date_bin());
				bin.setValue(bin_i.getValue());

				dashboardSparkline.getLine()
						.get(bin_i.getSentiment().getSentimentCode())
						.addToData(bin);
				index++;
			}

			if (bin_i_1 != null
					&& bin_i_1.getStart_date_bin().equals(
							bin_i.getStart_date_bin())) {
				if (bin_i_1.getSentiment() == TweetSentiment.POSITIVE
						|| bin_i_1.getSentiment() == TweetSentiment.NEUTRAL
						|| bin_i_1.getSentiment() == TweetSentiment.NEGATIVE) {

					JsonBin bin = new JsonBin();
					bin.setDate(bin_i_1.getEnd_date_bin());
					bin.setValue(bin_i_1.getValue());

					dashboardSparkline.getLine()
							.get(bin_i_1.getSentiment().getSentimentCode())
							.addToData(bin);
					index++;
				}

				// If the bin_i_1 is true then only we need to check for bin_i_2
				if (bin_i_2 != null
						&& bin_i_2.getStart_date_bin().equals(
								bin_i.getStart_date_bin())) {
					if (bin_i_2.getSentiment() == TweetSentiment.POSITIVE
							|| bin_i_2.getSentiment() == TweetSentiment.NEUTRAL
							|| bin_i_2.getSentiment() == TweetSentiment.NEGATIVE) {

						JsonBin bin = new JsonBin();
						bin.setDate(bin_i_2.getEnd_date_bin());
						bin.setValue(bin_i_2.getValue());

						dashboardSparkline.getLine()
								.get(bin_i_2.getSentiment().getSentimentCode())
								.addToData(bin);
						index++;
					}
				} else {
					if (bin_i.getSentiment() != TweetSentiment.POSITIVE
							&& bin_i_1.getSentiment() != TweetSentiment.POSITIVE) {
						JsonBin bin = new JsonBin();
						bin.setDate(bin_i.getEnd_date_bin());
						bin.setValue(ZERO); // set to ZERO

						dashboardSparkline
								.getLine()
								.get(TweetSentiment.POSITIVE.getSentimentCode())
								.addToData(bin);
					}

					// SECOND check for Neutral
					if (bin_i.getSentiment() != TweetSentiment.NEUTRAL
							&& bin_i_1.getSentiment() != TweetSentiment.NEUTRAL) {
						JsonBin bin = new JsonBin();
						bin.setDate(bin_i.getEnd_date_bin());
						bin.setValue(ZERO); // set to ZERO

						dashboardSparkline.getLine()
								.get(TweetSentiment.NEUTRAL.getSentimentCode())
								.addToData(bin);
					}

					// THIRD check for Negative
					if (bin_i.getSentiment() != TweetSentiment.NEGATIVE
							&& bin_i_1.getSentiment() != TweetSentiment.NEGATIVE) {
						JsonBin bin = new JsonBin();
						bin.setDate(bin_i.getEnd_date_bin());
						bin.setValue(ZERO); // set to ZERO

						dashboardSparkline
								.getLine()
								.get(TweetSentiment.NEGATIVE.getSentimentCode())
								.addToData(bin);
					}
				}
			} else {
				if (bin_i.getSentiment() != TweetSentiment.POSITIVE) {
					JsonBin bin = new JsonBin();
					bin.setDate(bin_i.getEnd_date_bin());
					bin.setValue(ZERO); // set to ZERO

					dashboardSparkline.getLine()
							.get(TweetSentiment.POSITIVE.getSentimentCode())
							.addToData(bin);
				}

				if (bin_i.getSentiment() != TweetSentiment.NEUTRAL) {
					JsonBin bin = new JsonBin();
					bin.setDate(bin_i.getEnd_date_bin());
					bin.setValue(ZERO); // set to ZERO

					dashboardSparkline.getLine()
							.get(TweetSentiment.NEUTRAL.getSentimentCode())
							.addToData(bin);
				}

				if (bin_i.getSentiment() != TweetSentiment.NEGATIVE) {
					JsonBin bin = new JsonBin();
					bin.setDate(bin_i.getEnd_date_bin());
					bin.setValue(ZERO); // set to ZERO

					dashboardSparkline.getLine()
							.get(TweetSentiment.NEGATIVE.getSentimentCode())
							.addToData(bin);
				}

			}

			Date start_bin_after_current = AggregateUtilityFunctions
					.addMinutesToDate(AGGREGATION_FACTOR_MINUTES,
							bin_i.getEnd_date_bin());

			// This is to check if there is no actual bin after current one,
			// then add a ZERO bin. This has a bug that if in the next
			// while-loop
			// the the ZERO bin will be added again, but thats ok it wont break
			// anything
			if (index < bins.size()
					&& (start_bin_after_current.compareTo(bins.get(index)
							.getEnd_date_bin()) != ZERO)) {
				JsonBin bin = new JsonBin();
				bin.setDate(start_bin_after_current);
				bin.setValue(ZERO); // set to ZERO

				dashboardSparkline.getLine()
						.get(TweetSentiment.POSITIVE.getSentimentCode())
						.addToData(bin);

				dashboardSparkline.getLine()
						.get(TweetSentiment.NEUTRAL.getSentimentCode())
						.addToData(bin);

				dashboardSparkline.getLine()
						.get(TweetSentiment.NEGATIVE.getSentimentCode())
						.addToData(bin);
			}

		}
		// for (TweetAggregatedBin tweetAggregatedBin : bins) {
		//
		// while (nextAggregatedDate.compareTo(tweetAggregatedBin
		// .getEnd_date_bin()) < 0) {
		// // no data found add empty data been
		//
		// JsonBin bin = new JsonBin();
		// bin.setDate(nextAggregatedDate);
		// bin.setValue(0);
		//
		// dashboardSparkline.getLine().get(0).addToData(bin);
		// dashboardSparkline.getLine().get(2).addToData(bin);
		// dashboardSparkline.getLine().get(4).addToData(bin);
		//
		// nextAggregatedDate = AggregateUtilityFunctions
		// .addMinutesToDate(AGGREGATION_FACTOR_MINUTES,
		// nextAggregatedDate);
		//
		// }
		//
		// if (nextAggregatedDate.compareTo(tweetAggregatedBin
		// .getEnd_date_bin()) == 0) {
		//
		// JsonBin bin = new JsonBin();
		// bin.setDate(tweetAggregatedBin.getEnd_date_bin());
		// bin.setValue(tweetAggregatedBin.getValue());
		//
		// if (tweetAggregatedBin.getSentiment() == TweetSentiment.POSITIVE) {
		// dashboardSparkline.getLine().get(4).addToData(bin);
		// isPositiveSet = true;
		// } else if (tweetAggregatedBin.getSentiment() ==
		// TweetSentiment.NEUTRAL) {
		// dashboardSparkline.getLine().get(2).addToData(bin);
		// isNeutralSet = true;
		// } else if (tweetAggregatedBin.getSentiment() ==
		// TweetSentiment.NEGATIVE) {
		// dashboardSparkline.getLine().get(0).addToData(bin);
		// isNegativeSet = true;
		// }
		// }
		//
		// if (isPositiveSet && isNeutralSet && isNegativeSet) {
		// nextAggregatedDate = AggregateUtilityFunctions
		// .addMinutesToDate(AGGREGATION_FACTOR_MINUTES,
		// nextAggregatedDate);
		//
		// isPositiveSet = false;
		// isNeutralSet = false;
		// isNegativeSet = false;
		// }
		// }
		//
		// while (nextAggregatedDate.compareTo(window_end_date) < 0) {
		// JsonBin bin = new JsonBin();
		// bin.setDate(nextAggregatedDate);
		// bin.setValue(0);
		//
		// dashboardSparkline.getLine().get(0).addToData(bin);
		// dashboardSparkline.getLine().get(2).addToData(bin);
		// dashboardSparkline.getLine().get(4).addToData(bin);
		//
		// nextAggregatedDate = AggregateUtilityFunctions.addMinutesToDate(
		// AGGREGATION_FACTOR_MINUTES, nextAggregatedDate);
		// }

		// Step 3 :: Return a final array.
		JsonSparklineObject object_0 = dashboardSparkline.getLine().get(0);
		LOG.info("*******Printing Info for Sentement 0*******");
		for (JsonBin bin : object_0.getData()) {
			LOG.info("Date:" + bin.getDate() + "Value:" + bin.getValue());
		}

		JsonSparklineObject object_2 = dashboardSparkline.getLine().get(2);
		LOG.info("*******Printing Info for Sentement 2*******");
		for (JsonBin bin : object_2.getData()) {
			LOG.info("Date:" + bin.getDate() + "Value:" + bin.getValue());
		}

		JsonSparklineObject object_4 = dashboardSparkline.getLine().get(4);
		LOG.info("*******Printing Info for Sentement 4*******");
		for (JsonBin bin : object_4.getData()) {
			LOG.info("Date:" + bin.getDate() + "Value:" + bin.getValue());
		}

		LOG.info("Final Size::");

		LOG.info("**********Done with STEP 2 for query '" + query.getQuery()
				+ "' ***************");
		return dashboardSparkline;
	}
}
