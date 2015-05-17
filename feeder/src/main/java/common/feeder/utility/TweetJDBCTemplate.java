package common.feeder.utility;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import twitter4j.Status;
import twitter4j.User;
import data.collection.entity.AnomalyTableObject;
import data.collection.entity.ITweetDAO;
import data.collection.entity.Queries;
import data.collection.entity.Tweet;
import data.collection.entity.TweetAggregatedBin;
import data.collection.entity.TweetSentimentCountEntity;
import data.collection.entity.TweetTableObject;
import data.collection.entity.Users;
import data.collection.mapper.QueriesMapper;
import data.collection.mapper.TweetAggregateBinMapper;
import data.collection.mapper.TweetCounterQueryJSONMapper;
import data.collection.mapper.TweetMapper;
import data.collection.mapper.UsersMapper;

public class TweetJDBCTemplate implements ITweetDAO, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6412304006074403221L;

	public StreamingResultSetEnabledJdbcTemplate jdbcTemplateObject;

	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplateObject = new StreamingResultSetEnabledJdbcTemplate(
				dataSource);
		this.jdbcTemplateObject.setFetchSize(Integer.MAX_VALUE);
	}

	// public void create(String name, Integer age) {
	// String SQL = "insert into Tweet (name, age) values (?, ?)";
	//
	// jdbcTemplateObject.update( SQL, name, age);
	// System.out.println("Created Record Name = " + name + " Age = " + age);
	// return;
	// }

	// ******************************************************************************************
	// *****************LIST/GET
	// Functions**************************************************
	// ******************************************************************************************

	public Tweet getTweet(Integer id) {
		String SQL = "select * from tweets where id = ?";
		Tweet Tweet = jdbcTemplateObject.queryForObject(SQL,
				new Object[] { id }, new TweetMapper());
		return Tweet;
	}

	public List<Tweet> listTweets(String SQL) {
		List<Tweet> Tweets = jdbcTemplateObject.query(SQL, new TweetMapper());

		return Tweets;
	}

	public Queries getQuery(Integer id) {
		String SQL = "select * from queries where query_id = ?";
		Queries queries = jdbcTemplateObject.queryForObject(SQL,
				new Object[] { id }, new QueriesMapper());
		return queries;
	}

	public List<Queries> listQueries(String SQL) {
		List<Queries> queriesList = jdbcTemplateObject.query(SQL,
				new QueriesMapper());

		return queriesList;
	}

	public List<Users> listUsers(String SQL) {
		List<Users> usersList = jdbcTemplateObject
				.query(SQL, new UsersMapper());
		return usersList;
	}

	public List<TweetSentimentCountEntity> listTweetCountBySentiment(String SQL) {
		List<TweetSentimentCountEntity> usersList = jdbcTemplateObject.query(
				SQL, new TweetCounterQueryJSONMapper());
		return usersList;
	}

	public List<TweetAggregatedBin> listAggregatedTweets(String SQL) {
		List<TweetAggregatedBin> tweetAggregateResult = jdbcTemplateObject
				.query(SQL, new TweetAggregateBinMapper());

		return tweetAggregateResult;
	}

	// *************************END*****************************************************************

	// ******************************************************************************************
	// *****************INSERT/UPDATE
	// Functions**************************************************
	// ******************************************************************************************

	public void insertAnomalies(AnomalyTableObject anomaly) {

		String sql = "INSERT INTO tweets "
				+ "(query_id, sentiment_id, tweet_id, timestamp,"
				+ " aggregation, window_length, value, note ) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		// jdbcTemplate = new JdbcTemplate(dataSource);

		jdbcTemplateObject.update(
				sql,
				new Object[] { anomaly.getQuery_id(), anomaly.getSentiment(),
						anomaly.getTweet_id(), anomaly.getTimestamp(),
						anomaly.getAggregation(), anomaly.getWindow_length(),
						anomaly.getValue(), anomaly.getNote() });
	}

	public void insertTweet(TweetTableObject tweet, final long query_id) {

		String sql = "INSERT INTO tweets "
				+ "(query_id, tweet_id, user_name, text, year, month, day, hour, min,"
				+ "sec, unix_timestamp, json_object, user_profile_image_url,"
				+ " sentiment, sentiment_original, created_at) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		// jdbcTemplate = new JdbcTemplate(dataSource);

		jdbcTemplateObject.update(
				sql,
				new Object[] { query_id, tweet.getTweet_id(),
						tweet.getUser_name(), tweet.getText(), tweet.getYear(),
						tweet.getMonth(), tweet.getDay(), tweet.getHour(),
						tweet.getMin(), tweet.getSec(),
						tweet.getUnix_timestamp(), tweet.getJsonObject(),
						tweet.getUser_profile_image_url(),
						tweet.getSentiment(), tweet.getSentiment_original(),
						tweet.getCreated_at() });
	}

	public void updateQueryForSparklineJSON(Queries query, String counterJson,
			String json) {

		String sql = "UPDATE queries SET config = ?, history_data = ? WHERE query_id = ?";

		jdbcTemplateObject.update(sql,
				new Object[] { counterJson, json, query.getQuery_id() });
	}

	public void insertBulkTweet(final List<TweetTableObject> tweets) {

		String sql = "INSERT INTO tweets "
				+ "(query_id, tweet_id, user_name, text, year, month, day, hour, min,"
				+ "sec, unix_timestamp, json_object, user_profile_image_url,"
				+ " sentiment, sentiment_original, created_at) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		jdbcTemplateObject.batchUpdate(sql, new BatchPreparedStatementSetter() {

			@Override
			public void setValues(PreparedStatement ps, int i)
					throws SQLException {
				TweetTableObject tweet = tweets.get(i);
				ps.setLong(1, tweet.getQuery_id_forinsert());
				ps.setString(2, tweet.getTweet_id());
				ps.setString(3, tweet.getUser_name());
				ps.setString(4, tweet.getText());
				ps.setInt(5, tweet.getYear());
				ps.setInt(6, tweet.getMonth());
				ps.setInt(7, tweet.getDay());
				ps.setInt(8, tweet.getHour());
				ps.setInt(9, tweet.getMin());
				ps.setInt(10, tweet.getSec());
				ps.setLong(11, tweet.getUnix_timestamp());
				ps.setString(12, tweet.getJsonObject());
				ps.setString(13, tweet.getUser_profile_image_url());
				ps.setInt(14, tweet.getSentiment());
				ps.setInt(15, tweet.getSentiment_original());
				ps.setObject(16, new java.sql.Date(tweet.getCreated_at()
						.getTime()));
			}

			@Override
			public int getBatchSize() {
				return tweets.size();
			}
		});

	}

	public void insertBulkUserDetails(final List<TweetTableObject> tweets) {

		String sql = "INSERT INTO TwitterUsers "
				+ "(user_id, user_name, name, profile_image_url, location, url, description, "
				+ "created_at, followers_count, "
				+ "friends_count, favourites_count, statuses_count, time_zone, "
				+ "last_update, reputation_score, activity_score) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
				+ "ON DUPLICATE KEY UPDATE friends_count= VALUES(friends_count),"
				+ "favourites_count= VALUES(favourites_count),"
				+ "statuses_count= VALUES(statuses_count),"
				+ "followers_count= VALUES(followers_count),"
				+ "reputation_score = VALUES(reputation_score)";

		jdbcTemplateObject.batchUpdate(sql, new BatchPreparedStatementSetter() {

			@Override
			public void setValues(PreparedStatement ps, int i)
					throws SQLException {
				TweetTableObject tweet = tweets.get(i);
				Status status = tweet.getStatus();
				double reputationScore = 0;
				User user = status.getUser();

				if (user.getFollowersCount() != 0) {
					reputationScore = (double) user.getFriendsCount()
							/ (double) user.getFollowersCount();
					DecimalFormat df = new DecimalFormat("##.#####");
					reputationScore = Double.valueOf(df.format(reputationScore));
				}
				// Log.info("reputationScore::::"+user.getFriendsCount()+"/"+user.getFollowersCount()+"="+reputationScore);
				ps.setLong(1, user.getId());
				ps.setString(2, "@" + user.getScreenName());
				ps.setString(3, tweet.getUser_name());
				ps.setString(4, user.getProfileImageURL());
				ps.setString(5, user.getLocation());
				ps.setString(6, user.getURL());
				ps.setString(7, user.getDescription());
				ps.setDate(8, new java.sql.Date(user.getCreatedAt().getTime()));
				ps.setInt(9, user.getFollowersCount());
				ps.setInt(10, user.getFriendsCount());
				ps.setInt(11, user.getFavouritesCount());
				ps.setInt(12, user.getStatusesCount());
				ps.setString(13, user.getTimeZone());
				ps.setDate(14, new java.sql.Date(tweet.getCreated_at()
						.getTime()));
				ps.setObject(15, reputationScore, java.sql.Types.DECIMAL);
				ps.setDouble(16, 0);
			}

			@Override
			public int getBatchSize() {
				return tweets.size();
			}
		});

	}

	public void insertBulkTweetGeoLocationFeature(List<TweetTableObject> tweets) {

		final List<TweetTableObject> tweets_selected = new ArrayList<TweetTableObject>(
				0);

		for (TweetTableObject tweetTableObject : tweets) {
			if (tweetTableObject.getStatus().getGeoLocation() != null) {
				tweets_selected.add(tweetTableObject);
			}
		}

		String sql = "INSERT INTO geolocation "
				+ "(tweet_id, latitude, longitude, exact_coordinates, user_address, user_time_zone, location)"
				+ " VALUES (?, ?, ?, ?, ?, ?, GeomFromText(?))"
				+ "ON DUPLICATE KEY UPDATE latitude = VALUES(latitude), latitude = VALUES(longitude),"
				+ "location = VALUES(location)";

		jdbcTemplateObject.batchUpdate(sql, new BatchPreparedStatementSetter() {

			@Override
			public void setValues(PreparedStatement ps, int i)
					throws SQLException {
				TweetTableObject tweet = tweets_selected.get(i);
				ps.setString(1, tweet.getTweet_id());
				ps.setDouble(2, tweet.getStatus().getGeoLocation()
						.getLatitude());
				ps.setDouble(3, tweet.getStatus().getGeoLocation()
						.getLongitude());
				ps.setInt(4, 1);
				ps.setString(5, tweet.getStatus().getUser().getLocation());
				ps.setString(6, tweet.getStatus().getUser().getTimeZone());
				ps.setString(7, "POINT("
						+ tweet.getStatus().getGeoLocation().getLatitude()
						+ " "
						+ tweet.getStatus().getGeoLocation().getLongitude()
						+ ")");
			}

			@Override
			public int getBatchSize() {
				return tweets_selected.size();
			}
		});

	}

	public void insertTweetGeoLocationFeature(TweetTableObject tweet) {

		if (tweet.getStatus().getGeoLocation() != null) {
			double latitude = tweet.getStatus().getGeoLocation().getLatitude();
			double longitude = tweet.getStatus().getGeoLocation()
					.getLongitude();

			String sql = "INSERT INTO geolocation "
					+ "(tweet_id, latitude, longitude, exact_coordinates, user_address, user_time_zone, location)"
					+ " VALUES (?, ?, ?, ?, ?, ?, GeomFromText(?))";

			// jdbcTemplate = new JdbcTemplate(dataSource);
			System.out.println("****************" + "GeomFromText('POINT("
					+ latitude + " " + longitude + ")')");
			jdbcTemplateObject.update(sql, new Object[] { tweet.getTweet_id(),
					latitude, longitude, 1,
					tweet.getStatus().getUser().getLocation(),
					tweet.getStatus().getUser().getTimeZone(),
					"POINT(" + latitude + " " + longitude + ")" });

		}
	}

	public void insertBulkRetweetFeature(List<TweetTableObject> tweets) {

		final List<TweetTableObject> tweets_selected = new ArrayList<TweetTableObject>(
				0);

		for (TweetTableObject tweetTableObject : tweets) {
			if (tweetTableObject.isRetweet()) {
				tweets_selected.add(tweetTableObject);
			}
		}

		// id int(11) NOT NULLauto inc id for this table
		// tweet_id varchar(22) NOT NULLJust a tweet id
		// is_retweet tinyint(1) NOT NULLflag to to know if this tweet is
		// retweet or not
		// retweets_count int(11) NULLif retweet then get this value from
		// retweeted_status object
		// old_tweet_id varchar(22) NULLnull if not retweet else orignial
		// tweet_id

		String sql = "INSERT INTO retweets "
				+ "(tweet_id, is_retweet, retweets_count, old_tweet_id)"
				+ " VALUES (?, ?, ?, ?)";

		jdbcTemplateObject.batchUpdate(sql, new BatchPreparedStatementSetter() {

			@Override
			public void setValues(PreparedStatement ps, int i)
					throws SQLException {
				TweetTableObject tweet = tweets_selected.get(i);
				ps.setString(1, tweet.getTweet_id());
				ps.setInt(2, 1);
				ps.setLong(3, tweet.getStatus().getRetweetedStatus()
						.getRetweetCount());
				ps.setString(
						4,
						String.valueOf(tweet.getStatus().getRetweetedStatus()
								.getId()));

			}

			@Override
			public int getBatchSize() {
				return tweets_selected.size();
			}
		});
	}

	// ******************************************************************************************
	// public void delete(Integer id){
	// String SQL = "delete from tweets where id = ?";
	// jdbcTemplateObject.update(SQL, id);
	// System.out.println("Deleted Record with ID = " + id );
	// return;
	// }

	// public void update(Integer id, Integer age){
	// String SQL = "update Tweet set age = ? where id = ?";
	// jdbcTemplateObject.update(SQL, age, id);
	// System.out.println("Updated Record with ID = " + id );
	// return;
	// }

}
