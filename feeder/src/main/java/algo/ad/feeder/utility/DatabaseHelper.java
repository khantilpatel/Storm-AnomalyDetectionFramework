package algo.ad.feeder.utility;

import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.datasource.DriverManagerDataSource;

import algo.ad.feeder.dao.Queries;
import algo.ad.feeder.dao.TweetJDBCTemplate;
import algo.ad.feeder.dao.Users;

public class DatabaseHelper {

	public static String[] getKeywordsFromDB(
			TweetJDBCTemplate tweetsJDBCTemplateMain, String DB_BASE_URL,
			String DB_NAME, int MAX_NUMBER_OF_QUERIES) {

		List<Queries> listofAllQueries = new ArrayList<Queries>(0);
		List<Users> listofAllUsers = new ArrayList<Users>(0);

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
			TweetJDBCTemplate tweetsJDBCTemplateMain, String DB_BASE_URL,
			String DB_NAME, int MAX_NUMBER_OF_QUERIES) {

		List<Queries> listofAllQueries = new ArrayList<Queries>(0);
		List<Users> listofAllUsers = new ArrayList<Users>(0);

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

			listofAllQueries.addAll(listofQueries);
		}

		return listofAllQueries;
	}
}
