package algo.ad.dao;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import common.feeder.utility.AggregateUtilityFunctions;
import common.feeder.utility.TweetJDBCTemplate;

import data.collection.entity.Tweet;
import algo.ad.utility.Algorithm_Peak_Windows;
import algo.ad.utility.Bin;
import algo.ad.utility.PickWindow;


public class TestJDBC {
	public static void main(String[] args) {
		DriverManagerDataSource datasource = new DriverManagerDataSource();
		datasource.setDriverClassName("com.mysql.jdbc.Driver");
		datasource.setUrl("jdbc:mysql://localhost:3306/test-replica");
		datasource.setUsername("root");
		datasource.setPassword("root");

		int AGGREGATE_FACTOR_IN_MINUTES = 360;
		
		TweetJDBCTemplate tweetsJDBCTemplate = new TweetJDBCTemplate();
		tweetsJDBCTemplate.setDataSource(datasource);
		
		System.out.println("------Listing Multiple Records--------");
		List<Tweet> tweets = tweetsJDBCTemplate
				.listTweets("select * from tweets where query_id = 91 AND sentiment IN ('2')");
		// Set Start date
		String date_string = "2013-06-18 06:00:00";
		DateTimeFormatter formatter = DateTimeFormat
				.forPattern("yyyy-MM-dd HH:mm:ss");
		DateTime dt = formatter.parseDateTime(date_string);
		Date startDate = dt.toDate();// tweets.get(0).getCreated_at();
		Date currentAggregateDateTime = AggregateUtilityFunctions
				.addMinutesToDate(AGGREGATE_FACTOR_IN_MINUTES, startDate);
		
		List<Bin> bins = new ArrayList<Bin>();
		Bin current_bin = new Bin();
		// Date endDate
		for (Tweet record : tweets) {
			if (record.getCreated_at().compareTo(currentAggregateDateTime) < 0) {
				current_bin.addTweets(record);
				current_bin.setCount(current_bin.getCount() + 1);
			} else {
				current_bin.setDate(currentAggregateDateTime);
				bins.add(current_bin);
				
				current_bin = new Bin();
				// current_bin.setCount(current_bin.getCount()+1);

				currentAggregateDateTime = AggregateUtilityFunctions
						.addMinutesToDate(AGGREGATE_FACTOR_IN_MINUTES,
								currentAggregateDateTime);
				if (currentAggregateDateTime.toString().equals(
						"Thu Jul 11 06:45:12 UTC 2013")) {
					System.out.println("Gochyaa");
				}

				if (record.getCreated_at().compareTo(currentAggregateDateTime) < 0) {
					current_bin.addTweets(record);
					current_bin.setCount(current_bin.getCount() + 1);
				} else {

					while (!(record.getCreated_at().compareTo(
							currentAggregateDateTime) < 0)) {
						current_bin.setDate(currentAggregateDateTime);
						bins.add(current_bin);

						current_bin = new Bin();
						// current_bin.setCount(current_bin.getCount()+1);

						currentAggregateDateTime = AggregateUtilityFunctions
								.addMinutesToDate(AGGREGATE_FACTOR_IN_MINUTES,
										currentAggregateDateTime);
					}

					if (record.getCreated_at().compareTo(
							currentAggregateDateTime) < 0) {
						current_bin.addTweets(record);
						current_bin.setCount(current_bin.getCount() + 1);
					}
				}

			}
			// System.out.print("Tweet_Id : " + record.getTweet_id());
			// System.out.print("unix_timestamp : " + record.getCreated_at());
		}
		Algorithm_Peak_Windows algo_anomalyDetection = new Algorithm_Peak_Windows();
		int index = 0;
		for (Bin bin : bins) {
			index++;
			algo_anomalyDetection.find_peak_window(bin,2);
			System.out.print(index + ". count : " + bin.getCount());
			System.out.print(" || unix_timestamp : " + bin.getDate());
			System.out.println("\n");
		}
		index = 0;
		System.out.println("\n ***********Peak Windows In the List***********");
		for (PickWindow window : algo_anomalyDetection.peak_Windows) {
			index++;
			System.out.println(index + ". StartTime:" + window.getStartTime()
					+ "EndTime" + window.getEndTime());
			System.out.println("\n");
		}
		// System.out.println("----Listing Record with ID = 2 -----" );
		// Student student = studentJDBCTemplate.getStudent(2);
		// System.out.print("ID : " + student.getId() );
		// System.out.print(", Name : " + student.getName() );
		// System.out.println(", Age : " + student.getAge());

	}
}