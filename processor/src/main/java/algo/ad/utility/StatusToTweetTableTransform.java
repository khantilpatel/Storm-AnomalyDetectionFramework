package algo.ad.utility;

import java.util.ArrayList;
import java.util.Calendar;

import data.collection.entity.RawTweet;
import data.collection.entity.TweetTableObject;
import twitter4j.Status;

public class StatusToTweetTableTransform {

	public static ArrayList<TweetTableObject> transformStatusToTweetList(
			ArrayList<RawTweet> statusList) {
		ArrayList<TweetTableObject> tweetsTBList = new ArrayList<TweetTableObject>();
		for (RawTweet rawTweet : statusList) {
			
	
			
			TweetTableObject tweetTB = new TweetTableObject();
			Status status = rawTweet.getStatus();
			
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(status.getCreatedAt());
			
			// query_id
			tweetTB.setTweet_id("" + status.getId());
			tweetTB.setText(status.getText());
			tweetTB.setUser_name(status.getUser().getName());
			tweetTB.setYear(calendar.get(Calendar.YEAR));
			tweetTB.setMonth(calendar.get(Calendar.MONTH));
			tweetTB.setDay(calendar.get(Calendar.DAY_OF_MONTH));
			tweetTB.setHour(calendar.get(Calendar.HOUR_OF_DAY));
			tweetTB.setMin(calendar.get(Calendar.MINUTE));
			tweetTB.setSec(calendar.get(Calendar.SECOND));
			tweetTB.setUnix_timestamp(status.getCreatedAt().getTime()/1000);
			tweetTB.setUser_profile_image_url(status.getUser().getProfileImageURL());
			tweetTB.setCreated_at(status.getCreatedAt());
			tweetTB.setJsonObject(rawTweet.getJsonObjectString());
			tweetTB.setRetweet(false);
			if(status.isRetweet())
			{
				tweetTB.setRetweet(true);
				tweetTB.setText_retweeted(status.getRetweetedStatus().getText());
			}
			
			tweetsTBList.add(tweetTB);
		}
		// sentiment
		// sentiment_original

		return tweetsTBList;
	}
	
	public static TweetTableObject transformStatusToTweet(
			RawTweet rawTweet) {

			TweetTableObject tweetTB = new TweetTableObject();
			Status status = rawTweet.getStatus();
			
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(status.getCreatedAt());
			
			// query_id
			tweetTB.setTweet_id("" + status.getId());
			tweetTB.setText(status.getText());
			tweetTB.setUser_name(status.getUser().getName());
			tweetTB.setYear(calendar.get(Calendar.YEAR));
			tweetTB.setMonth(calendar.get(Calendar.MONTH+1)); // Gregorian and Julian calendars is JANUARY which is 0;
			tweetTB.setDay(calendar.get(Calendar.DAY_OF_MONTH));
			tweetTB.setHour(calendar.get(Calendar.HOUR_OF_DAY));
			tweetTB.setMin(calendar.get(Calendar.MINUTE));
			tweetTB.setSec(calendar.get(Calendar.SECOND));
			tweetTB.setUnix_timestamp(status.getCreatedAt().getTime()/1000);
			tweetTB.setUser_profile_image_url(status.getUser().getProfileImageURL());
			tweetTB.setCreated_at(status.getCreatedAt());
			tweetTB.setJsonObject(rawTweet.getJsonObjectString());
			tweetTB.setRetweet(false);
			tweetTB.setStatus(status);
			if(status.isRetweet())
			{
				tweetTB.setRetweet(true);
				tweetTB.setText_retweeted(status.getRetweetedStatus().getText());
			}
			
			//tweet_id, latitude, longitude, location, exact_coordinates, user_address, user_time_zone
		// sentiment
		// sentiment_original

		return tweetTB;
	}
	

}
