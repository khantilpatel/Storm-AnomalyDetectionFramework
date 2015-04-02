package algo.ad.utility;

import java.util.ArrayList;

import twitter4j.Status;
import algo.ad.feeder.dao.TweetTableObject;

public class StatusToTweetTableTransform {

	public static ArrayList<TweetTableObject> transformStatusToTweet(
			ArrayList<Status> statusList) {
		ArrayList<TweetTableObject> tweetsTBList = new ArrayList<TweetTableObject>();
		for (Status status : statusList) {
			TweetTableObject tweetTB = new TweetTableObject();
			// query_id
			tweetTB.setTweet_id("" + status.getId());
			tweetTB.setText(status.getText());
			tweetTB.setUser_name(status.getUser().getName());
			tweetTB.setYear(status.getCreatedAt().getYear());
			tweetTB.setMonth(status.getCreatedAt().getMonth());
			tweetTB.setDay(status.getCreatedAt().getDay());
			tweetTB.setHour(status.getCreatedAt().getHours());
			tweetTB.setMin(status.getCreatedAt().getMinutes());
			tweetTB.setSec(status.getCreatedAt().getSeconds());
			tweetTB.setUnix_timestamp(status.getCreatedAt().getTime());
			tweetTB.setUser_profile_image_url(status.getUser().getURL());
			tweetTB.setCreated_at(status.getCreatedAt());
			tweetTB.setJsonObject(status.getSource());
			tweetsTBList.add(tweetTB);
		}
		// sentiment
		// sentiment_original

		return tweetsTBList;
	}

}
