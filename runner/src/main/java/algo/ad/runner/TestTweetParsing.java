package algo.ad.runner;

import java.text.DecimalFormat;

public class TestTweetParsing {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// String tweet =
		// "Sinhalese Sports Club 246/4 (20/20 ov); Colombo Cricket Club 182 (19.2/20 ov) #cricket 434";
		// String query = "#cricket";
		// if(tweet.toLowerCase().contains(query.toLowerCase()))
		// System.out.println("YES::"+ tweet +"||...contains...||" +query);
		// else
		// System.out.println("NO IT DOESN't!!");
		// try {
		// FilesUtil.writeToNewTextFile("log_tweet_parsing", "first line");
		//
		// FilesUtil.appendToTextFile("log_tweet_parsing", "\nSecond line");
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
//		Twitter _twitterStream;
//		   String consumerKey = "8Yp9j3R1oD6WeAeqKLMuA"; //args[0]; 
//	        String consumerSecret = "9SbeR8422jgT3njhZ7hP2og8ttIICD2VThraXn22Vw";//args[1]; 
//	        String accessToken = "61982388-L4t44vkyCGgANYREol9T7L6TwKiVd9mQkw3xBdZ3c";//args[2]; 
//	        String accessTokenSecret = "2bqJKti7KatTOSGQL7Z5knuqyCVtdN0MOGkeVsjkn5G7F";// args[3];
//	        String keyWords = "#DilDhadakneDo";// Arrays.copyOfRange(arguments, 4, arguments.length);
//		_twitterStream = new TwitterFactory().getInstance();
//
//		_twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
//		AccessToken token = new AccessToken(accessToken, accessTokenSecret);
//		_twitterStream.setOAuthAccessToken(token);
		
//		 try {
//	            Query query = new Query(keyWords);
//	            QueryResult result;
//	            int i = 0;
//	            do {
//	                result = _twitterStream.search(query);
//	                List<Status> tweets = result.getTweets();
//	                
//	                for (Status tweet : tweets) {
//	                    System.out.println(i+":: @" + tweet.getUser().getScreenName() + " - " + tweet.getText());
//	                }
//	                i++;
//	            } while ((query = result.nextQuery()) != null);
//	            System.exit(0);
//	        } catch (TwitterException te) {
//	            te.printStackTrace();
//	            System.out.println("Failed to search tweets: " + te.getMessage());
//	            System.exit(-1);
//	        }
		
	//	String hashtag ="#BieberRoast";
		
	//	String tweets = "Yes it's true! I'm roasting @justinbieber march 30 @comedycentral#bieberroast  can't wait to teach him stuff";
		
	//	boolean result = tweets.toLowerCase().contains(hashtag.toLowerCase());
		Integer a = 2228;
		Integer b = 3602;
		double result = (double)a/(double)b;
		System.out.println(result);
		//String strDouble = String.format("%.10f", 1.23456);
		DecimalFormat df = new DecimalFormat("##.#########");
		String strDouble = df.format(2323.4563458888999); 
		System.out.println(Double.valueOf(strDouble));
		//System.out.println(tweets.replace("@", ""));
	}

}
