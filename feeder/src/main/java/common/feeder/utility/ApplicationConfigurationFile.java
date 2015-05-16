package common.feeder.utility;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationConfigurationFile implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6687474980083368338L;

	private String jdbc_username;
	private String jdbc_password;
	private String jdbc_url;

	private String mongodb_base_url;
	private String mongodb_username;
	private String mongodb_password;
	private String mongodb_port;

	private String sentiment140_url;

	private String twitter_consumerKey;
	private String twitter_consumerSecret;
	private String twitter_accessToken;
	private String twitter_accessTokenSecret;

	private int sparkline_window_period;
	private int sparkline_aggregation_period;
	private int sparkline_refresh_interval;
	
	public Logger LOG = LoggerFactory
			.getLogger(ApplicationConfigurationFile.class);

	public ApplicationConfigurationFile() {

	}

	public String getJdbc_username() {
		return jdbc_username;
	}

	public void setJdbc_username(String jdbc_username) {
		this.jdbc_username = jdbc_username;
	}

	public String getJdbc_password() {
		return jdbc_password;
	}

	public void setJdbc_password(String jdbc_password) {
		this.jdbc_password = jdbc_password;
	}

	public String getJdbc_url() {
		return jdbc_url;
	}

	public void setJdbc_url(String jdbc_url) {
		this.jdbc_url = jdbc_url;
	}

	public String getMongodb_base_url() {
		return mongodb_base_url;
	}

	public void setMongodb_base_url(String mongodb_base_url) {
		this.mongodb_base_url = mongodb_base_url;
	}

	public String getMongodb_username() {
		return mongodb_username;
	}

	public void setMongodb_username(String mongodb_username) {
		this.mongodb_username = mongodb_username;
	}

	public String getMongodb_password() {
		return mongodb_password;
	}

	public void setMongodb_password(String mongodb_password) {
		this.mongodb_password = mongodb_password;
	}

	public String getMongodb_port() {
		return mongodb_port;
	}

	public void setMongodb_port(String mongodb_port) {
		this.mongodb_port = mongodb_port;
	}

	public String getSentiment140_url() {
		return sentiment140_url;
	}

	public void setSentiment140_url(String sentiment140_url) {
		this.sentiment140_url = sentiment140_url;
	}

	public String getTwitter_consumerKey() {
		return twitter_consumerKey;
	}

	public void setTwitter_consumerKey(String twitter_consumerKey) {
		this.twitter_consumerKey = twitter_consumerKey;
	}

	public String getTwitter_consumerSecret() {
		return twitter_consumerSecret;
	}

	public void setTwitter_consumerSecret(String twitter_consumerSecret) {
		this.twitter_consumerSecret = twitter_consumerSecret;
	}

	public String getTwitter_accessToken() {
		return twitter_accessToken;
	}

	public void setTwitter_accessToken(String twitter_accessToken) {
		this.twitter_accessToken = twitter_accessToken;
	}

	public String getTwitter_accessTokenSecret() {
		return twitter_accessTokenSecret;
	}

	public void setTwitter_accessTokenSecret(String twitter_accessTokenSecret) {
		this.twitter_accessTokenSecret = twitter_accessTokenSecret;
	}

	public int getSparkline_window_period() {
		return sparkline_window_period;
	}

	public void setSparkline_window_period(int sparkline_window_period) {
		this.sparkline_window_period = sparkline_window_period;
	}

	public int getSparkline_aggregation_period() {
		return sparkline_aggregation_period;
	}

	public void setSparkline_aggregation_period(int sparkline_aggregation_period) {
		this.sparkline_aggregation_period = sparkline_aggregation_period;
	}

	public int getSparkline_refresh_interval() {
		return sparkline_refresh_interval;
	}

	public void setSparkline_refresh_interval(int sparkline_refresh_interval) {
		this.sparkline_refresh_interval = sparkline_refresh_interval;
	}



	


}
