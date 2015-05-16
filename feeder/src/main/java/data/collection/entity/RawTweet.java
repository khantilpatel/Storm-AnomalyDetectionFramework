package data.collection.entity;

import java.io.Serializable;

import twitter4j.Status;

public class RawTweet implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2389912701904748764L;
	Status status;
	String jsonObjectString;
	public Status getStatus() {
		return status;
	}
	public void setStatus(Status status) {
		this.status = status;
	}
	public String getJsonObjectString() {
		return jsonObjectString;
	}
	public void setJsonObjectString(String jsonObjectString) {
		this.jsonObjectString = jsonObjectString;
	}
	
	
}
