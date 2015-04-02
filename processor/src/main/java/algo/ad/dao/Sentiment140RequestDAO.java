package algo.ad.dao;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Sentiment140RequestDAO {
	
	ArrayList<Sentiment140Request> data;

	public ArrayList<Sentiment140Request> getData() {
		return data;
	}

	public void setData(ArrayList<Sentiment140Request> data) {
		this.data = data;
	}

}
