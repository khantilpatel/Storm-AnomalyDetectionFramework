package algo.ad.dao;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Sentiment140ResponseDAO {
	
	ArrayList<Sentiment140Response> data;

	public ArrayList<Sentiment140Response> getData() {
		return data;
	}

	public void setData(ArrayList<Sentiment140Response> _data) {
		data = _data;
	}
	
	

}
