package data.collection.entity;

import java.io.Serializable;
import java.util.Date;

import data.collection.json.JsonAnomalyConfig;
import data.collection.json.JsonQueryConfig;



public class QueriesForEmitter implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2299087384545232093L;
	/**
	 * 
	 */
	long query_id;
	String query;
	int query_status;
	String db_name;
	JsonQueryConfig jsonQueryConfig;
	
	JsonAnomalyConfig jsonAnomalyConfig;
	
	Date start_date;
	//{"window_period": {"value": 1,"type": "month"},
	//"aggregation":{"value":3,"type":"hours"},
	//"window_threshold": 3,
	// "local_threshold": 3}
	
	
	public long getQuery_id() {
		return query_id;
	}
	public void setQuery_id(long query_id) {
		this.query_id = query_id;
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public int getQuery_status() {
		return query_status;
	}
	public void setQuery_status(int query_status) {
		this.query_status = query_status;
	}
	public String getDb_name() {
		return db_name;
	}
	public void setDb_name(String db_name) {
		this.db_name = db_name;
	}
	public JsonQueryConfig getJsonQueryConfig() {
		return jsonQueryConfig;
	}
	public void setJsonQueryConfig(JsonQueryConfig jsonQueryConfig) {
		this.jsonQueryConfig = jsonQueryConfig;
	}
	public JsonAnomalyConfig getJsonAnomalyConfig() {
		return jsonAnomalyConfig;
	}
	public void setJsonAnomalyConfig(JsonAnomalyConfig jsonAnomalyConfig) {
		this.jsonAnomalyConfig = jsonAnomalyConfig;
	}
	public Date getStart_date() {
		return start_date;
	}
	public void setStart_date(Date start_date) {
		this.start_date = start_date;
	}
	
	
	
}
