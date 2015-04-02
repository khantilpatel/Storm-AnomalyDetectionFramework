package algo.ad.feeder.dao;

import java.io.Serializable;



public class Queries implements Serializable {

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
	
}
