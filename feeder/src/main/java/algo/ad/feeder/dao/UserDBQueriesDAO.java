package algo.ad.feeder.dao;

import java.io.Serializable;

public class UserDBQueriesDAO implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8264148544749975955L;
	
	String db_name;
	String query_name;
	public String getDb_name() {
		return db_name;
	}
	public void setDb_name(String db_name) {
		this.db_name = db_name;
	}
	public String getQuery_name() {
		return query_name;
	}
	public void setQuery_name(String query_name) {
		this.query_name = query_name;
	}

}
