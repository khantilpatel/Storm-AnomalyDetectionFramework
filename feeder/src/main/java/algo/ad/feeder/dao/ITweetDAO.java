package algo.ad.feeder.dao;

import java.util.List;

import javax.sql.DataSource;

/**
 * DAO class representing Tweets table in database
 * It allow read only access to tweets table
 * @author khantil
 *
 */

public interface ITweetDAO {
	
	  /** 
	    * This is the method to be used to initialize
	    * database resources ie. connection.
	    */
	   public void setDataSource(DataSource ds);
	   /** 
	    * This is the method to be used to create
	    * a record in the Tweet table.
	    */
	//   public void create(String name, Integer age);
	   /** 
	    * This is the method to be used to list down
	    * a record from the Tweet table corresponding
	    * to a passed Tweet id.
	    */
	   public Tweet getTweet(Integer id);
	   /** 
	    * This is the method to be used to list down
	    * all the records from the Tweet table.
	    */
	   public List<Tweet> listTweets(String SQL);
	   /** 
	    * This is the method to be used to delete
	    * a record from the Tweet table corresponding
	    * to a passed Tweet id.
	    */
	//   public void delete(Integer id);
	   /** 
	    * This is the method to be used to update
	    * a record into the Tweet table.
	    */
	//   public void update(Integer id, Integer age);

}
