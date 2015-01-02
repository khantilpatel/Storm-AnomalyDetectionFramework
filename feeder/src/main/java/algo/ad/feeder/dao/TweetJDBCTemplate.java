package algo.ad.feeder.dao;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;

public class TweetJDBCTemplate implements ITweetDAO {
   private DataSource dataSource;
   public StreamingResultSetEnabledJdbcTemplate jdbcTemplateObject;
   
   public void setDataSource(DataSource dataSource) {
      this.dataSource = dataSource;
      this.jdbcTemplateObject = new StreamingResultSetEnabledJdbcTemplate(dataSource);
      this.jdbcTemplateObject.setFetchSize(Integer.MIN_VALUE);
   }

//   public void create(String name, Integer age) {
//      String SQL = "insert into Tweet (name, age) values (?, ?)";
//      
//      jdbcTemplateObject.update( SQL, name, age);
//      System.out.println("Created Record Name = " + name + " Age = " + age);
//      return;
//   }

   public Tweet getTweet(Integer id) {
      String SQL = "select * from tweets where id = ?";
      Tweet Tweet = jdbcTemplateObject.queryForObject(SQL, 
                        new Object[]{id}, new TweetMapper());
      return Tweet;
   }

   public List<Tweet> listTweets(String SQL) {
      List <Tweet> Tweets = jdbcTemplateObject.query(SQL, 
                                new TweetMapper());
  
      return Tweets;
   }

//   public void delete(Integer id){
//      String SQL = "delete from tweets where id = ?";
//      jdbcTemplateObject.update(SQL, id);
//      System.out.println("Deleted Record with ID = " + id );
//      return;
//   }

//   public void update(Integer id, Integer age){
//      String SQL = "update Tweet set age = ? where id = ?";
//      jdbcTemplateObject.update(SQL, age, id);
//      System.out.println("Updated Record with ID = " + id );
//      return;
//   }

}



