package common.feeder.utility;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class TweetJDBCTemplateConnectionPool implements Serializable  {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3803537386727290344L;
	
	public static Logger LOG = LoggerFactory.getLogger(TweetJDBCTemplateConnectionPool.class);
	
	static Map<String,TweetJDBCTemplate> mapJDBCTemplate =new HashMap<String,TweetJDBCTemplate>(0);
	
	
	public static TweetJDBCTemplate getTweetJDBCTemplate(String DB_NAME, ApplicationConfigurationFile config)
	{
		TweetJDBCTemplate jdbcTemplate = mapJDBCTemplate.get(DB_NAME);
		LOG.info("Came to get::"+ DB_NAME);
		if(jdbcTemplate == null)
		{
			LOG.info("Not found, now loading::"+ DB_NAME);
			
			DriverManagerDataSource datasource = new DriverManagerDataSource();
			datasource.setDriverClassName("com.mysql.jdbc.Driver");
			datasource.setUrl(config.getJdbc_url() + DB_NAME);// test-replica
			datasource.setUsername(config.getJdbc_username());
			datasource.setPassword(config.getJdbc_password());

			jdbcTemplate = new TweetJDBCTemplate();
			jdbcTemplate.setDataSource(datasource);
			
			mapJDBCTemplate.put(DB_NAME, jdbcTemplate);
		}
		else
		{
			LOG.info("Now Got::"+ DB_NAME);
		}
		return jdbcTemplate;
		
	}
	
	
}
