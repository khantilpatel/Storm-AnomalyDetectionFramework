package algo.ad.feeder.dao;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class UsersMapper implements RowMapper<Users> {
	public Users mapRow(ResultSet rs, int rowNum) throws SQLException {
		Users user = new Users();
		user.setDatabase_name(rs.getString("db_name"));
		user.setUsername(rs.getString("username"));
		
		return user;
	}
}