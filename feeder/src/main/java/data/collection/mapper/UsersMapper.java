package data.collection.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import data.collection.entity.Users;

public class UsersMapper implements RowMapper<Users> {
	public Users mapRow(ResultSet rs, int rowNum) throws SQLException {
		Users user = new Users();
		user.setDatabase_name(rs.getString("db_name"));
		user.setUsername(rs.getString("username"));
		
		return user;
	}
}