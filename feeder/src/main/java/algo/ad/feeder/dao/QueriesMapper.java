package algo.ad.feeder.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimeZone;

import org.springframework.jdbc.core.RowMapper;

public class QueriesMapper implements RowMapper<Queries> {
	public Queries mapRow(ResultSet rs, int rowNum) throws SQLException {
		Queries query = new Queries();
		query.setQuery_id(rs.getLong("query_id"));
		query.setQuery(rs.getString("query"));
		query.setQuery_status(rs.getInt("querystatus"));
		return query;
	}
}