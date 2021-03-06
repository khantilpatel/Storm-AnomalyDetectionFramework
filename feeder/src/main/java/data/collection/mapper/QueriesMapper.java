package data.collection.mapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import data.collection.entity.Queries;
import data.collection.json.JsonAnomalyConfig;
import data.collection.json.JsonQueryConfig;

public class QueriesMapper implements RowMapper<Queries> {
	public Queries mapRow(ResultSet rs, int rowNum) throws SQLException {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		Queries query = new Queries();

		query.setQuery_id(rs.getLong("query_id"));
		query.setQuery(rs.getString("query"));
		query.setQuery_status(rs.getInt("querystatus"));

		try {
			if (!(rs.getString("config").equals(""))) {

				query.setJsonQueryConfig(mapper.readValue(
						rs.getString("config"), JsonQueryConfig.class));

			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			if ((rs.getString("anomaly_config") != null && !rs.getString(
					"anomaly_config").equals(""))) {

				query.setJsonAnomalyConfig(mapper.readValue(
						rs.getString("anomaly_config"), JsonAnomalyConfig.class));

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return query;
	}
}