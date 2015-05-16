package common.feeder.utility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBConnection {

	Connection connection;
	String DBname;
	String username;
	String password;

	String dbUrl = "jdbc:mysql://localhost/";
	String dbClass = "com.mysql.jdbc.Driver";
	String query = "Select distinct(table_name) from INFORMATION_SCHEMA.TABLES";

	public DBConnection(String dBname, String username, String password) {
		super();
		DBname = dBname;
		this.username = username;
		this.password = password;

	}

	public void connect() {
		try {
			Class.forName(dbClass);
			connection = DriverManager.getConnection(dbUrl, username, password);

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void testConnect() {

		Statement statement;
		try {
			statement = connection.createStatement();

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String tableName = resultSet.getString(1);
				System.out.println("Table name : " + tableName);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	

	public void close() throws SQLException {
		connection.close();
	}
	
	

}
