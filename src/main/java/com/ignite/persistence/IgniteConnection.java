package com.ignite.persistence;

import java.sql.Connection;
import java.sql.DriverManager;

public class IgniteConnection {

	public static Connection getConnection() {
		Connection conn = null;

		try {
			// Register JDBC driver.
			Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

			// Open JDBC connection.
			conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
		} catch (Exception e) {
			e.printStackTrace();
		}

		return conn;
	}
}
