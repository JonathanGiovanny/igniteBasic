package com.ignite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalTime;

import com.ignite.persistence.IgniteConnection;

public class App {

	public static void main(String[] args) {
		Connection conn = IgniteConnection.getConnection();
		boolean createSchema = false;

		try {
			if (conn != null) {
				if (createSchema) {
					System.out.println("Connected! Time: " + LocalTime.now());
					createTables(conn);
					System.out.println("Tables created Time: " + LocalTime.now());
					populateTables(conn);
					System.out.println("Tables populated Time: " + LocalTime.now());
				}

				queryData(conn);
			} else {
				System.out.println("Something Happened :c - Time: " + LocalTime.now());
			}

			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void queryData(Connection conn) throws Exception {
		PreparedStatement stmt = null;

		// Get data
		try {
			stmt = conn.prepareStatement("SELECT p.name, c.name FROM Person p, City c WHERE p.city_id = c.id");
			ResultSet rs = stmt.executeQuery();

			while (rs.next())
				System.out.println(rs.getString(1) + ", " + rs.getString(2));

		} finally {
			if (stmt != null) {
				stmt.close();
			}
		}
	}

	@SuppressWarnings("resource")
	public static void populateTables(Connection conn) throws Exception {
		PreparedStatement stmt = null;

		// Populate City table
		try {
			stmt = conn.prepareStatement("INSERT INTO City (id, name) VALUES (?, ?)");
			stmt.setLong(1, 1L);
			stmt.setString(2, "Forest Hill");
			stmt.executeUpdate();

			stmt.setLong(1, 2L);
			stmt.setString(2, "Denver");
			stmt.executeUpdate();

			stmt.setLong(1, 3L);
			stmt.setString(2, "St. Petersburg");
			stmt.executeUpdate();

			// Populate Person table
			stmt = conn.prepareStatement("INSERT INTO Person (id, name, city_id) VALUES (?, ?, ?)");
			stmt.setLong(1, 1L);
			stmt.setString(2, "John Doe");
			stmt.setLong(3, 3L);
			stmt.executeUpdate();

			stmt.setLong(1, 2L);
			stmt.setString(2, "Jane Roe");
			stmt.setLong(3, 2L);
			stmt.executeUpdate();

			stmt.setLong(1, 3L);
			stmt.setString(2, "Mary Major");
			stmt.setLong(3, 1L);
			stmt.executeUpdate();

			stmt.setLong(1, 4L);
			stmt.setString(2, "Richard Miles");
			stmt.setLong(3, 2L);
			stmt.executeUpdate();

		} finally {
			if (stmt != null) {
				stmt.close();
			}
		}
	}

	@SuppressWarnings("resource")
	public static void createTables(Connection conn) throws Exception {
		PreparedStatement stmt = null;
		// Create database tables.
		try {
			stmt = conn.prepareStatement(
					"CREATE TABLE City ( id LONG PRIMARY KEY, name VARCHAR) WITH \"template=replicated\"");
			// Create table based on REPLICATED template.
			stmt.executeUpdate();

			// Create table based on PARTITIONED template with one backup.
			stmt = conn.prepareStatement("CREATE TABLE Person ( id LONG, name VARCHAR, city_id LONG, "
					+ " PRIMARY KEY (id, city_id)) WITH \"backups=1, affinityKey=city_id\"");
			stmt.executeUpdate();

			// Create an index on the City table.
			stmt = conn.prepareStatement("CREATE INDEX idx_city_name ON City (name)");
			stmt.executeUpdate();

			// Create an index on the Person table.
			stmt = conn.prepareStatement("CREATE INDEX idx_person_name ON Person (name)");
			stmt.executeUpdate();

		} finally {
			if (stmt != null) {
				stmt.close();
			}
		}
	}
}
