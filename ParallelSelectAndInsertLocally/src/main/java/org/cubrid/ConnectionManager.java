package org.cubrid;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConnectionManager {
	private static final Logger LOGGER = Logger.getLogger(ConnectionManager.class.getName());

	public Connection getConnection() throws ClassNotFoundException, IOException, SQLException {
		Properties properties = new Properties();
		Connection connection = null;

		try (Reader reader = new FileReader("databases.properties")) {
			properties.load(reader);
		} catch (IOException e) {
			throw e;
		}

		String url = properties.getProperty("Url");
		if (url == null) {
			new NullPointerException("The url is null");
		}
		
		String user = properties.getProperty("User");
		String password = properties.getProperty("Password");

		try {
			Class.forName("cubrid.jdbc.driver.CUBRIDDriver");
		} catch (ClassNotFoundException e) {
			throw e;
		}

		try {
			connection = DriverManager.getConnection(url, user, password);
			connection.setAutoCommit(false);
			connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
		} catch (SQLException e) {
			throw e;
		}

		return connection;
	}

	public long getTableRowCount(Connection connection, String tableName)
			throws IllegalArgumentException, SQLException {
		if (connection == null) {
			throw new IllegalArgumentException("The connection is null");
		}

		if (tableName == null) {
			throw new IllegalArgumentException("The table name is null");
		}

		StringBuilder query = new StringBuilder();
		query.append("select count(*) from ").append(tableName);

		try (PreparedStatement statement = connection.prepareStatement(query.toString());
				ResultSet resultSet = statement.executeQuery()) {
			if (resultSet.next()) {
				return resultSet.getLong(1);
			}
		} catch (SQLException e) {
			throw e;
		}

		throw new SQLException(String.format("Failed to get row count for table: %s", tableName));
	}

	private String getFindUsableIndexQuery(boolean withOwnerName) {
		StringBuilder query = new StringBuilder();

		query.append("select a.index_name, a.is_primary_key, b.key_attr_name").append(" ");
		query.append("from db_index as a, db_index_key as b, db_attribute as c").append(" ");
		query.append("where").append(" ");
		query.append("a.index_name = b.index_name").append(" ");
		query.append("and a.class_name = b.class_name").append(" ");
		query.append("and a.owner_name = b.owner_name").append(" ");
		query.append("and b.class_name = c.class_name").append(" ");
		query.append("and b.owner_name = c.owner_name").append(" ");
		query.append("and b.key_attr_name = c.attr_name").append(" ");
		query.append("and a.class_name = ?").append(" ");

		if (withOwnerName) {
			query.append("and a.owner_name = upper(?)").append(" ");
		} else {
			query.append("and a.owner_name = current_user").append(" ");
		}

		query.append("and a.filter_expression is null").append(" ");
		query.append("and a.have_function = 'NO'").append(" ");
		query.append("and a.status != 'INVISIBLE INDEX'").append(" ");
		query.append("and b.key_order = 0").append(" ");
		query.append("and c.is_nullable = 'NO'").append(" ");
		query.append("order by a.is_primary_key desc").append(" ");
		query.append("limit 1").append(" ");

		return query.toString();
	}

	public String getFirstColumnOfUsableIndex(Connection connection, String tableName) throws SQLException {
		if (connection == null) {
			throw new IllegalArgumentException("The connection is null");
		}

		if (tableName == null) {
			throw new IllegalArgumentException("The table name is null");
		}

		String bindTableName = null;
		String bindOwnerName = null;
		boolean withOwnerName = false;

		int indexOfDot = tableName.indexOf(".");
		if (indexOfDot > 0) {
			bindOwnerName = tableName.substring(0, indexOfDot);
			bindTableName = tableName.substring(indexOfDot + 1);
			withOwnerName = true;
		} else {
			bindTableName = tableName;
		}

		String query = getFindUsableIndexQuery(withOwnerName);

		try (PreparedStatement statement = connection.prepareStatement(query)) {
			if (withOwnerName) {
				statement.setString(1, bindTableName);
				statement.setString(2, bindOwnerName);
			} else {
				statement.setString(1, bindTableName);
			}

			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) {
					String indexName = resultSet.getString("index_name");
					String indexFirstColumnName = resultSet.getString("key_attr_name");

					LOGGER.log(Level.INFO, String.format("The %s index is usable", indexName));
					return indexFirstColumnName;
				}
			} catch (SQLException e) {
				throw e;
			}
		} catch (SQLException e) {
			throw e;
		}

		return null;
	}

	public static String getFetchSourceRecordsQuery(String tableName, String columnName) {
		if (tableName == null) {
			throw new IllegalArgumentException("The table name name is null");
		}

		StringBuilder query = new StringBuilder();

		if (columnName != null) {
			query.append("select /*+ USE_IDX */ * from ").append(tableName).append(" where ").append(columnName)
					.append(" = ").append(columnName).append(" order by ").append(columnName);
		} else {
			/* Consider using an 'ORDER BY' clause to ensure consistent data order. */
			query.append("select * from ").append(tableName);
		}

		query.append(" ").append("limit ?, ?");

		return query.toString();
	}

	public static String getInsertRecordToDestinationQuery(String sourceTableName, String sourceColumnName, String destinationTableName) {
		if (sourceTableName == null) {
			throw new IllegalArgumentException("The source table name name is null");
		}
		
		if (destinationTableName == null) {
			throw new IllegalArgumentException("The destination table name name is null");
		}

		StringBuilder query = new StringBuilder();
		query.append("insert into ").append(destinationTableName).append(" ")
				.append(getFetchSourceRecordsQuery(sourceTableName, sourceColumnName));

		return query.toString();
	}
}
