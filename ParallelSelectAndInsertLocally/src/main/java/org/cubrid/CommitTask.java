package org.cubrid;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public class CommitTask implements Callable<Void> {
	private Connection connection;

	public CommitTask(Connection connection) {
		this.connection = connection;
	}

	@Override
	public Void call() throws SQLException {
		try {
			connection.commit();
		} catch (SQLException e) {
			throw e;
		}

		return null;
	}
}
