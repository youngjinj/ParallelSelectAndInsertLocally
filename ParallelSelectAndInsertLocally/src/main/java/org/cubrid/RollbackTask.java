package org.cubrid;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public class RollbackTask implements Callable<Void> {
	private Connection connection;

	public RollbackTask(Connection connection) {
		this.connection = connection;
	}

	@Override
	public Void call() throws SQLException {
		try {
			connection.rollback();
		} catch (SQLException e) {
			throw e;
		}

		return null;
	}
}
