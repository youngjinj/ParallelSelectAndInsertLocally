package org.cubrid;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CopyTask implements Callable<Void> {
	private static final Logger LOGGER = Logger.getLogger(CopyTask.class.getName());

	private String sourceTableName;
	private String sourceIndexFirstColumnName;
	private Connection destinationConnection;
	private String destinationTableName;
	private ConcurrentLinkedQueue<Map<String, Long>> queue;
	private ProgressBarTask progressBar;

	public CopyTask(CopyTaskInfo copyTaskInfo) {
		this.sourceTableName = copyTaskInfo.getSourceTableName();
		this.sourceIndexFirstColumnName = copyTaskInfo.getSourceIndexFirstColumnName();
		this.destinationConnection = copyTaskInfo.getDestinationConnection();
		this.destinationTableName = copyTaskInfo.getDestinationTableName();
		this.queue = copyTaskInfo.getQueue();
		this.progressBar = copyTaskInfo.getProgressBar();

		assert (sourceTableName != null);
		assert (sourceIndexFirstColumnName != null);
		assert (destinationConnection != null);
		assert (destinationTableName != null);
		assert (queue != null);
		assert (progressBar != null);
	}

	@Override
	public Void call() throws SQLException {
		LOGGER.log(Level.INFO, String.format("[%s] Starting copy task", Thread.currentThread().getName()));

		String insertRecordToDestinationQuery = ConnectionManager.getInsertRecordToDestinationQuery(sourceTableName,
				sourceIndexFirstColumnName, destinationTableName);

		try (PreparedStatement statement = destinationConnection.prepareStatement(insertRecordToDestinationQuery)) {
			int insertedRowCount = 0;
			
			while (!Thread.interrupted()) {
				Map<String, Long> work = queue.poll();
				
				if (work != null) {
					statement.setLong(1, work.get(PrepareWorkTask.LABEL_OFFSET));
					statement.setLong(2, work.get(PrepareWorkTask.LABEL_ROW_COUNT));
				} else {
					break;
				}

				insertedRowCount = statement.executeUpdate();
				progressBar.updateProgress(insertedRowCount);
			}
		} catch (SQLException e) {
			throw e;
		}

		return null;
	}
}