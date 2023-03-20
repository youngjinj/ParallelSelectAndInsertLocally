package org.cubrid;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ParallelSelectAndInsert {
	private static final Logger LOGGER = Logger.getLogger(ParallelSelectAndInsert.class.getName());
	
	public static final int DEFAULT_BATCH_COUNT = 5000;

	private ConnectionManager manager;

	private String sourceTableName;
	private String destinationTableName;
	private int numThreads;
	private int batchCount;
	
	private ProgressBarTask progressBar;

	private List<Connection> destinationConnectionList;
	private List<CopyTask> copyTaskList;

	private ExecutorService executorPrepareService;
	private ExecutorService executorInsertService;

	public ParallelSelectAndInsert() {
		this.manager = new ConnectionManager();
		this.batchCount = DEFAULT_BATCH_COUNT;
	}

	public void start(String paramSourceTableName, String paramDestinationTableName, int paramNumThreads, ProgressBarTask paramProgressBar) {
		LOGGER.log(Level.INFO, "Starting Parallel Select and Insert program Locally");

		assert (manager != null);

		if (paramSourceTableName == null) {
			LOGGER.log(Level.SEVERE, "The source table name is null");
			return;
		} else {
			sourceTableName = paramSourceTableName;
		}

		if (paramDestinationTableName == null) {
			LOGGER.log(Level.SEVERE, "The destination table name is null");
			return;
		} else if (paramDestinationTableName.equals(paramSourceTableName)) {
			LOGGER.log(Level.SEVERE, "The source table name and destination table name cannot be the same");
			return;
		} else {
			destinationTableName = paramDestinationTableName;
		}

		if (paramNumThreads > 0) {
			numThreads = paramNumThreads;
		} else {
			numThreads = 1;
		}

		if (paramProgressBar != null) {
			progressBar = paramProgressBar;
		} else {
			LOGGER.log(Level.SEVERE, "ProgressBarTask is null");
			return;
		}

		try (Connection sourceConnection = manager.getConnection()) {
			sourceConnection.setAutoCommit(false);
			sourceConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			
			long rowCount = manager.getTableRowCount(sourceConnection, sourceTableName);
			if (rowCount == 0) {
				LOGGER.log(Level.WARNING, "No data to copy");
				return;
			}
			
			progressBar.setTotal(rowCount);

			/*-
			 * PrepareWorkTask
			 */
			ConcurrentLinkedQueue<Map<String, Long>> queue = new ConcurrentLinkedQueue<Map<String,Long>>();
			
			PrepareWorkTaskInfo prepareTaskInfo = new PrepareWorkTaskInfo();
			prepareTaskInfo.setRowCount(rowCount);
			prepareTaskInfo.setBatchCount(batchCount);
			prepareTaskInfo.setQueue(queue);
			
			executorPrepareService = Executors.newSingleThreadExecutor();
			PrepareWorkTask prepareWorkTask = new PrepareWorkTask(prepareTaskInfo);
			Future<Void> prepareWorkFuture = executorPrepareService.submit(prepareWorkTask);
			
			/*-
			 * CopyTask
			 */
			String sourceIndexFirstColumnName = manager.getFirstColumnOfUsableIndex(sourceConnection, sourceTableName);
			
			destinationConnectionList = new ArrayList<Connection>(numThreads);
			copyTaskList = new ArrayList<CopyTask>(numThreads);
			
			for (int i = 0; i < numThreads; i++) {
				Connection connection = manager.getConnection();
				connection.setAutoCommit(false);
				
				destinationConnectionList.add(connection);
				
				/*-
				 * CopyTaskInfo
				 */
				CopyTaskInfo copyTaskInfo = new CopyTaskInfo();
				copyTaskInfo.setSourceTableName(sourceTableName);

				if (sourceIndexFirstColumnName != null) {
					copyTaskInfo.setSourceIndexFirstColumnName(sourceIndexFirstColumnName);
				}
				
				copyTaskInfo.setDestinationConnection(connection);
				copyTaskInfo.setDestinationTableName(destinationTableName);
				copyTaskInfo.setQueue(queue);
				copyTaskInfo.setProgressBar(progressBar);
				
				copyTaskList.add(new CopyTask(copyTaskInfo));
			}
			
			executorInsertService = Executors.newFixedThreadPool(numThreads);

			List<Future<Void>> copyFutureList = null;
			try {
				copyFutureList = executorInsertService.invokeAll(copyTaskList);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			/*-
			 * Finish
			 */

			/* Because it is a select query, no commit is required. */
			sourceConnection.rollback();
			
			doCommit();
			doCloseConnection();

			executorInsertService.shutdown();
			executorPrepareService.shutdown();

			return;
		} catch (Exception e) {
			e.printStackTrace();
		}

		/*-
		 * Exception
		 */
		
		doRollback();
		doCloseConnection();
		
		if (!executorInsertService.isTerminated()) {
			executorInsertService.shutdownNow();
		}
		
		if (!executorPrepareService.isTerminated()) {
			executorPrepareService.shutdownNow();
		}

		return;
	}

	private void doCommit() {
		if (destinationConnectionList == null) {
			return;
		}

		try {
			for (Connection connection : destinationConnectionList) {
				connection.commit();
			}
			
			LOGGER.log(Level.INFO, "Committed ");
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, "Commit failed and transaction is being rolled back", e);
			doRollback();
		}
	}

	private void doRollback() {
		if (destinationConnectionList == null) {
			return;
		}

		try {
			for (Connection connection : destinationConnectionList) {
				connection.rollback();
			}
			
			LOGGER.log(Level.INFO, "Rollback ");
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, "Failed to rollback transaction", e);
		}
	}

	private void doCloseConnection() {
		if (destinationConnectionList == null) {
			return;
		}

		try {
			for (Connection connection : destinationConnectionList) {
				if (connection != null) {
					connection.close();
				}
			}
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, "Failed to close connection", e);
		}
	}
}
