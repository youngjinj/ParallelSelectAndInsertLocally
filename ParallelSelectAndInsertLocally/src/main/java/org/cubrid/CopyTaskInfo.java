package org.cubrid;

import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class CopyTaskInfo {
	private String sourceTableName;
	private String sourceIndexFirstColumnName;
	private Connection destinationConnection;
	private String destinationTableName;
	private ConcurrentLinkedQueue<Map<String, Long>> queue;
	private ProgressBarTask progressBar;

	public String getSourceTableName() {
		return sourceTableName;
	}

	public void setSourceTableName(String sourceTableName) {
		this.sourceTableName = sourceTableName;
	}

	public String getSourceIndexFirstColumnName() {
		return sourceIndexFirstColumnName;
	}

	public void setSourceIndexFirstColumnName(String sourceIndexFirstColumnName) {
		this.sourceIndexFirstColumnName = sourceIndexFirstColumnName;
	}

	public Connection getDestinationConnection() {
		return destinationConnection;
	}

	public void setDestinationConnection(Connection destinationConnection) {
		this.destinationConnection = destinationConnection;
	}

	public String getDestinationTableName() {
		return destinationTableName;
	}

	public void setDestinationTableName(String destinationTableName) {
		this.destinationTableName = destinationTableName;
	}

	public ConcurrentLinkedQueue<Map<String, Long>> getQueue() {
		return queue;
	}

	public void setQueue(ConcurrentLinkedQueue<Map<String, Long>> queue) {
		this.queue = queue;
	}

	public ProgressBarTask getProgressBar() {
		return progressBar;
	}

	public void setProgressBar(ProgressBarTask progressBar) {
		this.progressBar = progressBar;
	}
}
