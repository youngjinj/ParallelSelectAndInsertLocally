package org.cubrid;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PrepareWorkTaskInfo {
	private long rowCount;
	private long batchCount;
	private ConcurrentLinkedQueue<Map<String, Long>> queue;
	
	public long getRowCount() {
		return rowCount;
	}

	public void setRowCount(long rowCount) {
		this.rowCount = rowCount;
	}

	public long getBatchCount() {
		return batchCount;
	}

	public void setBatchCount(long batchCount) {
		this.batchCount = batchCount;
	}

	public ConcurrentLinkedQueue<Map<String, Long>> getQueue() {
		return queue;
	}

	public void setQueue(ConcurrentLinkedQueue<Map<String, Long>> queue) {
		this.queue = queue;
	}
}
