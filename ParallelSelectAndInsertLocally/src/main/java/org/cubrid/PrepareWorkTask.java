package org.cubrid;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PrepareWorkTask implements Callable<Void> {
	private static final Logger LOGGER = Logger.getLogger(PrepareWorkTask.class.getName());

	public static final String LABEL_OFFSET = "offset";
	public static final String LABEL_ROW_COUNT = "rowCount";

	private long rowCount;
	private long batchCount;
	private ConcurrentLinkedQueue<Map<String, Long>> queue;

	public PrepareWorkTask(PrepareWorkTaskInfo prepareTaskInfo) {
		this.rowCount = prepareTaskInfo.getRowCount();
		this.batchCount = prepareTaskInfo.getBatchCount();
		this.queue = prepareTaskInfo.getQueue();
	}

	@Override
	public Void call() {
		LOGGER.log(Level.INFO, String.format("[%s] Starting prepare task", Thread.currentThread().getName()));

		long nextOffset = 0L;

		while (!Thread.interrupted()) {
			Map<String, Long> work = new HashMap<String, Long>();
			work.put(LABEL_OFFSET, nextOffset);
			work.put(LABEL_ROW_COUNT, batchCount);

			queue.offer(work);

			nextOffset = nextOffset + batchCount;
			if (nextOffset >= rowCount) {
				break;
			}
		}

		return null;
	}
}
