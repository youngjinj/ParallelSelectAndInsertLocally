package org.cubrid;

import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProgressBarTask implements Callable<Void> {
	private static final Logger LOGGER = Logger.getLogger(ProgressBarTask.class.getName());

	private long progress;
	private long total;
	private boolean isTotal;
	private boolean isFinished;

	public ProgressBarTask() {
		this.progress = 0L;
		this.total = 0L;
		this.isTotal = false;
		this.isFinished = false;
	}

	@Override
	public Void call() {
		while (total == 0L) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				return null;
			}
		}

		while (!Thread.currentThread().isInterrupted()) {
			if (isTotal) {
				isFinished = true;
			}

			LOGGER.log(Level.INFO, String.format("Progress: %s %s/%s (-%s)", getProgressBar(progress, total), progress,
					total, (total - progress)));

			if (isFinished) {
				break;
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				return null;
			}
		}

		return null;
	}

	private String getProgressBar(long progress, long total) {
		StringBuilder progressBar = new StringBuilder();

		int progressPercentage = (int) (((double) progress / (double) total) * 100);
		int numberOfBars = progressPercentage / 5;

		progressBar.append(String.format("%3s", progressPercentage)).append("%").append(" ").append("[");

		for (int i = 0; i < numberOfBars; i++) {
			progressBar.append("=");
		}

		for (int i = 0; i < 20 - numberOfBars; i++) {
			progressBar.append(" ");
		}

		progressBar.append("]");

		return progressBar.toString();
	}

	public synchronized void updateProgress(int progress) {
		this.progress += progress;
		isTotal = (this.progress == total) ? true : false;
	}

	public synchronized void setTotal(long total) {
		this.total = total;
	}
}
