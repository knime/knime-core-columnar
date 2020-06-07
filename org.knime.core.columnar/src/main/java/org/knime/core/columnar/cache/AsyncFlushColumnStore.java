package org.knime.core.columnar.cache;

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnSelection;

//TODO: thread safety considerations
public class AsyncFlushColumnStore implements ColumnStore {

	// TODO: more threads?
	private static final ExecutorService ASYNC_EXECUTOR = Executors.newSingleThreadExecutor(new ThreadFactory() {
		private final AtomicLong m_threadCount = new AtomicLong();

		@Override
		public Thread newThread(final Runnable r) {
			return new Thread(r, "KNIME-BackgroundTableWriter-" + m_threadCount.incrementAndGet());
		}
	});

	private final ColumnStore m_delegate;

	private final ColumnStoreSchema m_schema;

	private final AtomicInteger m_numChunks = new AtomicInteger();

	private final Queue<ColumnData[]> m_unflushed = new ConcurrentLinkedQueue<>();

	private final ColumnDataWriter m_writer = new ColumnDataWriter() {

		@Override
		public void write(ColumnData[] batch) {
			if (m_writerClosed) {
				throw new IllegalStateException("Table store writer has already been closed.");
			}

			for (final ColumnData data : batch) {
				data.retain();
			}
			m_numChunks.incrementAndGet();
			m_unflushed.add(batch);

			if (m_delegateWriter == null) {
				m_delegateWriter = m_delegate.getWriter();
			}

			if (m_asyncFlushFuture != null && m_asyncFlushFuture.isDone()) {
				waitForFutureAndLogExceptions();
				m_asyncFlushFuture = null;
			}

			if (m_asyncFlushFuture == null) {
				m_asyncFlushFuture = ASYNC_EXECUTOR.submit(new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						ColumnData[] previousBatch;
						while ((previousBatch = m_unflushed.poll()) != null) {
//							if (Thread.currentThread().isInterrupted()) {
//								break;
//							}
							m_delegateWriter.write(previousBatch);
							for (final ColumnData data : previousBatch) {
								data.release();
							}
						}
						// TODO: race condition - delegate writer might never be closed
						if (m_writerClosed) {
							m_delegateWriter.close();
						}

						return null;
					}
				});
			}
		}

		@Override
		public void close() throws Exception {
			m_writerClosed = true;

			if (m_asyncFlushFuture != null && m_asyncFlushFuture.isDone()) {
				waitForFutureAndLogExceptions();
				m_asyncFlushFuture = null;
				m_delegateWriter.close();
			}
		}
	};

	// lazily initialized
	private ColumnDataWriter m_delegateWriter;

	private volatile boolean m_writerClosed;

	private volatile boolean m_storeClosed;

	private Future<Void> m_asyncFlushFuture;

	public AsyncFlushColumnStore(final ColumnStore delegate) {
		m_delegate = delegate;
		m_schema = delegate.getSchema();
	}

	void waitForFutureAndLogExceptions() {
		try {
			m_asyncFlushFuture.get();
		} catch (InterruptedException e) {
			// TODO replace with logger and also log exception
			System.err.println("Interrupted while writing cached rows to file.");
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			// TODO replace with logger and also log exception
			System.err.println("Failed to asynchronously write cached rows to file.");
		}
	}

	@Override
	public ColumnDataWriter getWriter() {
		return m_writer;
	}

	@Override
	public void saveToFile(File file) throws IOException {
		if (!m_writerClosed) {
			throw new IllegalStateException("Table store writer has not been closed.");
		}
		if (m_storeClosed) {
			throw new IllegalStateException("Column store has already been closed.");
		}

		if (m_asyncFlushFuture != null) {
			waitForFutureAndLogExceptions();
			m_asyncFlushFuture = null;
		}
		m_delegate.saveToFile(file);
	}

	@Override
	public ColumnDataReader createReader(ColumnSelection config) {
		if (!m_writerClosed) {
			throw new IllegalStateException("Table store writer has not been closed.");
		}
		if (m_storeClosed) {
			throw new IllegalStateException("Column store has already been closed.");
		}

		if (m_asyncFlushFuture != null) {
			waitForFutureAndLogExceptions();
			m_asyncFlushFuture = null;
		}
		return m_delegate.createReader(config);
	}

	@Override
	public ColumnStoreSchema getSchema() {
		return m_schema;
	}

	@Override
	public ColumnDataFactory getFactory() {
		if (m_storeClosed) {
			throw new IllegalStateException("Column store has already been closed.");
		}

		return m_delegate.getFactory();
	}

	@Override
	public void close() throws Exception {
		m_storeClosed = true;
		if (m_asyncFlushFuture != null) {
//			m_asyncFlushFuture.cancel(true);
			waitForFutureAndLogExceptions();
		}
		ColumnData[] batch;
		while ((batch = m_unflushed.poll()) != null) {
			for (final ColumnData data : batch) {
				data.release();
			}
		}
		m_writer.close();
		m_delegate.close();
	}

}
