package org.knime.core.columnar.cache;

import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_STORE_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_WRITER_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_WRITER_NOT_CLOSED;

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnSelection;

/**
 * A {@link ColumnStore} that asynchronously passes {@link ColumnData} on to a
 * delegate column store. It waits for the termination of this asynchronous
 * write process when data is read. The store allows concurrent reading via
 * multiple {@link ColumnDataReader ColumnDataReaders} once the
 * {@link ColumnDataWriter} has been closed.
 * 
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public class AsyncFlushColumnStore implements ColumnStore {

	public static final class AsyncFlushColumnStoreExecutor {

		private static final AtomicLong THREAD_COUNT = new AtomicLong();

		private final Semaphore m_semaphore;

		private final ExecutorService m_executor;

		public AsyncFlushColumnStoreExecutor(int queueSize) {
			m_semaphore = new Semaphore(queueSize);
			m_executor = Executors.newSingleThreadExecutor(
					r -> new Thread(r, "KNIME-BackgroundTableWriter-" + THREAD_COUNT.incrementAndGet()));
		}

		private Future<Void> submit(final Callable<Void> command) throws InterruptedException {
			m_semaphore.acquire();
			try {
				return m_executor.submit(() -> {
					try {
						return command.call();
					} finally {
						m_semaphore.release();
					}
				});
			} catch (RejectedExecutionException e) {
				m_semaphore.release();
				throw e;
			}
		}
	}

	private final class AsyncFlushColumnStoreWriter implements ColumnDataWriter {

		@Override
		public void write(ColumnData[] batch) {
			if (m_writerClosed) {
				throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
			}

			for (final ColumnData data : batch) {
				data.retain();
			}
			m_numChunks.incrementAndGet();

			if (m_delegateWriter == null) {
				m_delegateWriter = m_delegate.getWriter();
			}

			handleDoneFutures();

			try {
				m_futures.add(m_executor.submit(new Callable<Void>() {
					@Override
					public Void call() throws IOException {

						m_delegateWriter.write(batch);
						for (final ColumnData data : batch) {
							data.release();
						}
						return null;
					}
				}));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		@Override
		public void close() throws IOException {
			m_writerClosed = true;

			handleDoneFutures();

			if (m_delegateWriter != null) {
				if (m_futures.isEmpty()) {
					m_delegateWriter.close();
				} else {
					try {
                        m_futures.add(m_executor.submit(new Callable<Void>() {
                        	@Override
                        	public Void call() throws Exception {

                        		m_delegateWriter.close();
                        		return null;
                        	}
                        }));
                    } catch (InterruptedException ex) {
                        // TODO Auto-generated catch block
                    }
				}
			}
		}

		private void handleDoneFutures() {
			while (!m_futures.isEmpty()) {
				final Future<Void> future = m_futures.peek();
				if (future.isDone()) {
					waitForAndHandleFuture(future);
					// we do not have to concern ourselves with thread safety here:
					// there is only a single thread calling this method
					m_futures.remove();
				} else {
					break;
				}
			}
		}
	}

	private final AsyncFlushColumnStoreExecutor m_executor;

	private final ColumnStore m_delegate;

	private final ColumnStoreSchema m_schema;

	private final AtomicInteger m_numChunks = new AtomicInteger();

	private final Queue<Future<Void>> m_futures = new ConcurrentLinkedQueue<>();

	private final ColumnDataWriter m_writer;

	// lazily initialized
	private ColumnDataWriter m_delegateWriter;

	private volatile boolean m_writerClosed;

	private volatile boolean m_storeClosed;

	public AsyncFlushColumnStore(final ColumnStore delegate, final AsyncFlushColumnStoreExecutor executor) {
		m_delegate = delegate;
		m_schema = delegate.getSchema();
		m_executor = executor;
		m_writer = new AsyncFlushColumnStoreWriter();
	}

	void waitForAndHandleFutures() {
		Future<Void> future;
		while ((future = m_futures.poll()) != null) {
			waitForAndHandleFuture(future);
		}
	}

	private void waitForAndHandleFuture(Future<Void> future) {
		try {
			future.get();
		} catch (InterruptedException e) {
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			throw new IllegalStateException("Failed to asynchronously write cached rows to file.", e);
		}
	}

	@Override
	public ColumnDataWriter getWriter() {
		return m_writer;
	}

	@Override
	public void saveToFile(File file) throws IOException {
		if (!m_writerClosed) {
			throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
		}
		if (m_storeClosed) {
			throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
		}

		waitForAndHandleFutures();
		m_delegate.saveToFile(file);
	}

	@Override
	public ColumnDataReader createReader(ColumnSelection selection) {
		if (!m_writerClosed) {
			throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
		}
		if (m_storeClosed) {
			throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
		}

		waitForAndHandleFutures();
		return m_delegate.createReader(selection);
	}

	@Override
	public ColumnStoreSchema getSchema() {
		return m_schema;
	}

	@Override
	public ColumnDataFactory getFactory() {
		if (m_storeClosed) {
			throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
		}

		return m_delegate.getFactory();
	}

	@Override
	public void close() throws IOException {
		m_storeClosed = true;
		waitForAndHandleFutures();
		m_writer.close();
		m_delegate.close();
	}
}
