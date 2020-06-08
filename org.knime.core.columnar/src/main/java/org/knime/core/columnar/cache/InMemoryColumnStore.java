package org.knime.core.columnar.cache;

import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_STORE_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_WRITER_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_WRITER_NOT_CLOSED;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnSelection;

/**
 * A {@link ColumnStore} that holds {@link ColumnData} retained in memory until
 * closed. The store allows concurrent reading via multiple
 * {@link ColumnDataReader ColumnDataReaders} once the {@link ColumnDataWriter}
 * has been closed.
 * 
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public class InMemoryColumnStore implements ColumnStore, ReferencedData {

	private final ColumnStoreSchema m_schema;

	private final List<ColumnData[]> m_batches = new ArrayList<>();

	private final class InMemoryColumnStoreWriter implements ColumnDataWriter {

		@Override
		public void write(ColumnData[] batch) throws IOException {
			if (m_writerClosed) {
				throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
			}
			if (m_storeClosed) {
				throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
			}

			for (ColumnData data : batch) {
				data.retain();
				m_sizeOf += data.sizeOf();
			}
			m_batches.add(batch);
		}

		@Override
		public void close() throws Exception {
			m_writerClosed = true;
		}
	}

	private final class InMemoryColumnStoreReader implements ColumnDataReader {

		private final ColumnSelection m_selection;

		InMemoryColumnStoreReader(ColumnSelection selection) {
			m_selection = selection;
		}

		@Override
		public ColumnData[] read(int index) throws IOException {
			if (m_storeClosed) {
				throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
			}

			final int[] indices;
			if (m_selection != null) {
				indices = m_selection.get();
			} else {
				indices = null;
			}

			final int numRequested = indices != null ? indices.length : m_schema.getNumColumns();
			final ColumnData[] batch = new ColumnData[numRequested];

			for (int i = 0; i < numRequested; i++) {
				final ColumnData data = m_batches.get(index)[indices != null ? indices[i] : i];
				data.retain();
				batch[i] = data;
			}

			return batch;
		}

		@Override
		public int getNumChunks() {
			return m_batches.size();
		}

		@Override
		public int getMaxDataCapacity() {
			if (m_batches.isEmpty()) {
				return 0;
			}
			final ColumnData[] firstBatch = m_batches.get(0);
			if (firstBatch.length == 0) {
				return 0;
			}
			return firstBatch[0].getMaxCapacity();
		}

		@Override
		public void close() throws Exception {
			// no resources held
		}
	}

	private final ColumnDataWriter m_writer;

	private volatile boolean m_writerClosed;

	private volatile boolean m_storeClosed;

	private int m_sizeOf = 0;

	InMemoryColumnStore(ColumnStoreSchema schema) {
		m_schema = schema;
		m_writer = new InMemoryColumnStoreWriter();
	}

	@Override
	public void release() {
		for (ColumnData[] batch : m_batches) {
			for (ColumnData data : batch) {
				data.release();
			}
		}
	}

	@Override
	public void retain() {
		for (ColumnData[] batch : m_batches) {
			for (ColumnData data : batch) {
				data.retain();
			}
		}
	}

	@Override
	public int sizeOf() {
		return m_sizeOf;
	}

	@Override
	public ColumnDataWriter getWriter() {
		return m_writer;
	}

	@Override
	public void saveToFile(File f) throws IOException {
		if (!m_writerClosed) {
			throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
		}
		if (m_storeClosed) {
			throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
		}

		throw new UnsupportedOperationException("Saving to file not supported by in-memory column store.");
	}

	@Override
	public ColumnDataReader createReader(ColumnSelection selection) {
		if (!m_writerClosed) {
			throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
		}
		if (m_storeClosed) {
			throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
		}

		return new InMemoryColumnStoreReader(selection);
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

		throw new UnsupportedOperationException("Creating new ColumnData not supported by in-memory column store.");
	}

	@Override
	public void close() throws Exception {
		if (!m_storeClosed) {
			release();
		}
		m_storeClosed = true;
	}
}
