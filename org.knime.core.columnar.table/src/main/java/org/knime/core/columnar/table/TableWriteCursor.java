package org.knime.core.columnar.table;

import java.io.IOException;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataWriter;

//TODO similar logic required later for columnar access...
//TODO interface cursor, interface 'get', 'getRange' (late 'getSelection').
public class TableWriteCursor implements AutoCloseable {

	private final ColumnDataWriter m_writer;
	private final ColumnDataFactory m_factory;
	private final ColumnDataAccess<ColumnData>[] m_access;

	private ColumnData[] m_currentData;
	private long m_currentDataMaxIndex;
	private int m_index = -1;

	public TableWriteCursor(final ColumnDataFactory factory, final ColumnDataWriter writer,
			final ColumnDataAccess<ColumnData>[] access) {
		m_writer = writer;
		m_factory = factory;
		m_access = access;

		switchToNextData();
	}

	public void fwd() {
		if (++m_index > m_currentDataMaxIndex) {
			switchToNextData();
			m_index = 0;
		}
		for (int i = 0; i < m_access.length; i++) {
			m_access[i].fwd();
		}
	}

	public <W extends WriteValue> W get(int index) {
		@SuppressWarnings("unchecked")
		final W value = (W) m_access[index];
		return value;
	}

	@Override
	public void close() throws Exception {
		releaseCurrentData(m_index + 1);
		m_writer.close();
	}

	private void switchToNextData() {
		releaseCurrentData(m_index);
		m_currentData = m_factory.create();
		for (int i = 0; i < m_access.length; i++) {
			m_access[i].load(m_currentData[i]);
		}
		m_currentDataMaxIndex = m_currentData[0].getMaxCapacity() - 1;
	}

	private void releaseCurrentData(int numValues) {
		if (m_currentData != null) {
			for (final ColumnData data : m_currentData) {
				data.setNumValues(numValues);
			}
			try {
				m_writer.write(m_currentData);
			} catch (IOException e) {
				// TODO
				throw new RuntimeException("TODO", e);
			}
			for (ColumnData data : m_currentData) {
				data.release();
			}
		}
	}
}