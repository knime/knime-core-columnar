package org.knime.core.columnar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnSelection;

public class TestColumnStore implements ColumnStore {

	private final int m_maxDataCapacity;
	private final ColumnStoreSchema m_schema;

	public List<Double[][]> m_chunks;

	public TestColumnStore(ColumnStoreSchema schema, int maxDataCapacity) {
		m_schema = schema;
		m_maxDataCapacity = maxDataCapacity;
		m_chunks = new ArrayList<Double[][]>();
	}

	@Override
	public ColumnDataWriter getWriter() {
		return new TestColumnDataWriter(m_chunks);
	}

	@Override
	public ColumnDataFactory getFactory() {
		return new TestColumnDataFactory(m_schema.getNumColumns(), m_maxDataCapacity);
	}

	@Override
	public void saveToFile(File f) throws IOException {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public ColumnDataReader createReader(ColumnSelection config) {
		return new TestColumnDataReader(m_chunks, m_maxDataCapacity);
	}

	@Override
	public void close() throws Exception {
		m_chunks = null;
	}

	@Override
	public ColumnStoreSchema getSchema() {
		return m_schema;
	}

}
