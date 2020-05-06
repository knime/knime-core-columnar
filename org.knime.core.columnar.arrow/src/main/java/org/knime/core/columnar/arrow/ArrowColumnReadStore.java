package org.knime.core.columnar.arrow;

import java.io.File;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.knime.core.columnar.ColumnReadStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnSelection;

class ArrowColumnReadStore implements ColumnReadStore {

	private final BufferAllocator m_allocator;
	private final File m_source;
	private final ColumnStoreSchema m_schema;

	ArrowColumnReadStore(ColumnStoreSchema schema, File source) {
		m_schema = schema;
		m_source = source;
		final RootAllocator root = ArrowColumnStore.ROOT;
		m_allocator = root.newChildAllocator("ArrowColumnReadStore", 0, root.getLimit());
	}

	@Override
	public void close() throws Exception {
		m_allocator.close();
	}

	@Override
	public ColumnDataReader createReader(ColumnSelection config) {
		return new ArrowColumnDataReader(m_schema, m_source, m_allocator, config);
	}

	@Override
	public ColumnStoreSchema getSchema() {
		return m_schema;
	}
}
