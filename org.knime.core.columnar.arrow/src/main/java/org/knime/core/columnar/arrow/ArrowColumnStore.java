package org.knime.core.columnar.arrow;

import java.io.File;
import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnSelection;

import com.google.common.io.Files;

final class ArrowColumnStore implements ColumnStore {

	// TODO allow configurations of root allocator
	final static RootAllocator ROOT = new RootAllocator();
	final static String CFG_ARROW_CHUNK_SIZE = "CHUNK_SIZE";

	private final File m_file;
	private final BufferAllocator m_allocator;
	private final ColumnStoreSchema m_schema;
	private final ArrowColumnReadStore m_delegate;
	private final int m_chunkSize;
	private final ArrowColumnDataSpec<?>[] m_arrowSchema;

	ArrowColumnStore(ColumnStoreSchema schema, ArrowSchemaMapper mapper, File file, final int chunkSize) {
		m_file = file;
		m_schema = schema;
		m_arrowSchema = mapper.map(schema);
		m_chunkSize = chunkSize;
		// TODO make configurable
		m_allocator = ROOT.newChildAllocator("ArrowColumnStore", 0, ROOT.getLimit());
		m_delegate = new ArrowColumnReadStore(schema, file);
	}

	@Override
	public void close() throws Exception {
		m_delegate.close();
		m_file.delete();
		m_allocator.close();
	}

	@Override
	public ColumnDataWriter getWriter() {
		// TODO also write mapper type information for backwards-compatibility. Readers
		// can first get the mapper type from metadata and instantiate a mapper
		// themselves.
		return new ArrowColumnDataWriter(m_file, m_allocator, m_chunkSize);
	}

	@Override
	public ColumnDataReader createReader(ColumnSelection config) {
		return m_delegate.createReader(config);
	}

	@Override
	public ColumnDataFactory getFactory() {
		return new ColumnDataFactory() {
			@Override
			public ColumnData[] create() {
				final ColumnData[] chunk = new ColumnData[m_arrowSchema.length];
				for (int i = 0; i < m_arrowSchema.length; i++) {
					chunk[i] = m_arrowSchema[i].createEmpty(m_allocator);
					chunk[i].ensureCapacity(m_chunkSize);
				}
				return chunk;
			}
		};
	}

	@Override
	public ColumnStoreSchema getSchema() {
		return m_schema;
	}

	@Override
	public void saveToFile(File f) throws IOException {
		Files.copy(m_file, f);
	}
}
