package org.knime.core.columnar.arrow;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.BeforeClass;
import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.ColumnStoreUtils;
import org.knime.core.columnar.chunk.ColumnSelection;

public class AbstractArrowTest {

	private static ArrowColumnStoreFactory m_factory;

	@BeforeClass
	public static void startup() {
		m_factory = new ArrowColumnStoreFactory();
	}

	public ColumnStoreSchema createWideSchema(ColumnDataSpec<?> type, int width) {
		final ColumnDataSpec<?>[] types = new ColumnDataSpec<?>[width];
		for (int i = 0; i < width; i++) {
			types[i] = type;
		}
		return createSchema(types);
	}

	public ColumnStoreSchema createSchema(final ColumnDataSpec<?>... types) {
		return new ColumnStoreSchema() {

			@Override
			public int getNumColumns() {
				return types.length;
			}

			@Override
			public ColumnDataSpec<?> getColumnDataSpec(int index) {
				return types[index];
			}
		};
	}

	public ColumnSelection createSelection(int... selectedIndidces) {
		return selectedIndidces.length > 0 ? new ColumnSelection() {

			@Override
			public int[] get() {
				return selectedIndidces;
			}
		} : null;
	}

	public ColumnStore createStore(int chunkSize, ColumnStoreSchema schema) throws IOException {
		return m_factory.createWriteStore(schema, createTmpFile(), chunkSize);
	}

	public ColumnStore cache(ColumnStore store) {
		return ColumnStoreUtils.cache(store);
	}

	public ColumnStore createStore(int chunkSize, ColumnDataSpec<?>... types) throws IOException {
		return createStore(chunkSize, new ColumnStoreSchema() {

			@Override
			public int getNumColumns() {
				return types.length;
			}

			@Override
			public ColumnDataSpec<?> getColumnDataSpec(int idx) {
				return types[idx];
			}
		});
	}

	@SuppressWarnings("resource")
	public BufferAllocator newAllocator() {
		// Root closed by factory.
		return new RootAllocator().newChildAllocator("Test", 0, Long.MAX_VALUE);
	}

	// also deletes file on exit
	public File createTmpFile() throws IOException {
		// file
		final File f = Files.createTempFile("KNIME-" + UUID.randomUUID().toString(), ".knarrow").toFile();
		f.deleteOnExit();
		return f;
	}
}
