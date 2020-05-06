package org.knime.core.columnar.cache;

import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.NullableColumnData;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnReaderConfig;

/*
 TODO interface for cache
 TODO async pre-load here or in the actual cursor?
 TODO thread-safety
*/
public class CachedColumnStore implements ColumnStore, Flushable {

	// one cache for each column. use-case: two tables with different filters access
	// same table.
	private List<Map<Integer, ColumnData>> m_caches;

	// all types
	private ColumnStoreSchema m_types;

	// writer
	private ColumnDataWriter m_writer;

	// size of cache
	private int m_numChunks = 0;
	private int m_flushIndex = 0;

	private ColumnStore m_delegate;
	private CachedColumnReadStore m_readCache;
	private boolean m_finishedWriting = false;

	public CachedColumnStore(final ColumnStore delegate) {
		m_types = delegate.getSchema();
		m_delegate = delegate;
		m_caches = new ArrayList<>();
		for (int i = 0; i < m_types.getNumColumns(); i++) {
			m_caches.add(new TreeMap<Integer, ColumnData>());
		}

		// read store.
		m_readCache = new CachedColumnReadStore(delegate, m_caches);

		// Only one writer. Maybe a property of the delegate
		m_writer = delegate.getWriter();
	}

	@Override
	public ColumnDataWriter getWriter() {
		// TODO make sure we only have one writer for now.
		// Write each column individually in the corresponding cache
		return new ColumnDataWriter() {

			@Override
			public void write(final ColumnData[] data) {
				for (int i = 0; i < m_types.getNumColumns(); i++) {
					data[i].retain();
					m_caches.get(i).put(m_numChunks, data[i]);
				}
				m_numChunks++;
				m_readCache.incNumChunks();
			}

			@Override
			public void close() throws Exception {
				m_finishedWriting = true;
			}
		};
	}

	/**
	 * BIG TODO: implement flush differently for read-only scenario!!!!
	 */
	@Override
	public void flush() throws IOException {
		final ColumnData[] data = new NullableColumnData[m_caches.size()];
		// TODO always flush fully?
		// TODO for async we have to refactor the while-loop
		for (; m_flushIndex < m_numChunks; m_flushIndex++) {
			for (int i = 0; i < m_caches.size(); i++) {
				final Map<Integer, ColumnData> cache = m_caches.get(i);
				data[i] = cache.get(m_flushIndex);
			}
			// TODO reuse existing record object to avoid re-creation. later.
			m_writer.write(data);

			// for each column cache
			for (int i = 0; i < data.length; i++) {
				data[i].release();
			}
		}
		// for each column cache
		for (int i = 0; i < data.length; i++) {
			m_caches.get(i).clear();
		}
		if (m_flushIndex == m_numChunks & m_finishedWriting) {
			try {
				m_writer.close();
			} catch (Exception e) {
				// TODO OK?
				throw new IOException(e);
			}
		}
		// NB: keep caches open.
	}

	// Move to CachedRecordReadStore. Make sure caches are 'lazily' instantiated in
	// case of read access.
	@Override
	public ColumnDataReader createReader(ColumnReaderConfig config) {
		return m_readCache.createReader(config);
	}

	@Override
	public ColumnStoreSchema getSchema() {
		return m_types;
	}

	@Override
	public void close() throws Exception {
		m_readCache.clear();
		m_delegate.close();
	}

	@Override
	public ColumnDataFactory getFactory() {
		return m_delegate.getFactory();
	}

	@Override
	public void saveToFile(File f) throws IOException {
		flush();
		m_delegate.saveToFile(f);
	}
}
