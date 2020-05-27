package org.knime.core.columnar.cache;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnReadStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnReaderConfig;

//TODO: thread safety considerations
public class CachedColumnReadStore implements ColumnReadStore {

	private static final Object DUMMY = new Object();

	private class ColumnDataUniqueId {

		private final CachedColumnReadStore m_tableStore;

		private final int m_columnIndex;

		private final int m_chunkIndex;

		private ColumnDataUniqueId(final int columnIndex, final int chunkIndex) {
			m_tableStore = CachedColumnReadStore.this;
			m_columnIndex = columnIndex;
			m_chunkIndex = chunkIndex;
		}

		@Override
		public int hashCode() {
			int result = 17;
			result = 31 * result + m_tableStore.hashCode();
			result = 31 * result + m_columnIndex;
			result = 31 * result + m_chunkIndex;
			return result;
		}

		@Override
		public boolean equals(Object object) {
			if (object == this)
				return true;
			if (!(object instanceof ColumnDataUniqueId)) {
				return false;
			}
			final ColumnDataUniqueId other = (ColumnDataUniqueId) object;
			return Objects.equals(m_tableStore, other.m_tableStore) && m_columnIndex == other.m_columnIndex
					&& m_chunkIndex == other.m_chunkIndex;
		}

		int getColumnIndex() {
			return m_columnIndex;
		}

		int getChunkIndex() {
			return m_chunkIndex;
		}

		@Override
		public String toString() {
			return String.join(",", m_tableStore.toString(), Integer.toString(m_columnIndex),
					Integer.toString(m_chunkIndex));
		}
	}

	private final ColumnReadStore m_delegate;

	private final ColumnStoreSchema m_schema;

	private final AtomicInteger m_numChunks = new AtomicInteger();
	
	private final LoadingEvictingChunkCache<ColumnDataUniqueId, ColumnData> m_cache;

	private final Function<ColumnDataUniqueId, ColumnData> m_loader;

	private final BiConsumer<ColumnDataUniqueId, ColumnData> m_evictor;

	private final Map<ColumnDataUniqueId, Object> m_inCache = new ConcurrentHashMap<>();
	
	public CachedColumnReadStore(final ColumnReadStore delegate, final int cacheSize) {
		m_delegate = delegate;
		m_schema = delegate.getSchema();
		m_cache = new SizeBoundLruCache<>(cacheSize);
		
		// TODO: reading column chunks one by one is too expensive
		m_loader = id -> {
			m_inCache.put(id, DUMMY);
			ColumnReaderConfig config = new ColumnReaderConfig() {
				@Override
				public int[] getColumnIndices() {
					return new int[] { id.getColumnIndex() };
				}
			};
			try (final ColumnDataReader reader = m_delegate.createReader(config)) {
				m_numChunks.compareAndSet(0, reader.getNumEntries());
				return reader.read(id.getChunkIndex())[0];
			} catch (Exception e) {
				// TODO: handle exception properly
				throw new RuntimeException("Exception while loading column chunk.", e);
			}
		};
		
		m_evictor = (k, c) -> m_inCache.remove(k);
	}

	void addBatch(final ColumnData[] batch) {
		final int numChunks = m_numChunks.getAndIncrement();
		for (int i = 0; i < batch.length; i++) {
			final ColumnDataUniqueId ccUID = new ColumnDataUniqueId(i, numChunks);
			m_cache.retainAndPutIfAbsent(ccUID, batch[i], m_evictor);
		}
	}

	@Override
	public ColumnDataReader createReader(ColumnReaderConfig config) {

		// TODO: pre-fetch subsequent chunks on cache miss
		return new ColumnDataReader() {
			@Override
			public int getNumEntries() {
				return m_numChunks.get();
			}

			@Override
			public ColumnData[] read(int chunkIndex) {
				final int[] indices = config.getColumnIndices();
				final boolean isSelection = indices != null;
				final int numRequested = isSelection ? indices.length : m_schema.getNumColumns();
				final ColumnData[] data = new ColumnData[numRequested];

				for (int i = 0; i < numRequested; i++) {
					final ColumnDataUniqueId ccUID = new ColumnDataUniqueId(isSelection ? indices[i] : i, chunkIndex);
					data[i] = m_cache.retainAndGet(ccUID, m_loader, m_evictor);
				}

				return data;
			}

			@Override
			public void close() throws Exception {
			}
		};
	}

	@Override
	public ColumnStoreSchema getSchema() {
		return m_schema;
	}

	@Override
	public void close() throws Exception {
		for (ColumnDataUniqueId id : m_inCache.keySet()) {
			final ColumnData removed = m_cache.remove(id);
			if (removed != null) {
				removed.release();
			}
		}
		m_inCache.clear();
	}

}
