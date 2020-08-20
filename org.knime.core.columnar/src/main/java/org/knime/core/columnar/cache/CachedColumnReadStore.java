package org.knime.core.columnar.cache;

import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_STORE_CLOSED;

import java.io.IOException;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnReadStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.cache.SmallColumnStore.SmallColumnStoreCache;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnSelection;

/**
 * A {@link ColumnReadStore} that reads {@link ColumnData} from a delegate
 * column store and places it in a fixed-size {@link SmallColumnStoreCache LRU
 * cache} in memory for faster subsequent access. The store allows concurrent
 * reading via multiple {@link ColumnDataReader ColumnDataReaders}.
 * 
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class CachedColumnReadStore implements ColumnReadStore {

	public static final class CachedColumnReadStoreCache {

		private final LoadingEvictingCache<ColumnDataUniqueId, ColumnData> m_cache;

		public CachedColumnReadStoreCache(long cacheSize) {
			m_cache = new SizeBoundLruCache<>(cacheSize);
		}

		int size() {
			return m_cache.size();
		}
	}

	private final class CachedColumnStoreReader implements ColumnDataReader {

		private final ColumnSelection m_selection;

		CachedColumnStoreReader(ColumnSelection selection) {
			m_selection = selection;
		}

		@Override
		public ColumnData[] read(int chunkIndex) {
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
			final ColumnData[] data = new ColumnData[numRequested];
			final AtomicReference<ColumnData[]> loadedDataRef = new AtomicReference<>();

			for (int i = 0; i < numRequested; i++) {
				final int colIndex = indices != null ? indices[i] : i;
				final ColumnDataUniqueId ccUID = new ColumnDataUniqueId(colIndex, chunkIndex);

				final Supplier<ColumnData> loader = () -> {
					if (loadedDataRef.get() == null) {
						try (final ColumnDataReader reader = m_delegate.createReader(m_selection)) {
							/**
							 * The m_numChunks and m_maxDataCapacity fields are already set when data is
							 * added to this store via #addBatch. It is set here only for the case where we
							 * are only reading data.
							 */
							m_numChunks.compareAndSet(0, reader.getNumChunks());
							m_maxDataCapacity.compareAndSet(0, reader.getMaxDataCapacity());
							loadedDataRef.compareAndSet(null, reader.read(chunkIndex));
						} catch (Exception e) {
							throw new IllegalStateException("Exception while loading column data.", e);
						}
					}
					m_inCache.put(ccUID, DUMMY);
					final ColumnData[] loadedData = loadedDataRef.get();
					loadedData[colIndex].retain();
					return loadedData[colIndex];
				};
				data[i] = m_cache.retainAndGet(ccUID, loader, m_evictor);
			}
			
			final ColumnData[] loadedData = loadedDataRef.get();
			if (loadedData != null) {
				for (int i = 0; i < numRequested; i++) {
					final int colIndex = indices != null ? indices[i] : i;
					loadedData[colIndex].release();
				}
			}

			return data;
		}

		@Override
		public int getNumChunks() {
			if (m_numChunks.get() == 0) {
				try (final ColumnDataReader reader = m_delegate.createReader(m_selection)) {
					m_numChunks.compareAndSet(0, reader.getNumChunks());
				} catch (Exception e) {
					throw new IllegalStateException("Exception while getting number of chunks from delegate.", e);
				}
			}
			return m_numChunks.get();
		}

		@Override
		public int getMaxDataCapacity() {
			if (m_maxDataCapacity.get() == 0) {
				try (final ColumnDataReader reader = m_delegate.createReader(m_selection)) {
					m_maxDataCapacity.compareAndSet(0, reader.getMaxDataCapacity());
				} catch (Exception e) {
					throw new IllegalStateException("Exception while obtaining max data capacity from delegate.", e);
				}
			}
			return m_maxDataCapacity.get();
		}

		@Override
		public void close() throws IOException {
			// no resources held
		}
	}

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

		@Override
		public String toString() {
			return String.join(",", m_tableStore.toString(), Integer.toString(m_columnIndex),
					Integer.toString(m_chunkIndex));
		}
	}

	private final ColumnReadStore m_delegate;

	private final ColumnStoreSchema m_schema;

	private final AtomicInteger m_numChunks = new AtomicInteger();

	private final AtomicInteger m_maxDataCapacity = new AtomicInteger();

	private final LoadingEvictingCache<ColumnDataUniqueId, ColumnData> m_cache;

	private final BiConsumer<ColumnDataUniqueId, ColumnData> m_evictor;

	private final Map<ColumnDataUniqueId, Object> m_inCache = new ConcurrentHashMap<>();

	private volatile boolean m_storeClosed;

	public CachedColumnReadStore(final ColumnReadStore delegate, final CachedColumnReadStoreCache cache) {
		m_delegate = delegate;
		m_schema = delegate.getSchema();
		m_cache = cache.m_cache;
		m_evictor = (k, c) -> m_inCache.remove(k);
	}

	void addBatch(final ColumnData[] batch) {
		final int numChunks = m_numChunks.getAndIncrement();
		for (int i = 0; i < batch.length; i++) {
			m_maxDataCapacity.compareAndSet(0, batch[i].getMaxCapacity());
			final ColumnDataUniqueId ccUID = new ColumnDataUniqueId(i, numChunks);
			m_inCache.put(ccUID, DUMMY);
			m_cache.retainAndPutIfAbsent(ccUID, batch[i], m_evictor);
		}
	}

	@Override
	public ColumnDataReader createReader(ColumnSelection selection) {
		if (m_storeClosed) {
			throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
		}

		return new CachedColumnStoreReader(selection);
	}

	@Override
	public ColumnStoreSchema getSchema() {
		return m_schema;
	}

	@Override
	public void close() throws IOException {
		for (ColumnDataUniqueId id : m_inCache.keySet()) {
			final ColumnData removed = m_cache.remove(id);
			if (removed != null) {
				removed.release();
			}
		}
		m_inCache.clear();
		m_storeClosed = true;
	}
}
