package org.knime.core.columnar;

import org.knime.core.columnar.cache.CachedColumnReadStore;
import org.knime.core.columnar.cache.CachedColumnStore;

public class ColumnStoreUtils {

	public static CachedColumnStore cache(final ColumnStore store) {
		return new CachedColumnStore(store);
	}

	public static CachedColumnReadStore cache(final ColumnReadStore store) {
		return new CachedColumnReadStore(store);
	}

}
