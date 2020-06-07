package org.knime.core.columnar.chunk;

public final class ColumnSelectionUtil {

	private ColumnSelectionUtil() {
	}

	public static ColumnSelection create(int[] indices) {
		return new ColumnSelection() {

			@Override
			public int[] get() {
				return indices;
			}
		};
	}
}
