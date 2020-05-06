package org.knime.core.columnar.data;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.NullableColumnData;

public interface BinarySupplData<C extends NullableColumnData> extends NullableColumnData {
	C getChunk();

	VarBinaryData getBinarySupplData();

	public static class BinarySupplDataSpec<C extends NullableColumnData>
			implements ColumnDataSpec<BinarySupplData<C>> {
		private final ColumnDataSpec<C> m_spec;

		public BinarySupplDataSpec(final ColumnDataSpec<C> spec) {
			m_spec = spec;
		}

		public ColumnDataSpec<C> getChildSpec() {
			return m_spec;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public Class getColumnDataType() {
			return BinarySupplData.class;
		}
	}
}
