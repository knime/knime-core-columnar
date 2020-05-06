package org.knime.core.columnar.data;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.NullableColumnData;

public interface StructColumnData extends NullableColumnData {

	NullableColumnData getChunk(int index);

	public class StructDataSpec implements ColumnDataSpec<StructColumnData> {

		private ColumnDataSpec<?>[] m_childSpecs;

		public StructDataSpec(ColumnDataSpec<?>... childSpecs) {
			m_childSpecs = childSpecs;
		}

		@Override
		public Class<? extends StructColumnData> getColumnDataType() {
			return StructColumnData.class;
		}

		public ColumnDataSpec<?>[] getChildChunkSpecs() {
			return m_childSpecs;
		}
	}
}