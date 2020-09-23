/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 */

package org.knime.core.data.columnar.types;

import org.knime.core.columnar.data.ColumnDataSpec;
import org.knime.core.columnar.data.LongData;
import org.knime.core.columnar.data.LongData.LongReadData;
import org.knime.core.columnar.data.LongData.LongWriteData;
import org.knime.core.data.DataCell;
import org.knime.core.data.LongValue;
import org.knime.core.data.columnar.ColumnType;
import org.knime.core.data.columnar.IndexSupplier;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.values.LongReadValue;
import org.knime.core.data.values.LongWriteValue;

public final class LongColumnType implements ColumnType<LongWriteData, LongReadData> {

	public static final LongColumnType INSTANCE = new LongColumnType();

	private LongColumnType() {
	}

	@Override
	public ColumnDataSpec getColumnDataSpec() {
		return LongData.LongDataSpec.INSTANCE;
	}

	@Override
	public LongReadValue createReadValue(LongReadData data, IndexSupplier index) {
		return new DefaultLongReadValue(data, index);
	}

	@Override
	public LongWriteValue createWriteValue(LongWriteData data, IndexSupplier index) {
		return new DefaultLongWriteValue(data, index);
	}



	private static final class DefaultLongReadValue implements LongReadValue {

		private final IndexSupplier m_index;
		private final LongReadData m_data;

		DefaultLongReadValue(LongReadData data, IndexSupplier index) {
			m_data = data;
			m_index = index;
		}

		@Override
		public long getLongValue() {
			return m_data.getLong(m_index.getIndex());
		}

		@Override
		public double getDoubleValue() {
			return m_data.getLong(m_index.getIndex());
		}

		@Override
		public DataCell getDataCell() {
			return new LongCell(m_data.getLong(m_index.getIndex()));
		}
	}

	private static final class DefaultLongWriteValue implements LongWriteValue {

		private final IndexSupplier m_index;
		private final LongWriteData m_data;

		DefaultLongWriteValue(LongWriteData data, IndexSupplier index) {
			m_data = data;
			m_index = index;
		}

		@Override
		public void setLongValue(final long value) {
			m_data.setLong(m_index.getIndex(), value);
		}

		@Override
		public void setValue(final LongValue value) {
			m_data.setLong(m_index.getIndex(), value.getLongValue());
		}
	}
}
