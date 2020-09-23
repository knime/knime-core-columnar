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

import org.knime.core.columnar.data.BooleanData;
import org.knime.core.columnar.data.BooleanData.BooleanReadData;
import org.knime.core.columnar.data.BooleanData.BooleanWriteData;
import org.knime.core.columnar.data.ColumnDataSpec;
import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataCell;
import org.knime.core.data.columnar.ColumnType;
import org.knime.core.data.columnar.IndexSupplier;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.values.BooleanReadValue;
import org.knime.core.data.values.BooleanWriteValue;

public final class BooleanColumnType implements ColumnType<BooleanWriteData, BooleanReadData> {

	@Override
	public ColumnDataSpec getColumnDataSpec() {
		return BooleanData.BooleanDataSpec.INSTANCE;
	}

	@Override
	public BooleanReadValue createReadValue(BooleanReadData data, IndexSupplier index) {
		return new DefaultBooleanReadValue(data, index);
	}

	@Override
	public BooleanWriteValue createWriteValue(BooleanWriteData data, IndexSupplier index) {
		return new DefaultBooleanWriteValue(data, index);
	}

	private static final class DefaultBooleanReadValue implements //
			BooleanReadValue {

		private final IndexSupplier m_index;
		private final BooleanReadData m_data;

		DefaultBooleanReadValue(BooleanReadData data, IndexSupplier index) {
			m_data = data;
			m_index = index;
		}

		@Override
		public boolean getBooleanValue() {
			return m_data.getBoolean(m_index.getIndex());
		}

		@Override
		public double getDoubleValue() {
			return m_data.getBoolean(m_index.getIndex()) ? 1 : 0;
		}

		@Override
		public long getLongValue() {
			return m_data.getBoolean(m_index.getIndex()) ? 1 : 0;
		}

		@Override
		public int getIntValue() {
			return m_data.getBoolean(m_index.getIndex()) ? 1 : 0;
		}

		@Override
		public DataCell getDataCell() {
			return getBooleanValue() ? BooleanCell.TRUE : BooleanCell.FALSE;
		}
	}

	private static final class DefaultBooleanWriteValue implements //
			BooleanWriteValue {

		private final IndexSupplier m_index;
		private final BooleanWriteData m_data;

		private DefaultBooleanWriteValue(BooleanWriteData data, IndexSupplier index) {
			m_data = data;
			m_index = index;
		}

		@Override
		public void setBooleanValue(final boolean value) {
			m_data.setBoolean(m_index.getIndex(), value);
		}

		@Override
		public void setValue(final BooleanValue value) {
			m_data.setBoolean(m_index.getIndex(), value.getBooleanValue());
		}
	}
}
