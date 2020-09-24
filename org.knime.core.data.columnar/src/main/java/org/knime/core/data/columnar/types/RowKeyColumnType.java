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
import org.knime.core.columnar.data.StringData.StringDataSpec;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.VoidData.VoidDataSpec;
import org.knime.core.data.RowKeyConfig;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.columnar.ColumnType;
import org.knime.core.data.columnar.IndexSupplier;
import org.knime.core.data.values.ReadValue;
import org.knime.core.data.values.RowKeyReadValue;
import org.knime.core.data.values.RowKeyWriteValue;
import org.knime.core.data.values.WriteValue;

/**
 * TODO can be implemented differently later.
 *
 * @author Christian Dietz
 */
public class RowKeyColumnType implements ColumnType<StringWriteData, StringReadData> {

	private RowKeyConfig m_config;

	// TODO
	public RowKeyColumnType(RowKeyConfig config) {
		m_config = config;
	}

	@Override
	public ColumnDataSpec getColumnDataSpec() {
		switch (m_config) {
		case CUSTOM:
			return StringDataSpec.DICT_DISABLED;
		case NOKEY:
			return VoidDataSpec.INSTANCE;
		default:
			throw new IllegalArgumentException("Unknown or unsupported RowKeyConfig.");
		}
	}

	@Override
	public ReadValue createReadValue(StringReadData data, IndexSupplier index) {
		return new CustomRowKeyReadValue(data, index);
	}

	@Override
	public WriteValue<?> createWriteValue(StringWriteData data, IndexSupplier index) {
		return new CustomRowKeyWriteValue(data, index);
	}

	private static class CustomRowKeyReadValue implements RowKeyReadValue {

		private final IndexSupplier m_index;
		private final StringReadData m_data;

		private CustomRowKeyReadValue(StringReadData data, IndexSupplier index) {
			m_data = data;
			m_index = index;
		}

		@Override
		public boolean equals(Object obj) {
			return super.equals(obj);
		}

		@Override
		public String getString() {
			return m_data.getString(m_index.getIndex());
		}
	}

	private static class CustomRowKeyWriteValue implements RowKeyWriteValue {

		private final IndexSupplier m_index;
		private final StringWriteData m_data;

		private CustomRowKeyWriteValue(StringWriteData data, IndexSupplier index) {
			m_data = data;
			m_index = index;
		}

		@Override
		public void setValue(RowKeyReadValue value) {
			m_data.setString(m_index.getIndex(), value.getString());
		}

		@Override
		public void setRowKey(String key) {
			m_data.setString(m_index.getIndex(), key);
		}

		@Override
		public void setRowKey(RowKeyValue key) {
			m_data.setString(m_index.getIndex(), key.getString());
		}

		@Override
		public boolean equals(Object obj) {
			return super.equals(obj);
		}
	}

}