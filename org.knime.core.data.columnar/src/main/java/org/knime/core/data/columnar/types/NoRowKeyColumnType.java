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
import org.knime.core.columnar.data.VoidData.VoidDataSpec;
import org.knime.core.columnar.data.VoidData.VoidReadData;
import org.knime.core.columnar.data.VoidData.VoidWriteData;
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
public class NoRowKeyColumnType implements ColumnType<VoidWriteData, VoidReadData> {

	@Override
	public ColumnDataSpec getColumnDataSpec() {
		return VoidDataSpec.INSTANCE;
	}

	@Override
	public ReadValue createReadValue(VoidReadData data, IndexSupplier index) {
		return NoRowKeyReadValue.INSTANCE;
	}

	@Override
	public WriteValue<?> createWriteValue(VoidWriteData data, IndexSupplier index) {
		return NoRowKeyWriteValue.INSTANCE;
	}

	private static class NoRowKeyReadValue implements RowKeyReadValue {
		private final static NoRowKeyReadValue INSTANCE = new NoRowKeyReadValue();

		private NoRowKeyReadValue() {
		}

		@Override
		public String getString() {
			return null;
		}
	}

	private static class NoRowKeyWriteValue implements RowKeyWriteValue {

		private final static NoRowKeyWriteValue INSTANCE = new NoRowKeyWriteValue();

		private NoRowKeyWriteValue() {
		}

		@Override
		public void setValue(RowKeyReadValue value) {
		}

		@Override
		public void setRowKey(String key) {
		}

		@Override
		public void setRowKey(RowKeyValue key) {
		}

	}

}