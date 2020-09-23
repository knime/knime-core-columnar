package org.knime.core.data.columnar.types;
///*
// * ------------------------------------------------------------------------
// *
// *  Copyright by KNIME AG, Zurich, Switzerland
// *  Website: http://www.knime.com; Email: contact@knime.com
// *
// *  This program is free software; you can redistribute it and/or modify
// *  it under the terms of the GNU General Public License, Version 3, as
// *  published by the Free Software Foundation.
// *
// *  This program is distributed in the hope that it will be useful, but
// *  WITHOUT ANY WARRANTY; without even the implied warranty of
// *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// *  GNU General Public License for more details.
// *
// *  You should have received a copy of the GNU General Public License
// *  along with this program; if not, see <http://www.gnu.org/licenses>.
// *
// *  Additional permission under GNU GPL version 3 section 7:
// *
// *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
// *  Hence, KNIME and ECLIPSE are both independent programs and are not
// *  derived from each other. Should, however, the interpretation of the
// *  GNU GPL Version 3 ("License") under any applicable laws result in
// *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
// *  you the additional permission to use and propagate KNIME together with
// *  ECLIPSE with only the license terms in place for ECLIPSE applying to
// *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
// *  license terms of ECLIPSE themselves allow for the respective use and
// *  propagation of ECLIPSE together with KNIME.
// *
// *  Additional permission relating to nodes for KNIME that extend the Node
// *  Extension (and in particular that are based on subclasses of NodeModel,
// *  NodeDialog, and NodeView) and that only interoperate with KNIME through
// *  standard APIs ("Nodes"):
// *  Nodes are deemed to be separate and independent programs and to not be
// *  covered works.  Notwithstanding anything to the contrary in the
// *  License, the License does not apply to Nodes, you are not required to
// *  license Nodes under the License, and you are granted a license to
// *  prepare and propagate Nodes, in each case even if such Nodes are
// *  propagated with or for interoperation with KNIME.  The owner of a Node
// *  may freely choose the license terms applicable to such Node, including
// *  when such Node is propagated with or for interoperation with KNIME.
// * ---------------------------------------------------------------------
// */
//
//package org.knime.core.data2.types;
//
//import org.knime.core.columnar.ColumnDataSpec;
//import org.knime.core.columnar.data.VarBinaryData.VarBinaryDataSpec;
//import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
//import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
//import org.knime.core.data.DataCell;
//import org.knime.core.data.DataType;
//import org.knime.core.data2.ColumnType;
//import org.knime.core.data2.DataCellReadValue;
//import org.knime.core.data2.IndexSupplier;
//import org.knime.core.data2.WriteValue;
//
//// TODO IntReadValue / IntWriteValue interfaces
//public final class DataCellColumnType implements ColumnType<VarBinaryWriteData, VarBinaryReadData> {
//
//	// TODO caching
//	public static final DataCellColumnType INSTANCE = new DataCellColumnType();
//
//	private DataCellColumnType() {
//	}
//
//	@Override
//	public ColumnDataSpec getColumnDataSpec() {
//		// TODO make configurable? default size etc?
//		return VarBinaryDataSpec.INSTANCE;
//	}
//
//	@Override
//	public DataCellReadValue createReadValue(VarBinaryReadData data, IndexSupplier index) {
//		return new DataCellReadValue(data, index);
//	}
//
//	@Override
//	public DataCellWriteValue createWriteValue(VarBinaryWriteData data, IndexSupplier index) {
//		return new DataCellWriteValue(data, index);
//	}
//
//	public static final class DataCellReadValue implements //
//			DataCellReadValue//
//	{
//
//		private final IndexSupplier m_index;
//		private final VarBinaryReadData m_data;
//
//		DataCellReadValue(VarBinaryReadData data, IndexSupplier index) {
//			m_data = data;
//			m_index = index;
//		}
//
//		@Override
//		public DataCell getDataCell() {
//			return deserialize(m_data.getBytes(m_index.getIndex()));
//		}
//
//		private DataCell deserialize(byte[] bytes) {
//			// TODO Auto-generated method stub
//			return null;
//		}
//	}
//
//	public static final class DataCellWriteValue implements //
//			WriteValue<DataCellReadValue> {
//
//		private final IndexSupplier m_index;
//		private final VarBinaryWriteData m_data;
//
//		DataCellWriteValue(VarBinaryWriteData data, IndexSupplier index) {
//			m_data = data;
//			m_index = index;
//		}
//
//		@Override
//		public void setValue(DataCellReadValue cell) {
//			setDataCell(cell.toDataCell());
//		}
//
//		public void setDataCell(DataCell cell) {
//			m_data.setBytes(m_index.getIndex(), serialize(cell));
//		}
//
//		private byte[] serialize(DataCell cell) {
//			if (cell.getType().isCollectionType()) {
//				final DataType elementType = cell.getType().getCollectionElementType();
//
//			}
//			// TODO check for validity against data type (e.g. that cell is compatible to
//			// spec?)
//			// TODO heap cache?
//			// TODO serialize blobstores
//			// TODO serialize filestores
//			// TODO serialize collections and inner elements (special impl for primitive
//			// collections?)
//			return null;
//		}
//	}
//}
