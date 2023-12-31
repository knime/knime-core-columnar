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
 *
 * History
 *   10 Feb 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.access;

import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryReadAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryWriteAccess;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 * A ColumnarValueFactory implementation wrapping {@link VarBinaryReadData} / {@link VarBinaryWriteData} as
 * {@link VarBinaryReadAccess} / {@link VarBinaryWriteAccess}.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
final class ColumnarVarBinaryAccessFactory implements ColumnarAccessFactory {

    /** Instance **/
    static final ColumnarVarBinaryAccessFactory INSTANCE = new ColumnarVarBinaryAccessFactory();

    private ColumnarVarBinaryAccessFactory() {
    }

    @Override
    public ColumnarVarBinaryWriteAccess createWriteAccess(final ColumnDataIndex index) {
        return new ColumnarVarBinaryWriteAccess(index);
    }

    @Override
    public ColumnarVarBinaryReadAccess createReadAccess(final ColumnDataIndex index) {
        return new ColumnarVarBinaryReadAccess(index);
    }

    static final class ColumnarVarBinaryReadAccess extends AbstractReadAccess<VarBinaryReadData>
        implements VarBinaryReadAccess {

        ColumnarVarBinaryReadAccess(final ColumnDataIndex index) {
            super(index);
        }

        @Override
        public byte[] getByteArray() {
            return m_data.getBytes(m_index.getIndex());
        }

        @Override
        public <T> T getObject(final ObjectDeserializer<T> deserializer) {
            return m_data.getObject(m_index.getIndex(), deserializer);
        }
    }

    static final class ColumnarVarBinaryWriteAccess extends AbstractWriteAccess<VarBinaryWriteData>
        implements VarBinaryWriteAccess {

        ColumnarVarBinaryWriteAccess(final ColumnDataIndex index) {
            super(index);
        }

        @Override
        public void setByteArray(final byte[] value) {
            m_data.setBytes(m_index.getIndex(), value);
        }

        @Override
        public void setByteArray(final byte[] array, final int index, final int length) {
            final byte[] subArray = new byte[length];
            System.arraycopy(array, index, subArray, 0, length);
            m_data.setBytes(m_index.getIndex(), subArray);
        }

        @Override
        public <T> void setObject(final T value, final ObjectSerializer<T> serializer) {
            m_data.setObject(m_index.getIndex(), value, serializer);
        }

        @Override
        public void setFromNonMissing(final ReadAccess access) {
            var binaryAccess = (VarBinaryReadAccess)access;
            if (binaryAccess.hasObjectAndSerializer()) {
                m_data.setObject(m_index.getIndex(), binaryAccess.getObject(), binaryAccess.getSerializer());
            } else {
                m_data.setBytes(m_index.getIndex(), binaryAccess.getByteArray());
            }
        }

    }
}
