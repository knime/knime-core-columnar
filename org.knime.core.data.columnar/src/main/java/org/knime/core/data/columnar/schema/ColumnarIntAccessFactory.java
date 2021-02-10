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
 *  aInt with this program; if not, see <http://www.gnu.org/licenses>.
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
package org.knime.core.data.columnar.schema;

import org.knime.core.columnar.data.DataSpec;
import org.knime.core.columnar.data.IntData.IntDataSpec;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.data.DataCell;
import org.knime.core.data.IntValue;
import org.knime.core.data.columnar.ColumnDataIndex;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.v2.access.IntAccess.IntReadAccess;
import org.knime.core.data.v2.access.IntAccess.IntWriteAccess;

/**
 * A ColumnarValueFactory implementation wrapping {@link IntReadData} / {@link IntWriteData} as {@link IntReadAccess} /
 * {@link IntWriteAccess}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
final class ColumnarIntAccessFactory
    implements ColumnarAccessFactory<IntReadData, IntReadAccess, IntWriteData, IntWriteAccess> {

    /** INSTANCE **/
    static final ColumnarIntAccessFactory INSTANCE = new ColumnarIntAccessFactory();

    private ColumnarIntAccessFactory() {
    }

    @Override
    public IntDataSpec getColumnDataSpec() {
        return DataSpec.intSpec();
    }

    @Override
    public final IntReadAccess createReadAccess(final IntReadData data, final ColumnDataIndex index) {
        return new DefaultIntReadAccess(data, index);
    }

    @Override
    public final IntWriteAccess createWriteAccess(final IntWriteData data, final ColumnDataIndex index) {
        return new DefaultIntWriteAccess(data, index);
    }

    private static final class DefaultIntReadAccess extends AbstractAccess<IntReadData> implements IntReadAccess {

        DefaultIntReadAccess(final IntReadData data, final ColumnDataIndex index) {
            super(data, index);
        }

        @Override
        public int getIntValue() {
            return m_data.getInt(m_index.getIndex());
        }

        @Override
        public boolean isMissing() {
            return m_data.isMissing(m_index.getIndex());
        }

        @Override
        public DataCell getDataCell() {
            return new IntCell(m_data.getInt(m_index.getIndex()));
        }

        @Override
        public double getDoubleValue() {
            return m_data.getInt(m_index.getIndex());
        }

        @Override
        public double getRealValue() {
            return m_data.getInt(m_index.getIndex());
        }

        @Override
        public double getImaginaryValue() {
            return 0;
        }

        @Override
        public double getMinSupport() {
            return m_data.getInt(m_index.getIndex());
        }

        @Override
        public double getCore() {
            return m_data.getInt(m_index.getIndex());
        }

        @Override
        public double getMaxSupport() {
            return m_data.getInt(m_index.getIndex());
        }

        @Override
        public double getMinCore() {
            return m_data.getInt(m_index.getIndex());
        }

        @Override
        public double getMaxCore() {
            return m_data.getInt(m_index.getIndex());
        }

        @Override
        public double getCenterOfGravity() {
            return m_data.getInt(m_index.getIndex());
        }

        @Override
        public long getLongValue() {
            return m_data.getInt(m_index.getIndex());
        }

    }

    private static final class DefaultIntWriteAccess extends AbstractAccess<IntWriteData> implements IntWriteAccess {

        DefaultIntWriteAccess(final IntWriteData data, final ColumnDataIndex index) {
            super(data, index);
        }

        @Override
        public void setIntValue(final int value) {
            m_data.setInt(m_index.getIndex(), value);
        }

        @Override
        public void setMissing() {
            m_data.setMissing(m_index.getIndex());
        }

        @Override
        public void setValue(final IntValue value) {
            m_data.setInt(m_index.getIndex(), value.getIntValue());
        }

    }

}
