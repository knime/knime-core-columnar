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
 *   May 7, 2021 (marcel): created
 */
package org.knime.core.columnar.access;

import org.knime.core.columnar.data.FloatData.FloatReadData;
import org.knime.core.columnar.data.FloatData.FloatWriteData;
import org.knime.core.table.access.FloatAccess.FloatReadAccess;
import org.knime.core.table.access.FloatAccess.FloatWriteAccess;
import org.knime.core.table.access.ReadAccess;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class ColumnarFloatAccessFactory implements ColumnarAccessFactory {

    public static final ColumnarFloatAccessFactory INSTANCE = new ColumnarFloatAccessFactory();

    private ColumnarFloatAccessFactory() {}

    @Override
    public ColumnarFloatReadAccess createReadAccess(final ColumnDataIndex index) {
        return new ColumnarFloatReadAccess(index);
    }

    @Override
    public ColumnarFloatWriteAccess createWriteAccess(final ColumnDataIndex index) {
        return new ColumnarFloatWriteAccess(index);
    }

    static final class ColumnarFloatReadAccess extends AbstractReadAccess<FloatReadData> implements FloatReadAccess {

        ColumnarFloatReadAccess(final ColumnDataIndex index) {
            super(index);
        }

        @Override
        public float getFloatValue() {
            return m_data.getFloat(m_index.getIndex());
        }

    }

    static final class ColumnarFloatWriteAccess extends AbstractWriteAccess<FloatWriteData>
        implements FloatWriteAccess {

        ColumnarFloatWriteAccess(final ColumnDataIndex index) {
            super(index);
        }

        @Override
        public void setFloatValue(final float value) {
            m_data.setFloat(m_index.getIndex(), value);
        }

        @Override
        public void setFromNonMissing(final ReadAccess access) {
            if (access.getClass() == ColumnarFloatReadAccess.class) {
                final var columnar = (ColumnarFloatReadAccess)access;
                m_data.copyFrom(columnar.m_data, columnar.m_index.getIndex(), m_index.getIndex());
            } else if (access.isMissing()) {
                m_data.setMissing(m_index.getIndex());
            } else {
                m_data.setFloat(m_index.getIndex(), ((FloatReadAccess)access).getFloatValue());
            }
        }

    }
}
