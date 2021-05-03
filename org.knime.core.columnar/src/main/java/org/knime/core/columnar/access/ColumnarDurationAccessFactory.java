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
package org.knime.core.columnar.access;

import java.time.Duration;

import org.knime.core.columnar.data.DurationData.DurationReadData;
import org.knime.core.columnar.data.DurationData.DurationWriteData;
import org.knime.core.table.access.DurationAccess.DurationReadAccess;
import org.knime.core.table.access.DurationAccess.DurationWriteAccess;

final class ColumnarDurationAccessFactory
    implements ColumnarAccessFactory {

    /** INSTANCE **/
    static final ColumnarDurationAccessFactory INSTANCE = new ColumnarDurationAccessFactory();

    private ColumnarDurationAccessFactory() {
    }

    @Override
    public final ColumnarDurationReadAccess createReadAccess(final ColumnDataIndex index) {
        return new ColumnarDurationReadAccess(index);
    }

    @Override
    public final ColumnarDurationWriteAccess createWriteAccess(final ColumnDataIndex index) {
        return new ColumnarDurationWriteAccess(index);
    }

    static final class ColumnarDurationReadAccess extends AbstractReadAccess<DurationReadData> implements DurationReadAccess {

        ColumnarDurationReadAccess(final ColumnDataIndex index) {
            super(index);
        }

        @Override
        public Duration getDurationValue() {
            return m_data.getDuration(m_index.getIndex());
        }

        @Override
        public boolean isMissing() {
            return m_data.isMissing(m_index.getIndex());
        }

    }

    static final class ColumnarDurationWriteAccess extends AbstractWriteAccess<DurationWriteData> implements DurationWriteAccess {

        ColumnarDurationWriteAccess(final ColumnDataIndex index) {
            super(index);
        }

        @Override
        public void setDurationValue(final Duration value) {
            m_data.setDuration(m_index.getIndex(), value);
        }

    }

}
