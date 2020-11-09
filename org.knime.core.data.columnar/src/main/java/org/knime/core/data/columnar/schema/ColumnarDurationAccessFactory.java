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
package org.knime.core.data.columnar.schema;

import java.time.Duration;

import org.knime.core.columnar.ColumnDataIndex;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.data.DurationData.DurationDataSpec;
import org.knime.core.columnar.data.DurationData.DurationReadData;
import org.knime.core.columnar.data.DurationData.DurationWriteData;
import org.knime.core.data.v2.access.DurationAccess.DurationReadAccess;
import org.knime.core.data.v2.access.DurationAccess.DurationWriteAccess;
import org.knime.core.data.v2.access.ReadAccess;
import org.knime.core.data.v2.access.WriteAccess;

/**
 * A ColumnarValueFactory implementation wrapping {@link ColumnReadData} / {@link ColumnWriteData} as {@link ReadAccess}
 * / {@link WriteAccess}
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
final class ColumnarDurationAccessFactory
    implements ColumnarAccessFactory<DurationReadData, DurationReadAccess, DurationWriteData, DurationWriteAccess> {

    /** Instance **/
    public static final ColumnarDurationAccessFactory INSTANCE = new ColumnarDurationAccessFactory();

    private ColumnarDurationAccessFactory() {
    }

    @Override
    public DurationDataSpec getColumnDataSpec() {
        return DurationDataSpec.INSTANCE;
    }

    @Override
    public DurationReadAccess createReadAccess(final DurationReadData data, final ColumnDataIndex index) {
        return new DefaultDurationReadAccess(data, index);
    }

    @Override
    public DurationWriteAccess createWriteAccess(final DurationWriteData data, final ColumnDataIndex index) {
        return new DefaultDurationWriteAccess(data, index);
    }

    private static final class DefaultDurationReadAccess extends AbstractAccess<DurationReadData>
        implements DurationReadAccess {

        public DefaultDurationReadAccess(final DurationReadData data, final ColumnDataIndex index) {
            super(data, index);
        }

        @Override
        public boolean isMissing() {
            return m_data.isMissing(m_index.getIndex());
        }

        @Override
        public Duration getDuration() {
            return m_data.getDuration(m_index.getIndex());
        }
    }

    private static final class DefaultDurationWriteAccess extends AbstractAccess<DurationWriteData>
        implements DurationWriteAccess {

        public DefaultDurationWriteAccess(final DurationWriteData data, final ColumnDataIndex index) {
            super(data, index);
        }

        @Override
        public void setMissing() {
            m_data.setMissing(m_index.getIndex());
        }

        @Override
        public void setDuration(final Duration date) {
            m_data.setDuration(m_index.getIndex(), date);
        }
    }
}
