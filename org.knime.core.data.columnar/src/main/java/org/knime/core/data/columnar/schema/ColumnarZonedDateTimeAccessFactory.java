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

import java.time.ZonedDateTime;

import org.knime.core.columnar.ColumnDataIndex;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.data.ZonedDateTimeData.ZonedDateTimeDataSpec;
import org.knime.core.columnar.data.ZonedDateTimeData.ZonedDateTimeReadData;
import org.knime.core.columnar.data.ZonedDateTimeData.ZonedDateTimeWriteData;
import org.knime.core.data.v2.access.ReadAccess;
import org.knime.core.data.v2.access.WriteAccess;
import org.knime.core.data.v2.access.ZonedDateTimeAccess.ZonedDateTimeReadAccess;
import org.knime.core.data.v2.access.ZonedDateTimeAccess.ZonedDateTimeWriteAccess;

/**
 * A ColumnarValueFactory implementation wrapping {@link ColumnReadData} / {@link ColumnWriteData} as {@link ReadAccess}
 * / {@link WriteAccess}
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
final class ColumnarZonedDateTimeAccessFactory implements
    ColumnarAccessFactory<ZonedDateTimeReadData, ZonedDateTimeReadAccess, ZonedDateTimeWriteData, ZonedDateTimeWriteAccess> {

    /** Instance **/
    public static final ColumnarZonedDateTimeAccessFactory INSTANCE = new ColumnarZonedDateTimeAccessFactory();

    private ColumnarZonedDateTimeAccessFactory() {
    }

    @Override
    public ZonedDateTimeDataSpec getColumnDataSpec() {
        return ZonedDateTimeDataSpec.INSTANCE;
    }

    @Override
    public ZonedDateTimeReadAccess createReadAccess(final ZonedDateTimeReadData data, final ColumnDataIndex index) {
        return new DefaultZonedDateTimeReadAccess(data, index);
    }

    @Override
    public ZonedDateTimeWriteAccess createWriteAccess(final ZonedDateTimeWriteData data, final ColumnDataIndex index) {
        return new DefaultZonedDateTimeWriteAccess(data, index);
    }

    private static final class DefaultZonedDateTimeReadAccess extends AbstractAccess<ZonedDateTimeReadData>
        implements ZonedDateTimeReadAccess {

        public DefaultZonedDateTimeReadAccess(final ZonedDateTimeReadData data, final ColumnDataIndex index) {
            super(data, index);
        }

        @Override
        public boolean isMissing() {
            return m_data.isMissing(m_index.getIndex());
        }

        @Override
        public ZonedDateTime getZonedDateTime() {
            return m_data.getZonedDateTime(m_index.getIndex());
        }
    }

    private static final class DefaultZonedDateTimeWriteAccess extends AbstractAccess<ZonedDateTimeWriteData>
        implements ZonedDateTimeWriteAccess {

        public DefaultZonedDateTimeWriteAccess(final ZonedDateTimeWriteData data, final ColumnDataIndex index) {
            super(data, index);
        }

        @Override
        public void setMissing() {
            m_data.setMissing(m_index.getIndex());
        }

        @Override
        public void setZonedDateTime(final ZonedDateTime date) {
            m_data.setZonedDateTime(m_index.getIndex(), date);
        }
    }
}
