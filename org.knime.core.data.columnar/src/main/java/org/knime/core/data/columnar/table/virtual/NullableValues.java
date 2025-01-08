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
 *   Mar 10, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataValue;
import org.knime.core.data.MissingCell;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class NullableValues {

    private NullableValues() {}

    static final class NullableWriteValue {

        private final WriteValue<?> m_writeValue;

        private final WriteAccess m_writeAccess;

        NullableWriteValue(final WriteValue<?> writeValue, final WriteAccess writeAccess) {
            m_writeValue = writeValue;
            m_writeAccess = writeAccess;
        }

        void setDataCell(final DataCell cell) {
            if (cell.isMissing()) {
                m_writeAccess.setMissing();
            } else {
                setUnchecked(m_writeValue, cell);
            }
        }

        @SuppressWarnings("unchecked")
        private static <D extends DataValue> void setUnchecked(final WriteValue<D> writeValue, final DataValue value) {
            writeValue.setValue((D)value);
        }

        void setReadValue(final NullableReadValue value) {
            if (value.isMissing()) {
                m_writeAccess.setMissing();
            } else {
                setUnchecked(m_writeValue, value.getValue());
            }
        }

    }

    @SuppressWarnings("unchecked")
    static <R extends ReadAccess, V extends ReadValue> V createReadValue(final ValueFactory<R, ?> valueFactory,
        final ReadAccess readAccess) {
        return (V)valueFactory.createReadValue((R)readAccess);
    }

    @SuppressWarnings("unchecked")
    static <W extends WriteAccess, V extends WriteValue<?>> V createWriteValue(final ValueFactory<?, W> valueFactory,
        final WriteAccess writeAccess) {
        return (V)valueFactory.createWriteValue((W)writeAccess);
    }

    static NullableReadValue createNullableReadValue(final ValueFactory<?, ?> valueFactory,
        final ReadAccess readAccess) {
        return new NullableReadValue(createReadValue(valueFactory, readAccess), readAccess);
    }

    static NullableWriteValue createNullableWriteValue(final ValueFactory<?, ?> valueFactory,
        final WriteAccess writeAccess) {
        return new NullableWriteValue(createWriteValue(valueFactory, writeAccess), writeAccess);
    }

    static final class NullableReadValue {

        private static final MissingCell MISSING = new MissingCell("");

        private final ReadValue m_readValue;

        private final ReadAccess m_readAccess;

        NullableReadValue(final ReadValue readValue, final ReadAccess readAccess) {
            m_readValue = readValue;
            m_readAccess = readAccess;
        }

        ReadValue getValue() {
            return m_readValue;
        }

        boolean isMissing() {
            return m_readAccess.isMissing();
        }

        DataCell getDataCell() {
            if (m_readAccess.isMissing()) {
                return MISSING;
            } else {
                return m_readValue.getDataCell();
            }
        }
    }
}
