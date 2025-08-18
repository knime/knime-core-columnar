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
 *   Jul 20, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual;

import static java.util.Objects.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataTableSpecCreator;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.v2.schema.DataTableValueSchemaUtils;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchema.ValueSchemaColumn;
import org.knime.core.data.v2.value.BooleanValueFactory;
import org.knime.core.data.v2.value.DefaultRowKeyValueFactory;
import org.knime.core.data.v2.value.DoubleValueFactory;
import org.knime.core.data.v2.value.IntValueFactory;
import org.knime.core.table.access.BooleanAccess.BooleanReadAccess;
import org.knime.core.table.access.DoubleAccess.DoubleReadAccess;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.schema.traits.DefaultDataTraits;

/**
 * Helper class that holds various mocks to be used by tests related to buffering.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public class TestHelper {

    static final DataTableSpec SOURCE_SPEC = new DataTableSpecCreator()//
        .addColumns(//
            new DataColumnSpecCreator("boolean", BooleanCell.TYPE).createSpec(), //
            new DataColumnSpecCreator("int", IntCell.TYPE).createSpec(), //
            new DataColumnSpecCreator("double", DoubleCell.TYPE).createSpec()//
        ).createSpec();

    static final ValueSchema SCHEMA = DataTableValueSchemaUtils.create(SOURCE_SPEC, //
        new ValueSchemaColumn(DefaultRowKeyValueFactory.INSTANCE), //
        new ValueSchemaColumn(SOURCE_SPEC.getColumnSpec(0), BooleanValueFactory.INSTANCE, DefaultDataTraits.EMPTY), //
        new ValueSchemaColumn(SOURCE_SPEC.getColumnSpec(1), IntValueFactory.INSTANCE, DefaultDataTraits.EMPTY), //
        new ValueSchemaColumn(SOURCE_SPEC.getColumnSpec(2), DoubleValueFactory.INSTANCE, DefaultDataTraits.EMPTY));

    ReadAccessRow m_readAccessRow;

    StringReadAccess m_rowKeyReadAccess;

    BooleanReadAccess m_booleanReadAccess;

    IntReadAccess m_intReadAccess;

    DoubleReadAccess m_doubleReadAccess;

    void restub() {
        m_readAccessRow = mock(ReadAccessRow.class);
        m_rowKeyReadAccess = mock(StringReadAccess.class);
        m_booleanReadAccess = mock(BooleanReadAccess.class);
        m_intReadAccess = mock(IntReadAccess.class);
        m_doubleReadAccess = mock(DoubleReadAccess.class);
        stubReadAccessRow();
    }

    void stubAccesses(final String rowKey, final Boolean booleanValue, final Integer intValue,
        final Double doubleValue) {
        when(m_rowKeyReadAccess.getStringValue()).thenReturn(rowKey);
        stubIsMissing(m_booleanReadAccess, booleanValue);
        if (!isNull(booleanValue)) {
            when(m_booleanReadAccess.getBooleanValue()).thenReturn(booleanValue);
        }
        stubIsMissing(m_intReadAccess, intValue);
        if (!isNull(intValue)) {
            when(m_intReadAccess.getIntValue()).thenReturn(intValue);
        }
        stubIsMissing(m_doubleReadAccess, doubleValue);
        if (!isNull(doubleValue)) {
            when(m_doubleReadAccess.getDoubleValue()).thenReturn(doubleValue);
        }
    }

    private static void stubIsMissing(final ReadAccess access, final Object value) {
        when(access.isMissing()).thenReturn(isNull(value));
    }

    private void stubReadAccessRow() {
        when(m_readAccessRow.getAccess(0)).thenReturn(m_rowKeyReadAccess);
        when(m_readAccessRow.getAccess(1)).thenReturn(m_booleanReadAccess);
        when(m_readAccessRow.getAccess(2)).thenReturn(m_intReadAccess);
        when(m_readAccessRow.getAccess(3)).thenReturn(m_doubleReadAccess);
    }
}
