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
 *   Jul 26, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpecCreator;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.table.access.BooleanAccess.BooleanReadAccess;
import org.knime.core.table.access.DoubleAccess.DoubleReadAccess;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.Selection;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Contains unit tests for {@link FilteredRowRead}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
@RunWith(MockitoJUnitRunner.class)
public class FilteredRowReadTest {

    @Mock
    private Selection.ColumnSelection m_columnSelection;

    @Mock
    private ReadAccessRow m_readAccessRow;

    @Mock
    private StringReadAccess m_rowKey;

    @Mock
    private BooleanReadAccess m_booleanAccess;

    @Mock
    private IntReadAccess m_intAccess;

    @Mock
    private DoubleReadAccess m_doubleAccess;

    private ReadAccess[] m_accesses;

    @Before
    public void init() {
        m_accesses = new ReadAccess[] {m_rowKey, m_booleanAccess, m_intAccess, m_doubleAccess};
    }

    @Test
    public void testFilterFirstColumn() {
        // the index 0 is the row key
        setupColumnSelection(0, 2, 3);
        var filteredSchema = filteredSchema(TestHelper.SCHEMA, m_columnSelection.getSelected());
        int numColumns = TestHelper.SCHEMA.numColumns();
        final FilteredRowRead rowRead =
            new FilteredRowRead(filteredSchema, m_readAccessRow, m_columnSelection, numColumns);
        when(m_rowKey.getStringValue()).thenReturn("Row0");
        assertEquals("Row0", rowRead.getRowKey().getString());
        assertNPE(rowRead, 0);
        assertFalse(rowRead.isMissing(1));
        when(m_intAccess.getIntValue()).thenReturn(14);
        assertEquals(14, rowRead.<IntValue> getValue(1).getIntValue());
        assertFalse(rowRead.isMissing(2));
        when(m_doubleAccess.getDoubleValue()).thenReturn(1.3);
        assertEquals(1.3, rowRead.<DoubleValue> getValue(2).getDoubleValue(), 1e-8);
    }


    @Test
    public void testGetNumColumns() {
        setupColumnSelection(0, 2, 3);
        var filteredSchema = filteredSchema(TestHelper.SCHEMA, m_columnSelection.getSelected());
        int numColumns = TestHelper.SCHEMA.numColumns();
        final FilteredRowRead rowRead =
                new FilteredRowRead(filteredSchema, m_readAccessRow, m_columnSelection, numColumns);
        assertEquals("Applying a TableFilter does not reduce the number of columns.", 3, rowRead.getNumColumns());
    }

    private static void assertNPE(final FilteredRowRead rowRead, final int idx) {
        try {//NOSONAR
            rowRead.isMissing(idx);
            fail(String.format("Column %s is filtered, therefore we expect an NPE here.", idx));
        } catch (NullPointerException npe) {
            // expected
        }
    }

    private static ColumnarValueSchema filteredSchema(final ColumnarValueSchema schema, final int... columnIndices)
    {
        var valueFactories = IntStream.of(columnIndices)//
                .mapToObj(schema::getValueFactory)//
                .toArray(ValueFactory<?, ?>[]::new);
        var originalSpec = schema.getSourceSpec();
        var specCreator = new DataTableSpecCreator(originalSpec);
        specCreator.dropAllColumns();
        var permutationStream = IntStream.of(columnIndices);
        // The RowID is not part of the DataTableSpec.
        permutationStream = permutationStream
                .filter(i -> i > 0) // skip if present
                .map(i -> i - 1); // translate "indices including RowID" to "indices without RowID"
        specCreator.addColumns(permutationStream.mapToObj(originalSpec::getColumnSpec).toArray(DataColumnSpec[]::new));
        return ColumnarValueSchemaUtils.create(specCreator.createSpec(), valueFactories);
    }

    private void setupColumnSelection(final int... included) {
        int idx = 0;
        for (int i : included) {
            when(m_columnSelection.isSelected(i)).thenReturn(true);
            when(m_columnSelection.allSelected()).thenReturn(false);
            when(m_columnSelection.getSelected()).thenReturn(included);
            when(m_readAccessRow.getAccess(idx)).thenReturn(m_accesses[i]);
            idx++;
        }
    }
}
