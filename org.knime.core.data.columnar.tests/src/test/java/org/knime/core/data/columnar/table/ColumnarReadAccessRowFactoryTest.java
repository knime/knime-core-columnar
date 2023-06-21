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
 *   May 2, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.knime.core.columnar.access.ColumnDataIndex;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.cursor.ColumnarReadAccessRowFactory;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.table.access.DoubleAccess.DoubleReadAccess;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.row.Selection.ColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.traits.DefaultDataTraits;

/**
 * Contains unit tests for ColumnarReadAccessRowFactory.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class ColumnarReadAccessRowFactoryTest {

    private ColumnDataIndex m_indexInBatch;

    private ColumnSelection m_columnSelection;

    private ReadBatch m_batch;

    private ColumnarReadAccessRowFactory m_factory;

    private StringReadData m_stringData;

    private IntReadData m_intData;

    private DoubleReadData m_doubleData;

    private NullableReadData[] m_datas;

    private static final ColumnarSchema SCHEMA = new DefaultColumnarSchema(//
        List.of(DataSpec.stringSpec(), DataSpec.intSpec(), DataSpec.doubleSpec()), //
        Stream.generate(DefaultDataTraits::empty).limit(3).toList()//
    );

    @BeforeEach
    void setup() {
        m_indexInBatch = mock(ColumnDataIndex.class);
        m_columnSelection = mock(ColumnSelection.class);
        m_batch = mock(ReadBatch.class);
        m_stringData = mock(StringReadData.class);
        m_intData = mock(IntReadData.class);
        m_doubleData = mock(DoubleReadData.class);
        m_datas = new NullableReadData[] {m_stringData, m_intData, m_doubleData};
        m_factory = new ColumnarReadAccessRowFactory(SCHEMA);
    }

    @Test
    void testSingleColumn() throws Exception {
        when(m_columnSelection.allSelected()).thenReturn(false);
        when(m_columnSelection.getSelected()).thenReturn(new int[]{1});
        var row = m_factory.createRow(m_indexInBatch, m_columnSelection);
        assertEquals(3, row.size(), "Wrong size.");
        stubBatch(1);
        row.setBatch(m_batch);
        verify(m_batch).get(1);
        assertNull(row.getAccess(0), "Only the column at position 1 is selected.");
        assertNull(row.getAccess(2), "Only the column at position 1 is selected.");
        when(m_indexInBatch.getIndex()).thenReturn(4);
        when(m_intData.getInt(4)).thenReturn(42);
        assertEquals(42, row.<IntReadAccess> getAccess(1).getIntValue(),
            "Expected a different value at the current position.");
    }

    @Test
    void testFilteredColumns() throws Exception {
        when(m_columnSelection.allSelected()).thenReturn(false);
        int[] selected = {1, 2};
        when(m_columnSelection.getSelected()).thenReturn(selected);
        var row = m_factory.createRow(m_indexInBatch, m_columnSelection);
        assertEquals(3, row.size(), "Wrong size.");
        stubBatch(1, 2);
        when(m_indexInBatch.getIndex()).thenReturn(2);
        when(m_intData.getInt(2)).thenReturn(13);
        when(m_doubleData.getDouble(2)).thenReturn(13.37);
        row.setBatch(m_batch);
        for (int c : selected) {
            verify(m_batch).get(c);
        }
        assertNull(row.getAccess(0), "The first column is not selected and should be null.");
        assertEquals(13, row.<IntReadAccess>getAccess(1).getIntValue(), "Wrong int value returned.");
        assertEquals(13.37, row.<DoubleReadAccess>getAccess(2).getDoubleValue(), "Wrong double value returned.");
    }

    @Test
    void testAllColumnsSelected() throws Exception {
        when(m_columnSelection.allSelected()).thenReturn(true);
        var row = m_factory.createRow(m_indexInBatch, m_columnSelection);
        assertEquals(3, row.size(), "Wrong size.");
        stubBatch(0, 1, 2);
        row.setBatch(m_batch);
        for (int c = 0; c < 3; c++) {
            verify(m_batch).get(c);
        }
        when(m_indexInBatch.getIndex()).thenReturn(300);
        when(m_stringData.getString(300)).thenReturn("foo");
        when(m_intData.getInt(300)).thenReturn(-1);
        when(m_doubleData.getDouble(300)).thenReturn(3.14);
        assertEquals("foo", row.<StringReadAccess>getAccess(0).getStringValue(), "Wrong string value returned.");
        assertEquals(-1, row.<IntReadAccess>getAccess(1).getIntValue(), "Wrong int value returned.");
        assertEquals(3.14, row.<DoubleReadAccess>getAccess(2).getDoubleValue(), "Wrong double value returned.");
    }

    private void stubBatch(final int... selectedColumns) {
        for (int c : selectedColumns) {
            when(m_batch.get(c)).thenReturn(m_datas[c]);
        }
    }

}
