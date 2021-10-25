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
 *   Oct 13, 2021 (eric.axt): created
 */
package org.knime.core.columnar.access;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.knime.core.columnar.access.ColumnarListAccessFactory.ColumnarListReadAccess;
import org.knime.core.columnar.access.ColumnarListAccessFactory.ColumnarListWriteAccess;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.testing.data.TestIntData.TestIntDataFactory;
import org.knime.core.columnar.testing.data.TestListData;
import org.knime.core.columnar.testing.data.TestListData.TestListDataFactory;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.IntAccess.IntWriteAccess;
import org.knime.core.table.access.ListAccess.ListReadAccess;
import org.knime.core.table.access.ListAccess.ListWriteAccess;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.ListDataSpec;

/**
 *
 * @author Eric Axt KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnarListAccessFactorySingleNestedListsTest {

    final ListDataSpec nestedSpec = new ListDataSpec(new ListDataSpec(IntDataSpec.INSTANCE));

    @SuppressWarnings("unchecked")
    final ColumnarListAccessFactory<ListReadData, ListWriteData> accessFactory =
        (ColumnarListAccessFactory<ListReadData, ListWriteData>)ColumnarAccessFactoryMapper
            .createAccessFactory(nestedSpec);

    final TestListDataFactory dataFactory =
        new TestListDataFactory(new TestListDataFactory(TestIntDataFactory.INSTANCE));

    final TestListData listData = dataFactory.createWriteData(2);

    @Test
    public void overwriteWrittenData() {

        ColumnDataIndex mockDex = mock(ColumnDataIndex.class);
        when(mockDex.getIndex()).thenReturn(1);

        ColumnarListWriteAccess<ListWriteData> listWriteAccessRow = accessFactory.createWriteAccess(mockDex);
        listWriteAccessRow.setData(listData);
        ColumnarListReadAccess<ListReadData> listReadAccessRow = accessFactory.createReadAccess(mockDex);
        listReadAccessRow.setData(listData);

        setValue(listWriteAccessRow, 5);
        assertEquals(getValue(listReadAccessRow), 5);

        listReadAccessRow.setData(listData);

        setValue(listWriteAccessRow, 3);
        assertEquals(getValue(listReadAccessRow), 3);
    }

    @Test
    public void iterateColumnNestedLists() {

        final ColumnDataIndex mockDex = mock(ColumnDataIndex.class);
        when(mockDex.getIndex()).thenReturn(0);

        ColumnarListWriteAccess<ListWriteData> outer = accessFactory.createWriteAccess(mockDex);
        outer.setData(listData);

        setValue(outer, 5);

        when(mockDex.getIndex()).thenReturn(1);
        setValue(outer, 3);

        when(mockDex.getIndex()).thenReturn(0);
        ColumnarListReadAccess<ListReadData> listReadAccessRow = accessFactory.createReadAccess(mockDex);
        listReadAccessRow.setData(listData);

        assertEquals(getValue(listReadAccessRow), 5);

        when(mockDex.getIndex()).thenReturn(1);
        assertEquals(getValue(listReadAccessRow), 3);

    }

    private static void setValue(final ListWriteAccess outer, final int value) {
        outer.create(1);
        ListWriteAccess rowListWriteAcess = outer.getWriteAccess();
        outer.setWriteIndex(0);
        rowListWriteAcess.create(1);
        outer.setWriteIndex(0);
        IntWriteAccess rowIntWriteValue = rowListWriteAcess.getWriteAccess();
        rowIntWriteValue.setIntValue(value);
    }

    private static int getValue(final ListReadAccess outer) {
        ListReadAccess rowListReader = outer.getAccess();
        IntReadAccess rowIntReader = rowListReader.getAccess();
        return rowIntReader.getIntValue();
    }

}
