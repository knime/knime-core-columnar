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
 *   2 Feb 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar.table;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.stream.IntStream;

import org.junit.Test;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.ColumnarTableTestUtils.TestColumnStoreFactory;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.ValueSchema;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnarDataContainerDelegateTest extends ColumnarTest {

    private static final DataCell MISSING_CELL = DataType.getMissingCell();

    private static DataTableSpec createSpec(final int nCols) {
        return new DataTableSpec(IntStream.range(0, nCols)
            .mapToObj(i -> new DataColumnSpecCreator(Integer.toString(i), IntCell.TYPE).createSpec())
            .toArray(DataColumnSpec[]::new));
    }

    @SuppressWarnings("resource")
    private static ColumnarDataContainerDelegate createColumnarDataContainerDelegate(final DataTableSpec spec)
        throws IOException {
        final ValueSchema valueSchema =
            ValueSchema.create(spec, RowKeyType.CUSTOM, NotInWorkflowWriteFileStoreHandler.create());
        final ColumnarValueSchema schema = ColumnarValueSchemaUtils.create(valueSchema);

        final ColumnarRowContainerSettings settings = new ColumnarRowContainerSettings(false, 0, false);
        final ColumnarRowContainer container =
            ColumnarRowContainer.create(null, -1, schema, TestColumnStoreFactory.INSTANCE, settings);

        return new ColumnarDataContainerDelegate(spec, container);
    }

    @Test
    public void testWriteRead() throws IOException {
        final int nCols = 2;
        final int nRows = 2;
        try (final ColumnarDataContainerDelegate delegate = createColumnarDataContainerDelegate(createSpec(nCols))) {
            for (int i = 0; i < nRows; i++) {
                final RowKey rowKey = new RowKey(Integer.toString(i));
                final DataCell[] cells = new DataCell[nCols];
                for (int j = 0; j < nCols; j++) {
                    cells[j] = i % 2 == 0 ? MISSING_CELL : new IntCell(i);
                }
                delegate.addRowToTable(new DefaultRow(rowKey, cells));
            }
            delegate.close(); // NOSONAR
            try (@SuppressWarnings("resource")
            ColumnarContainerTable table = (ColumnarContainerTable)delegate.getTable()) {
                ColumnarRowIteratorTest.compare(table, 0, 1);
            }
        }
    }

    @Test(expected = NullPointerException.class)
    public void testNullPointerExceptionOnWrite() throws IOException {
        @SuppressWarnings("resource")
        final ColumnarDataContainerDelegate delegate = createColumnarDataContainerDelegate(createSpec(1));
        try {
            delegate.addRowToTable(null);
        } finally {
            delegate.clear();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testIllegalStateExceptionOnWriteAfterClose() throws IOException {
        @SuppressWarnings("resource")
        final ColumnarDataContainerDelegate delegate = createColumnarDataContainerDelegate(createSpec(0));
        delegate.close();
        try {
            delegate.addRowToTable(new DefaultRow("1", Collections.emptyList()));
        } finally {
            delegate.clear();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testIllegalStateExceptionOnWriteAfterClear() throws IOException {
        try (final ColumnarDataContainerDelegate delegate = createColumnarDataContainerDelegate(createSpec(0))) {
            delegate.clear();
            delegate.addRowToTable(new DefaultRow("1", Collections.emptyList()));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArgumentExceptionOnWrite() throws IOException {
        @SuppressWarnings("resource")
        final ColumnarDataContainerDelegate delegate = createColumnarDataContainerDelegate(createSpec(1));
        try {
            delegate.addRowToTable(new DefaultRow("1", Collections.emptyList()));
        } finally {
            delegate.clear();
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void testIllegalStateExceptionOnGetTableBeforeClose() throws IOException {
        final ColumnarDataContainerDelegate delegate = createColumnarDataContainerDelegate(createSpec(0));
        try {
            delegate.getTable();
        } finally {
            delegate.clear();
        }
    }

    @Test
    public void testGetTableSpec() throws IOException {
        final DataTableSpec spec = createSpec(1);
        @SuppressWarnings("resource")
        final ColumnarDataContainerDelegate delegate = createColumnarDataContainerDelegate(spec);
        assertEquals(spec, delegate.getTableSpec());
        delegate.close();
        assertEquals(spec, delegate.getTableSpec());
        delegate.clear();
    }

    @Test
    public void testGetSize() throws IOException {
        final int nRows = 42;
        try (final ColumnarDataContainerDelegate delegate = createColumnarDataContainerDelegate(createSpec(0))) {
            for (int i = 0; i < nRows; i++) {
                assertEquals(i, delegate.size());
                delegate.addRowToTable(new DefaultRow(Integer.toString(i), Collections.emptyList()));
            }
            delegate.close(); // NOSONAR
            assertEquals(nRows, delegate.size());
            delegate.clear();
        }
    }

    @Test
    public void testClearTwice() throws IOException {
        @SuppressWarnings("resource")
        final ColumnarDataContainerDelegate delegate = createColumnarDataContainerDelegate(createSpec(1));
        delegate.clear();
        delegate.close();
        delegate.clear();
    }

    @Test
    public void testCloseTwice() throws IOException {
        @SuppressWarnings("resource")
        final ColumnarDataContainerDelegate delegate = createColumnarDataContainerDelegate(createSpec(1));
        delegate.close();
        delegate.clear();
        delegate.close();
    }

    //    @throws NullPointerException if the argument is <code>null</code>
    //    * @throws  If the state forbids to add rows.
    //    * @throws  if the structure of the row forbids to
    //    *         add it to the table
    //    * @throws DataContainerException An IllegalArgumentException may also be
    //    *         wrapped in a DataContainerException if the writing takes place
    //    *         asynchronously. This exception may be caused by the writing of
    //    *         a row that was added previously (not necessarily the current
    //    *         argument). This exception may also indicate the interruption
    //    *         of a write process.
    //    * @throws org.knime.core.util.DuplicateKeyException
    //    *         If the row's key has already been added.

}
