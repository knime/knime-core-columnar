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
 *   28 Oct 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar.table;

import java.io.File;
import java.io.IOException;
import java.util.stream.IntStream;

import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.columnar.store.ColumnStoreSchema;
import org.knime.core.columnar.testing.TestColumnStore;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.RowWrite;
import org.knime.core.data.v2.ValueSchema;
import org.knime.core.data.v2.value.IntValueFactory.IntWriteValue;
import org.knime.core.node.ExecutionContext;

final class ColumnarTableTestUtils {

    static final class TestColumnStoreFactory implements ColumnStoreFactory {

        static final TestColumnStoreFactory INSTANCE = new TestColumnStoreFactory();

        private TestColumnStoreFactory() {
        }

        @Override
        public ColumnStore createStore(final ColumnStoreSchema schema, final File file) {
            return TestColumnStore.create(schema);
        }

        @Override
        public ColumnReadStore createReadStore(final ColumnStoreSchema schema, final File file) {
            throw new UnsupportedOperationException("Loading from file not supported by test column store.");
        }

    }

    static ColumnarValueSchema createSchema(final int nCols) {
        final DataTableSpec spec = new DataTableSpec(IntStream.range(0, nCols)
            .mapToObj(i -> new DataColumnSpecCreator(Integer.toString(i), IntCell.TYPE).createSpec())
            .toArray(DataColumnSpec[]::new));
        final ValueSchema valueSchema =
            ValueSchema.create(spec, RowKeyType.CUSTOM, NotInWorkflowWriteFileStoreHandler.create());
        return ColumnarValueSchemaUtils.create(valueSchema);
    }

    static ColumnarRowContainer createColumnarRowContainer() {
        return createColumnarRowContainer(null);
    }

    static ColumnarRowContainer createColumnarRowContainer(final ExecutionContext exec) {
        return createColumnarRowContainer(1, exec);
    }

    private static ColumnarRowContainer createColumnarRowContainer(final int nCols, final ExecutionContext exec) {
        final ColumnarRowContainerSettings settings = new ColumnarRowContainerSettings(true, 60, true);
        try {
            return ColumnarRowContainer.create(exec, -1, createSchema(nCols), TestColumnStoreFactory.INSTANCE,
                settings);
        } catch (IOException e) {
            throw new IllegalStateException("Exception when trying to create RowContainer.", e);
        }
    }

    static ColumnarContainerTable createUnsavedColumnarContainerTable(final int nRows) {
        return createUnsavedColumnarContainerTable(1, nRows, null);
    }

    static ColumnarContainerTable createUnsavedColumnarContainerTable(final int nCols, final int nRows) {
        return createUnsavedColumnarContainerTable(nCols, nRows, null);
    }

    private static ColumnarContainerTable createUnsavedColumnarContainerTable(final int nCols, final int nRows,
        final ExecutionContext exec) {
        try (final ColumnarRowContainer container = createColumnarRowContainer(nCols, exec);
                final ColumnarRowWriteCursor cursor = container.createCursor()) {
            for (int i = 0; i < nRows; i++) {
                final RowWrite row = cursor.forward();
                row.setRowKey(Integer.toString(i));
                for (int j = 0; j < nCols; j++) {
                    if (i % 2 == 0) { // NOSONAR
                        row.setMissing(j);
                    } else {
                        row.<IntWriteValue> getWriteValue(j).setIntValue(i);
                    }
                }
            }
            return (ColumnarContainerTable)container.finishInternal();
        } catch (Exception e) {
            throw new IllegalStateException("Exception when trying to create UnsavedColumnarContainerTable.", e);
        }
    }

    private ColumnarTableTestUtils() {
    }

}
