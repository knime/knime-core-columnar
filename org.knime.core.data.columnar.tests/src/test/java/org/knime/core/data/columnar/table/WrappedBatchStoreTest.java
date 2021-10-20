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
package org.knime.core.data.columnar.table;

import static org.junit.Assert.assertEquals;
import static org.knime.core.data.columnar.TestDataBatchStoreUtils.createSpec;
import static org.knime.core.data.columnar.TestDataBatchStoreUtils.readDefaultTable;
import static org.knime.core.data.columnar.TestDataBatchStoreUtils.writeDefaultTable;

import java.io.IOException;

import org.junit.Test;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.columnar.testing.TestBatchStore;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchemaUtils;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class WrappedBatchStoreTest extends ColumnarTest {

    private static ColumnarValueSchema generateDefaultSchema() {
        final DataTableSpec spec = createSpec();
        final ValueSchema valueSchema =
            ValueSchemaUtils.create(spec, RowKeyType.CUSTOM, NotInWorkflowWriteFileStoreHandler.create());
        return ColumnarValueSchemaUtils.create(valueSchema);
    }

    @SuppressWarnings("resource")
    private static WrappedBatchStore generateDefaultWrappedBatchStore() {
        final TestBatchStore delegate = TestBatchStore.create(SCHEMA);
        return new WrappedBatchStore(delegate, delegate);
    }

    static final ColumnarValueSchema SCHEMA = generateDefaultSchema();

    @Test
    public void testWriteRead() throws IOException {
        try (final WrappedBatchStore store = generateDefaultWrappedBatchStore()) {
            writeDefaultTable(store);
            readDefaultTable(store);
            readDefaultTable(store);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateAfterWriterClose() throws IOException {
        try (final WrappedBatchStore store = generateDefaultWrappedBatchStore()) {
            try (final BatchWriter writer = store.getWriter()) {
                writer.close(); // NOSONAR
                writer.create(SCHEMA.numColumns());
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateAfterStoreClose() throws IOException {
        try (final WrappedBatchStore store = generateDefaultWrappedBatchStore()) {
            try (final BatchWriter writer = store.getWriter()) {
                store.close(); // NOSONAR
                writer.create(SCHEMA.numColumns());
            }
        }
    }

    @Test
    public void testWriterSingleton() throws IOException {
        try (final WrappedBatchStore store = generateDefaultWrappedBatchStore();
                final BatchWriter writer1 = store.getWriter();
                final BatchWriter writer2 = store.getWriter()) {
            assertEquals(writer1, writer2);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetWriterAfterWriterClose() throws IOException {
        try (final WrappedBatchStore store = generateDefaultWrappedBatchStore()) {
            try (final BatchWriter writer = store.getWriter()) { // NOSONAR
            }
            try (final BatchWriter writer = store.getWriter()) { // NOSONAR
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetWriterAfterStoreClose() throws IOException {
        try (final WrappedBatchStore store = generateDefaultWrappedBatchStore()) {
            store.close(); // NOSONAR
            try (final BatchWriter writer = store.getWriter()) { // NOSONAR
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterWriterClose() throws IOException {
        try (final WrappedBatchStore store = generateDefaultWrappedBatchStore()) {
            @SuppressWarnings("resource")
            final BatchWriter writer = store.getWriter();
            final ReadBatch batch = writer.create(0).close(0);
            batch.release();
            writer.close();
            writer.write(batch);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterStoreClose() throws IOException {
        try (final WrappedBatchStore store = generateDefaultWrappedBatchStore()) {
            @SuppressWarnings("resource")
            final BatchWriter writer = store.getWriter();
            final ReadBatch batch = writer.create(0).close(0);
            batch.release();
            store.close(); // NOSONAR
            writer.write(batch);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderWhileWriterOpen() throws IOException {
        try (final WrappedBatchStore store = generateDefaultWrappedBatchStore()) {
            try (final RandomAccessBatchReader reader = store.createRandomAccessReader()) { // NOSONAR
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderAfterStoreClose() throws IOException {
        try (final WrappedBatchStore store = generateDefaultWrappedBatchStore()) {
            writeDefaultTable(store);
            store.close(); // NOSONAR
            try (final RandomAccessBatchReader reader = store.createRandomAccessReader()) { // NOSONAR
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterReaderClose() throws IOException {
        try (final WrappedBatchStore store = generateDefaultWrappedBatchStore()) {
            writeDefaultTable(store);
            try (final RandomAccessBatchReader reader = store.createRandomAccessReader()) {
                reader.close(); // NOSONAR
                reader.readRetained(0);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterStoreClose() throws IOException {
        try (final WrappedBatchStore store = generateDefaultWrappedBatchStore()) {
            writeDefaultTable(store);
            try (final RandomAccessBatchReader reader = store.createRandomAccessReader()) {
                store.close(); // NOSONAR
                reader.readRetained(0);
            }
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void exceptionOnReadIndexOutOfBoundsLower() throws IOException {
        try (final WrappedBatchStore store = generateDefaultWrappedBatchStore()) {
            writeDefaultTable(store);
            try (final RandomAccessBatchReader reader = store.createRandomAccessReader()) {
                reader.readRetained(-1);
            }
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void exceptionOnReadIndexOutOfBoundsUpper() throws IOException {
        try (final WrappedBatchStore store = generateDefaultWrappedBatchStore()) {
            writeDefaultTable(store);
            try (final RandomAccessBatchReader reader = store.createRandomAccessReader()) {
                reader.readRetained(Integer.MAX_VALUE);
            }
        }
    }

}
