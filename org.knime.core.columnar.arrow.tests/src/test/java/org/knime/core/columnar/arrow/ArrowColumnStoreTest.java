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
 *   Oct 4, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Test;
import org.knime.core.columnar.ColumnarSchema;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.DataSpec;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.store.ColumnStoreFactory;

/**
 * Test {@link ArrowColumnStoreFactory}, ArrowColumnReadStore and ArrowColumnStore.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class ArrowColumnStoreTest {

    /**
     * Test writing and reading some data using the writer and reader from an ArrowColumnStore and ArrowColumnReadStore.
     * The stores are created by a {@link ArrowColumnStoreFactory} with the default allocator.
     *
     * @throws IOException
     */
    @Test
    public void testCreateWriterReaderDefaultAllocator() throws IOException {
        testCreateWriterReader(new ArrowColumnStoreFactory());
    }

    /**
     * Test the {@link ArrowColumnStoreFactory} with an allocator with a very small limit which is not enough for
     * allocating the data.
     */
    @Test
    public void testCreateWriterReaderCustomUnsufficientAllocator() {
        try (final RootAllocator allocator = new RootAllocator()) {
            final ArrowColumnStoreFactory factory = new ArrowColumnStoreFactory(allocator, 10, 10);
            assertThrows(OutOfMemoryException.class, () -> testCreateWriterReader(factory));
        }
    }

    /**
     * Test writing and reading some data using the writer and reader from an ArrowColumnStore and ArrowColumnReadStore.
     * The stores are created by a {@link ArrowColumnStoreFactory} with a custom allocator which remembers the child
     * allocators created.
     *
     * @throws IOException
     */
    @Test
    @SuppressWarnings("resource")
    public void testCreateWriterReaderCustomAllocator() throws IOException {
        final int limit = 16385;
        final List<BufferAllocator> childAllocators = new ArrayList<>();
        final AllocationListener allocationListener = new AllocationListener() {

            @Override
            public void onChildAdded(final BufferAllocator parentAllocator, final BufferAllocator childAllocator) {
                childAllocators.add(childAllocator);
            }
        };
        try (final RootAllocator allocator = new RootAllocator(allocationListener, limit)) {
            final ArrowColumnStoreFactory factory = new ArrowColumnStoreFactory(allocator, 0, limit);
            testCreateWriterReader(factory);
        }
        assertEquals(2, childAllocators.size());
        assertEquals(0, childAllocators.get(0).getAllocatedMemory());
        assertEquals(0, childAllocators.get(1).getAllocatedMemory());
    }

    @SuppressWarnings("resource")
    private static void testCreateWriterReader(final ColumnStoreFactory factory) throws IOException {
        final int chunkSize = 64;
        final ColumnarSchema schema = ArrowTestUtils.createSchema(DataSpec.doubleSpec());

        final Path writePath = ArrowTestUtils.createTmpKNIMEArrowPath();
        final Path readPath = ArrowTestUtils.createTmpKNIMEArrowPath();
        Files.delete(readPath);

        // Use the write store to write some data
        try (final BatchStore writeStore = factory.createStore(schema, writePath)) {
            assertEquals(schema, writeStore.getSchema());

            // Create a batch
            final WriteBatch writeBatch = writeStore.getWriter().create(chunkSize);
            final DoubleWriteData data = (DoubleWriteData)writeBatch.get(0);
            for (int i = 0; i < chunkSize; i++) {
                data.setDouble(i, i);
            }
            final ReadBatch readBatch = writeBatch.close(chunkSize);

            // Write the batch
            try (final BatchWriter writer = writeStore.getWriter()) {
                writer.write(readBatch);
            }
            readBatch.release();

            // Assert that the file has been written
            assertTrue(Files.exists(writePath));
            assertTrue(Files.size(writePath) > 0);

            Files.copy(writePath, readPath);
        }

        // Assert that the file for reading exists
        assertTrue(Files.exists(readPath));
        assertTrue(Files.size(readPath) > 0);

        // Use the read store to read some data
        try (final BatchReadStore readStore = factory.createReadStore(schema, readPath)) {
            assertEquals(schema, readStore.getSchema());

            // Read the batch
            final ReadBatch readBatch;
            try (final RandomAccessBatchReader reader = readStore.createReader()) {
                readBatch = reader.readRetained(0);
            }

            // Check the batch
            assertEquals(chunkSize, readBatch.length());
            final DoubleReadData data = (DoubleReadData)readBatch.get(0);
            assertEquals(chunkSize, data.length());
            for (int i = 0; i < chunkSize; i++) {
                assertFalse(data.isMissing(i));
                assertEquals(i, data.getDouble(i), 0);
            }
            readBatch.release();
        }

        // Assert that the file for reading exists
        assertTrue(Files.exists(readPath));
        assertTrue(Files.size(readPath) > 0);
    }
}
