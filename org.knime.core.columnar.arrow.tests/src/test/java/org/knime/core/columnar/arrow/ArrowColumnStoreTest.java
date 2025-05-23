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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.knime.core.columnar.arrow.compress.ArrowCompressionUtil.ARROW_NO_COMPRESSION;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory.ArrowColumnStoreFactoryCreator;
import org.knime.core.columnar.arrow.ArrowTestUtils.OffHeapOrOnHeap;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.columnar.arrow.mmap.MappedMessageSerializerTestUtil;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.SequentialBatchReadable;
import org.knime.core.columnar.batch.SequentialBatchReader;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.columnar.store.FileHandle;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.traits.DefaultDataTraits;

/**
 * Test {@link ArrowColumnStoreFactory}, ArrowColumnReadStore and ArrowColumnStore.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class ArrowColumnStoreTest {

    static FileHandle writePath;

    static FileHandle readPath;

    /**
     * Before running the tests, we create temporary paths where ArrowBatchStores can write to, or read from.
     *
     * @throws IOException
     */
    @BeforeAll
    public static void beforeClass() throws IOException {
        writePath = ArrowTestUtils.createTmpKNIMEArrowFileSupplier();
        readPath = ArrowTestUtils.createTmpKNIMEArrowFileSupplier();
    }

    /**
     * After each test, we clean up the temporary files so the next test can start from scratch
     *
     * @throws IOException
     */
    @AfterEach
    public void after() throws IOException {
        writePath.delete();
        readPath.delete();
        MappedMessageSerializerTestUtil.assertAllClosed();
    }

    /**
     * Test writing and reading some data using the writer and reader from an ArrowColumnStore and ArrowColumnReadStore.
     * The stores are created by a {@link ArrowColumnStoreFactory} with the default allocator.
     *
     * @throws IOException
     */
    @ParameterizedTest
    @EnumSource(OffHeapOrOnHeap.class)
    void testCreateWriterReader(final OffHeapOrOnHeap impl) throws IOException {
        impl.setProperty();
        testCreateWriterReader(createStoreFactory(10000));
    }

    /**
     * Test the {@link ArrowColumnStoreFactory} with an allocator with a very small limit which is not enough for
     * allocating the data.
     */
    @ParameterizedTest
    @EnumSource(OffHeapOrOnHeap.class)
    void testCreateWriterReaderMemoryLimit(final OffHeapOrOnHeap impl) {
        impl.setProperty();
        final var factory = createStoreFactory(10);
        assertThrows(OutOfMemoryException.class, () -> testCreateWriterReader(factory));
    }

    /**
     * Test writing and reading some data using the writer and reader from an ArrowColumnStore and ArrowColumnReadStore.
     * The stores are created by a {@link ArrowColumnStoreFactory} with a custom allocator which remembers the child
     * allocators created.
     *
     * @throws IOException
     */
    @ParameterizedTest
    @EnumSource(OffHeapOrOnHeap.class)
    @SuppressWarnings("resource")
    void testCreateWriterReaderChildAllocators(final OffHeapOrOnHeap impl) throws IOException {
        impl.setProperty();
        final List<BufferAllocator> childAllocators = new ArrayList<>();
        final AllocationListener allocationListener = new AllocationListener() {

            @Override
            public void onChildAdded(final BufferAllocator parentAllocator, final BufferAllocator childAllocator) {
                childAllocators.add(childAllocator);
            }
        };
        try (final RootAllocator allocator = new RootAllocator(allocationListener, 16385)) {
            final ArrowColumnStoreFactory factory =
                new ArrowColumnStoreFactory(allocator, ArrowCompressionUtil.getDefaultCompression());
            testCreateWriterReader(factory);
        }
        assertEquals(2, childAllocators.size());
        assertEquals(0, childAllocators.get(0).getAllocatedMemory());
        assertEquals(0, childAllocators.get(1).getAllocatedMemory());
    }

    /**
     * Test reading batches out of order from an Arrow file after it is completely written.
     *
     * @throws IOException
     */
    @ParameterizedTest
    @EnumSource(OffHeapOrOnHeap.class)
    void testRandomAccessReadAfterWrite(final OffHeapOrOnHeap impl) throws IOException {
        impl.setProperty();
        final int chunkSize = 64;
        final ColumnarSchema schema = new DefaultColumnarSchema(DataSpec.intSpec(), DefaultDataTraits.EMPTY);

        // Use the write store to write some data
        try (final RootAllocator allocator = new RootAllocator();
                final BatchStore store =
                    (new ArrowColumnStoreFactory(allocator, ARROW_NO_COMPRESSION).createStore(schema, writePath))) {
            assertEquals(0, store.numBatches());

            @SuppressWarnings("resource")
            final BatchWriter writer = store.getWriter();
            ReadBatch batch;

            // Write batch 0
            batch = fillBatch(writer.create(chunkSize), chunkSize, 0);
            writer.write(batch);
            batch.release();
            assertEquals(1, store.numBatches());

            // Write batch 1
            batch = fillBatch(writer.create(chunkSize), chunkSize, 1);
            writer.write(batch);
            batch.release();
            assertEquals(2, store.numBatches());

            // Write batch 2
            batch = fillBatch(writer.create(chunkSize), chunkSize, 2);
            writer.write(batch);
            batch.release();
            assertEquals(3, store.numBatches());

            // Write batch 3
            batch = fillBatch(writer.create(chunkSize), chunkSize, 3);
            writer.write(batch);
            batch.release();
            assertEquals(4, store.numBatches());

            // Write batch 4
            batch = fillBatch(writer.create(chunkSize), chunkSize, 4);
            writer.write(batch);
            batch.release();
            assertEquals(5, store.numBatches());

            // Close the writer
            writer.close();

            @SuppressWarnings("resource")
            final RandomAccessBatchReader reader = store.createRandomAccessReader(); // NOSONAR

            // Read back batch 1 already
            batch = reader.readRetained(1);
            assertBatchData(batch, chunkSize, 1);
            batch.release();

            // Read back batch 4
            batch = reader.readRetained(3);
            assertBatchData(batch, chunkSize, 3);
            batch.release();

            // Read back batch 0
            batch = reader.readRetained(0);
            assertBatchData(batch, chunkSize, 0);
            batch.release();

            // Read back batch 3
            batch = reader.readRetained(3);
            assertBatchData(batch, chunkSize, 3);
            batch.release();

            // Close the reader
            reader.close();

            assertEquals(5, store.numBatches());
        }
    }

    /**
     * Test that reading from an Arrow file before it is completely written using the RandomAccess reader will fail.
     *
     * @throws IOException
     */
    @SuppressWarnings("resource")
    @ParameterizedTest
    @EnumSource(OffHeapOrOnHeap.class)
    void testRandomAccessReadBeforeCloseThrows(final OffHeapOrOnHeap impl) throws IOException {
        impl.setProperty();
        final int chunkSize = 64;
        final ColumnarSchema schema = new DefaultColumnarSchema(DataSpec.intSpec(), DefaultDataTraits.EMPTY);

        // Use the write store to write some data
        try (final RootAllocator allocator = new RootAllocator();
                final BatchStore store =
                    (new ArrowColumnStoreFactory(allocator, ARROW_NO_COMPRESSION)).createStore(schema, writePath)) {
            assertEquals(0, store.numBatches());

            final BatchWriter writer = store.getWriter();
            ReadBatch batch;

            // Write batch 0
            batch = fillBatch(writer.create(chunkSize), chunkSize, 0);
            writer.write(batch);
            batch.release();
            assertEquals(1, store.numBatches());

            // Write batch 1
            batch = fillBatch(writer.create(chunkSize), chunkSize, 1);
            writer.write(batch);
            batch.release();
            assertEquals(2, store.numBatches());

            // This should throw because the writer is not closed
            var reader = store.createRandomAccessReader(); // NOSONAR
            assertThrows(IOException.class, () -> reader.readRetained(0));
        }
    }

    /**
     * Test reading from an Arrow file before it is completely written (no dictionaries).
     *
     * @throws IOException
     */
    @ParameterizedTest
    @EnumSource(OffHeapOrOnHeap.class)
    void testPartialFileBatchReadable(final OffHeapOrOnHeap impl) throws IOException {
        impl.setProperty();
        final int chunkSize = 64;
        final ColumnarSchema schema = new DefaultColumnarSchema(DataSpec.intSpec(), DefaultDataTraits.EMPTY);

        try (var allocator = new RootAllocator()) {

            final ArrowColumnStoreFactory factory = new ArrowColumnStoreFactory(allocator, ARROW_NO_COMPRESSION);

            // Use the write store to write some data
            @SuppressWarnings("resource")
            final var writeStore = factory.createStore(schema, writePath);

            assertEquals(0, writeStore.numBatches());

            @SuppressWarnings("resource")
            final BatchWriter writer = writeStore.getWriter();
            ReadBatch batch;

            // Write batch 0
            batch = fillBatch(writer.create(chunkSize), chunkSize, 0);
            writer.write(batch);
            batch.release();
            assertEquals(1, writeStore.numBatches());

            // Write batch 1
            batch = fillBatch(writer.create(chunkSize), chunkSize, 1);
            writer.write(batch);
            batch.release();
            assertEquals(2, writeStore.numBatches());

            // Create the partial readable
            @SuppressWarnings("resource")
            final SequentialBatchReadable readable =
                factory.createPartialFileReadable(writePath.asPath(), writeStore.getOffsetProvider());
            @SuppressWarnings("resource")
            final SequentialBatchReader reader = readable.createSequentialReader();

            // Read back batch 0
            batch = reader.forward();
            assertBatchData(batch, chunkSize, 0);
            batch.release();

            // Read back batch 1
            batch = reader.forward();
            assertBatchData(batch, chunkSize, 1);
            batch.release();

            // Write batch 2
            batch = fillBatch(writer.create(chunkSize), chunkSize, 2);
            writer.write(batch);
            batch.release();

            // Write batch 3
            batch = fillBatch(writer.create(chunkSize), chunkSize, 3);
            writer.write(batch);
            batch.release();

            // Ignore batch 2
            reader.forward().release();

            // Read back batch 3
            batch = reader.forward();
            assertBatchData(batch, chunkSize, 3);
            batch.release();

            // Write batch 4
            batch = fillBatch(writer.create(chunkSize), chunkSize, 4);
            writer.write(batch);
            batch.release();

            // Close the writer
            writer.close();

            // Read back batch 4
            batch = reader.forward();
            assertBatchData(batch, chunkSize, 4);
            batch.release();

            assertEquals(5, writeStore.numBatches());

            writeStore.close();
            readable.close();
        }
    }

    @SuppressWarnings("resource")
    private static void testCreateWriterReader(final ColumnStoreFactory factory) throws IOException {
        final int chunkSize = 64;
        final ColumnarSchema schema = new DefaultColumnarSchema(DataSpec.doubleSpec(), DefaultDataTraits.EMPTY);

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
            assertTrue(Files.exists(writePath.asPath()));
            assertTrue(Files.size(writePath.asPath()) > 0);

            Files.copy(writePath.asPath(), readPath.asPath());
        }

        // Assert that the file written to does exist after closing the store
        assertTrue(Files.exists(writePath.asPath()));

        // Assert that the file for reading exists
        assertTrue(Files.exists(readPath.asPath()));
        assertTrue(Files.size(readPath.asPath()) > 0);

        // Use the read store to read some data
        try (final BatchReadStore readStore = factory.createReadStore(readPath.asPath())) {
            assertEquals(schema, readStore.getSchema());
            assertEquals(1, readStore.numBatches());

            // Read the batch
            final ReadBatch readBatch;
            try (final RandomAccessBatchReader reader = readStore.createRandomAccessReader()) {
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

            assertEquals(1, readStore.numBatches());
        }

        // Assert that the file for reading exists
        assertTrue(Files.exists(readPath.asPath()));
        assertTrue(Files.size(readPath.asPath()) > 0);
    }

    /** Fill the given batch (consisting of one int column) with some random data */
    private static ReadBatch fillBatch(final WriteBatch batch, final int chunkSize, final long seed) {
        final Random random = new Random(seed);
        final IntWriteData data = (IntWriteData)batch.get(0);
        for (int i = 0; i < chunkSize; i++) {
            data.setInt(i, random.nextInt());
        }
        return batch.close(chunkSize);
    }

    /** Assert that the batch contains data written by {@link #fillBatch(WriteBatch, int, long)} with the same seed. */
    private static void assertBatchData(final ReadBatch batch, final int chunkSize, final long seed) {
        final Random random = new Random(seed);
        final IntReadData data = (IntReadData)batch.get(0);
        assertEquals(chunkSize, data.length());
        for (int i = 0; i < chunkSize; i++) {
            assertEquals(random.nextInt(), data.getInt(i));
        }
    }

    /** Create an {@link ArrowColumnStoreFactory} and set the memory limit */
    private static ArrowColumnStoreFactory createStoreFactory(final long limit) {
        return new ArrowColumnStoreFactoryCreator().createFactory(limit);
    }
}
