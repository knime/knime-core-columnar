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
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.knime.core.columnar.arrow.ArrowTestUtils.DictionaryEncodedData;
import org.knime.core.columnar.arrow.ArrowTestUtils.DictionaryEncodedDataFactory;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.filter.DefaultColumnSelection;
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
     * Before running the tests, we create temporary paths where ArrowBatchStores can write
     * to, or read from.
     *
     * @throws IOException
     */
    @BeforeClass
    public static void beforeClass() throws IOException {
        writePath = ArrowTestUtils.createTmpKNIMEArrowFileSupplier();
        readPath = ArrowTestUtils.createTmpKNIMEArrowFileSupplier();
    }

    /**
     * After each test, we clean up the temporary files so the next test can start from scratch
     * @throws IOException
     */
    @After
    public void after() throws IOException {
        writePath.delete();
        readPath.delete();
    }

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

    /**
     * Test reading from an Arrow file before it is completely written.
     *
     * @throws IOException
     */
    @Test
    public void testReadBeforeFullyWritten() throws IOException {
        final int chunkSize = 64;
        final ColumnarSchema schema = new DefaultColumnarSchema(DataSpec.intSpec(), DefaultDataTraits.EMPTY);

        // Use the write store to write some data
        try (final RootAllocator allocator = new RootAllocator();
                final BatchStore store = new ArrowBatchStore(schema, writePath, ARROW_NO_COMPRESSION, allocator)) {
            assertEquals(0, store.numBatches());
            assertThrows(IllegalStateException.class, store::batchLength);

            @SuppressWarnings("resource")
            final BatchWriter writer = store.getWriter();
            ReadBatch batch;

            // Write batch 0
            batch = fillBatch(writer.create(chunkSize), chunkSize, 0);
            writer.write(batch);
            batch.release();
            assertEquals(1, store.numBatches());
            assertEquals(chunkSize, store.batchLength());

            // Write batch 1
            batch = fillBatch(writer.create(chunkSize), chunkSize, 1);
            writer.write(batch);
            batch.release();
            assertEquals(2, store.numBatches());

            @SuppressWarnings("resource")
            final RandomAccessBatchReader reader = store.createRandomAccessReader(); // NOSONAR

            // Read back batch 1 already
            batch = reader.readRetained(1);
            assertBatchData(batch, chunkSize, 1);
            batch.release();

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

            // Read back batch 0
            batch = reader.readRetained(0);
            assertBatchData(batch, chunkSize, 0);
            batch.release();

            // Read back batch 3
            batch = reader.readRetained(3);
            assertBatchData(batch, chunkSize, 3);
            batch.release();

            // Write batch 4
            batch = fillBatch(writer.create(chunkSize), chunkSize, 4);
            writer.write(batch);
            batch.release();
            assertEquals(5, store.numBatches());

            // Close the writer
            writer.close();

            // Read back batch 4
            batch = reader.readRetained(3);
            assertBatchData(batch, chunkSize, 3);
            batch.release();

            // Close the reader
            reader.close();

            assertEquals(5, store.numBatches());
            assertEquals(chunkSize, store.batchLength());
        }
    }

    /**
     * Test reading from an Arrow file before it is completely written (including dictionaries).
     *
     * @throws IOException
     */
    @Test
    public void testReadBeforeFullyWrittenDictionary() throws IOException {
        // NOTE:
        // There is no data that makes use of dictionaries except the test data.
        // Therefore we cannot use the store.
        final int chunkSize = 64;
        final ArrowColumnDataFactory[] factories = new ArrowColumnDataFactory[]{new DictionaryEncodedDataFactory()};

                // Use the write store to write some data
        try (final RootAllocator allocator = new RootAllocator()) {

            @SuppressWarnings("resource")
            final ArrowBatchWriter writer =
                new ArrowBatchWriter(writePath, factories, ARROW_NO_COMPRESSION, allocator);
            ReadBatch batch;

            // Write batch 0
            batch = fillBatchDict(writer.create(chunkSize), chunkSize, 0);
            writer.write(batch);
            batch.release();

            // Write batch 1
            batch = fillBatchDict(writer.create(chunkSize), chunkSize, 1);
            writer.write(batch);
            batch.release();

            @SuppressWarnings("resource")
            final RandomAccessBatchReader reader = new ArrowPartialFileBatchReader(writePath.asFile(), allocator,
                factories, new DefaultColumnSelection(1), writer.getOffsetProvider());

            // Read back batch 1 already
            batch = reader.readRetained(1);
            assertBatchDataDict(batch, chunkSize, 1);
            batch.release();

            // Write batch 2
            batch = fillBatchDict(writer.create(chunkSize), chunkSize, 2);
            writer.write(batch);
            batch.release();

            // Write batch 3
            batch = fillBatchDict(writer.create(chunkSize), chunkSize, 3);
            writer.write(batch);
            batch.release();

            // Read back batch 0
            batch = reader.readRetained(0);
            assertBatchDataDict(batch, chunkSize, 0);
            batch.release();

            // Read back batch 3
            batch = reader.readRetained(3);
            assertBatchDataDict(batch, chunkSize, 3);
            batch.release();

            // Write batch 4
            batch = fillBatchDict(writer.create(chunkSize), chunkSize, 4);
            writer.write(batch);
            batch.release();

            // Close the writer
            writer.close();

            // Read back batch 4
            batch = reader.readRetained(3);
            assertBatchDataDict(batch, chunkSize, 3);
            batch.release();

            // Close the reader
            reader.close();
        }
    }

    /**
     * Test reading from an Arrow file before it is completely written (including dictionaries).
     *
     * @throws IOException
     */
    @Test
    public void testPartialFileBatchReadable() throws IOException {
        final int chunkSize = 64;
        final ColumnarSchema schema = new DefaultColumnarSchema(DataSpec.intSpec(), DefaultDataTraits.EMPTY);

        try (final RootAllocator allocator = new RootAllocator()) {
            final ArrowColumnStoreFactory factory =
                new ArrowColumnStoreFactory(allocator, 0, allocator.getLimit(), ARROW_NO_COMPRESSION);

            // Use the write store to write some data
            @SuppressWarnings("resource")
            final var writeStore = factory.createStore(schema, writePath);

            assertEquals(0, writeStore.numBatches());
            assertThrows(IllegalStateException.class, writeStore::batchLength);

            @SuppressWarnings("resource")
            final BatchWriter writer = writeStore.getWriter();
            ReadBatch batch;

            // Write batch 0
            batch = fillBatch(writer.create(chunkSize), chunkSize, 0);
            writer.write(batch);
            batch.release();
            assertEquals(1, writeStore.numBatches());
            assertEquals(chunkSize, writeStore.batchLength());

            // Write batch 1
            batch = fillBatch(writer.create(chunkSize), chunkSize, 1);
            writer.write(batch);
            batch.release();
            assertEquals(2, writeStore.numBatches());

            // Create the partial readable
            @SuppressWarnings("resource")
            final ArrowPartialFileBatchReadable readable =
                factory.createPartialFileReadable(writePath.asPath(), writeStore.getOffsetProvider());
            @SuppressWarnings("resource")
            final RandomAccessBatchReader reader = readable.createRandomAccessReader();

            // Read back batch 1 already
            batch = reader.readRetained(1);
            assertBatchData(batch, chunkSize, 1);
            batch.release();

            // Write batch 2
            batch = fillBatch(writer.create(chunkSize), chunkSize, 2);
            writer.write(batch);
            batch.release();
            assertEquals(3, writeStore.numBatches());

            // Write batch 3
            batch = fillBatch(writer.create(chunkSize), chunkSize, 3);
            writer.write(batch);
            batch.release();
            assertEquals(4, writeStore.numBatches());

            // Read back batch 0
            batch = reader.readRetained(0);
            assertBatchData(batch, chunkSize, 0);
            batch.release();

            // Read back batch 3
            batch = reader.readRetained(3);
            assertBatchData(batch, chunkSize, 3);
            batch.release();

            // Write batch 4
            batch = fillBatch(writer.create(chunkSize), chunkSize, 4);
            writer.write(batch);
            batch.release();
            assertEquals(5, writeStore.numBatches());

            // Close the writer
            writer.close();

            // Read back batch 4
            batch = reader.readRetained(3);
            assertBatchData(batch, chunkSize, 3);
            batch.release();

            // Close the reader
            reader.close();

            assertEquals(5, writeStore.numBatches());
            assertEquals(chunkSize, writeStore.batchLength());

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
            assertEquals(chunkSize, readStore.batchLength());

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
            assertEquals(chunkSize, readStore.batchLength());
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

    /** Fill the given batch (consisting of one dict encoded column) with some random data */
    private static ReadBatch fillBatchDict(final WriteBatch batch, final int chunkSize, final long seed) {
        final DictionaryEncodedData data = (DictionaryEncodedData)batch.get(0);
        ArrowTestUtils.fillData(data, chunkSize, seed);
        return batch.close(chunkSize);
    }

    /**
     * Assert that the batch contains data written by {@link #fillBatchDict(WriteBatch, int, long)} with the same seed.
     */
    private static void assertBatchDataDict(final ReadBatch batch, final int chunkSize, final long seed) {
        final DictionaryEncodedData data = (DictionaryEncodedData)batch.get(0);
        ArrowTestUtils.checkData(data, chunkSize, seed);
    }
}
