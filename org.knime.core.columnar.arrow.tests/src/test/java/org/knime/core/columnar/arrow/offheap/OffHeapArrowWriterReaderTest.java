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
 *   Sep 26, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow.offheap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.knime.core.columnar.arrow.compress.ArrowCompressionUtil.ARROW_LZ4_FRAME_COMPRESSION;
import static org.knime.core.columnar.arrow.compress.ArrowCompressionUtil.ARROW_NO_COMPRESSION;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils;
import org.knime.core.columnar.arrow.ArrowTestUtils;
import org.knime.core.columnar.arrow.compress.ArrowCompression;
import org.knime.core.columnar.arrow.mmap.MappedMessageSerializerTestUtil;
import org.knime.core.columnar.arrow.offheap.OffHeapTestData.ComplexData;
import org.knime.core.columnar.arrow.offheap.OffHeapTestData.ComplexDataFactory;
import org.knime.core.columnar.arrow.offheap.OffHeapTestData.DictionaryEncodedData;
import org.knime.core.columnar.arrow.offheap.OffHeapTestData.DictionaryEncodedDataFactory;
import org.knime.core.columnar.arrow.offheap.OffHeapTestData.SimpleData;
import org.knime.core.columnar.arrow.offheap.OffHeapTestData.SimpleDataFactory;
import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.filter.DefaultColumnSelection;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.columnar.store.FileHandle;

/**
 * Test the ArrowBatchWriter and ArrowBatchReader.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public class OffHeapArrowWriterReaderTest {

    private RootAllocator m_alloc;

    private FileHandle m_path;

    /**
     * Create the root allocator for each test
     *
     * @throws IOException
     */
    @Before
    public void before() throws IOException {
        m_alloc = new RootAllocator();
        m_path = ArrowTestUtils.createTmpKNIMEArrowFileSupplier();
    }

    /**
     * Close the root allocator for each test
     *
     * @throws IOException
     */
    @After
    public void after() throws IOException {
        m_alloc.close();
        MappedMessageSerializerTestUtil.assertAllClosed();
        m_path.delete();
    }

    /**
     * Test writing and reading one column one batch of simple integer data.
     *
     * @throws IOException
     */
    @Test
    public void testSimpleReadWriteOneBatchOneColumn() throws IOException {
        final OffHeapArrowColumnDataFactory[] factories = new OffHeapArrowColumnDataFactory[]{new SimpleDataFactory()};
        testReadWrite(64, 64, 1, 1, factories, new SimpleDataChecker(false), ARROW_NO_COMPRESSION);
    }

    /**
     * Test compressed writing and reading one column one batch of simple integer data.
     *
     * @throws IOException
     */
    @Test
    public void testSimpleReadWriteOneBatchOneColumnCompressed() throws IOException {
        final OffHeapArrowColumnDataFactory[] factories = new OffHeapArrowColumnDataFactory[]{new SimpleDataFactory()};
        testReadWrite(64, 64, 1, 1, factories, new SimpleDataChecker(false), ARROW_LZ4_FRAME_COMPRESSION);
    }

    /**
     * Test writing and reading one column one batch of simple integer data with missing values.
     *
     * @throws IOException
     */
    @Test
    public void testSimpleReadWriteOneBatchOneColumnMissing() throws IOException {
        final OffHeapArrowColumnDataFactory[] factories = new OffHeapArrowColumnDataFactory[]{new SimpleDataFactory()};
        testReadWrite(64, 64, 1, 1, factories, new SimpleDataChecker(true), ARROW_NO_COMPRESSION);
    }

    /**
     * Test compressed writing and reading one column one batch of simple integer data with missing values.
     *
     * @throws IOException
     */
    @Test
    public void testSimpleReadWriteOneBatchOneColumnMissingCompressed() throws IOException {
        final OffHeapArrowColumnDataFactory[] factories = new OffHeapArrowColumnDataFactory[]{new SimpleDataFactory()};
        testReadWrite(64, 64, 1, 1, factories, new SimpleDataChecker(true), ARROW_LZ4_FRAME_COMPRESSION);
    }

    /**
     * Test writing and reading 10 columns one batch of simple integer data.
     *
     * @throws IOException
     */
    @Test
    public void testSimpleReadWriteOneBatchTenColumn() throws IOException {
        final int numColumns = 10;
        final OffHeapArrowColumnDataFactory[] factories = IntStream.range(0, numColumns).mapToObj(i -> new SimpleDataFactory())
            .toArray(OffHeapArrowColumnDataFactory[]::new);
        testReadWrite(64, 64, numColumns, 1, factories, new SimpleDataChecker(false), ARROW_NO_COMPRESSION);
    }

    /**
     * Test writing and reading 10 columns, 10 batches of simple integer data.
     *
     * @throws IOException
     */
    @Test
    public void testSimpleReadWriteTenBatchTenColumn() throws IOException {
        final int numColumns = 10;
        final OffHeapArrowColumnDataFactory[] factories = IntStream.range(0, numColumns).mapToObj(i -> new SimpleDataFactory())
            .toArray(OffHeapArrowColumnDataFactory[]::new);
        testReadWrite(64, 64, numColumns, 10, factories, new SimpleDataChecker(false), ARROW_NO_COMPRESSION);
    }

    /**
     * Test writing and reading one column one batch of dictionary encoded data.
     *
     * @throws IOException
     */
    @Test
    public void testDictionaryReadWriteOneBatchOneColumn() throws IOException {
        final OffHeapArrowColumnDataFactory[] factory = new OffHeapArrowColumnDataFactory[]{new DictionaryEncodedDataFactory()};
        testReadWrite(16, 16, 1, 1, factory, new DictionaryEncodedDataChecker(), ARROW_NO_COMPRESSION);
    }

    /**
     * Test compressed writing and reading one column one batch of dictionary encoded data.
     *
     * @throws IOException
     */
    @Test
    public void testDictionaryReadWriteOneBatchOneColumnCompressed() throws IOException {
        final OffHeapArrowColumnDataFactory[] factory = new OffHeapArrowColumnDataFactory[]{new DictionaryEncodedDataFactory()};
        testReadWrite(16, 16, 1, 1, factory, new DictionaryEncodedDataChecker(), ARROW_LZ4_FRAME_COMPRESSION);
    }

    /**
     * Test writing and reading 10 columns one batch of dictionary encoded data.
     *
     * @throws IOException
     */
    @Test
    public void testDictionaryReadWriteOneBatchTenColumn() throws IOException {
        final int numColumns = 10;
        final OffHeapArrowColumnDataFactory[] factories = IntStream.range(0, numColumns)
            .mapToObj(i -> new DictionaryEncodedDataFactory()).toArray(OffHeapArrowColumnDataFactory[]::new);
        testReadWrite(16, 16, numColumns, 1, factories, new DictionaryEncodedDataChecker(), ARROW_NO_COMPRESSION);
    }

    /**
     * Test writing and reading 10 columns, 10 batches of dictionary encoded data.
     *
     * @throws IOException
     */
    @Test
    public void testDictionaryReadWriteTenBatchTenColumn() throws IOException {
        final int numColumns = 10;
        final OffHeapArrowColumnDataFactory[] factories = IntStream.range(0, numColumns)
            .mapToObj(i -> new DictionaryEncodedDataFactory()).toArray(OffHeapArrowColumnDataFactory[]::new);
        testReadWrite(16, 16, numColumns, 10, factories, new DictionaryEncodedDataChecker(), ARROW_NO_COMPRESSION);
    }

    /**
     * Test writing and reading one column one batch of very complex data.
     *
     * @throws IOException
     */
    @Test
    public void testComplexReadWriteOneBatchOneColumn() throws IOException {
        final OffHeapArrowColumnDataFactory[] factories = new OffHeapArrowColumnDataFactory[]{new ComplexDataFactory()};
        testReadWrite(64, 64, 1, 1, factories, new ComplexDataChecker(), ARROW_NO_COMPRESSION);
    }

    /**
     * Test writing and reading 10 columns one batch of very complex data.
     *
     * @throws IOException
     */
    @Test
    public void testComplexReadWriteOneBatchTenColumn() throws IOException {
        final int numColumns = 10;
        final OffHeapArrowColumnDataFactory[] factories = IntStream.range(0, numColumns)
            .mapToObj(i -> new ComplexDataFactory()).toArray(OffHeapArrowColumnDataFactory[]::new);
        testReadWrite(64, 64, numColumns, 1, factories, new ComplexDataChecker(), ARROW_NO_COMPRESSION);
    }

    /**
     * Test writing and reading 10 columns, 10 batches of very complex data.
     *
     * @throws IOException
     */
    @Test
    public void testComplexReadWrite3Batch5Column() throws IOException {
        final int numColumns = 5;
        final OffHeapArrowColumnDataFactory[] factories = IntStream.range(0, numColumns)
            .mapToObj(i -> new ComplexDataFactory()).toArray(OffHeapArrowColumnDataFactory[]::new);
        testReadWrite(8, 8, numColumns, 3, factories, new ComplexDataChecker(), ARROW_NO_COMPRESSION);
    }

    /**
     * Test writing a shorter batch after a longer batch.
     *
     * @throws IOException
     */
    @Test
    public void testSimpleShortChunkReadWrite() throws IOException {
        final OffHeapArrowColumnDataFactory[] factories = new OffHeapArrowColumnDataFactory[]{new SimpleDataFactory()};
        final SimpleDataChecker dataChecker = new SimpleDataChecker(false);

        // Create the data
        final SimpleData data1 = createWrite(factories[0], 64);
        dataChecker.fillData(data1, 0, 64, 0);
        final ReadBatch batch1 = new DefaultReadBatch(new NullableReadData[]{data1});

        final SimpleData data2 = createWrite(factories[0], 64);
        dataChecker.fillData(data2, 0, 13, 1);
        final ReadBatch batch2 = new DefaultReadBatch(new NullableReadData[]{data2});

        // Write the data to a file
        try (final OffHeapArrowBatchWriter writer = new OffHeapArrowBatchWriter(m_path, factories, ARROW_NO_COMPRESSION, m_alloc)) {
            writer.write(batch1);
            writer.write(batch2);
        }
        batch1.release();
        batch2.release();

        // Read the data from the file
        try (final OffHeapArrowBatchReader reader =
            new OffHeapArrowBatchReader(m_path.asFile(), m_alloc, factories, new DefaultColumnSelection(1))) {

            // Batch 1
            final ReadBatch batch1Read = reader.readRetained(0);
            assertEquals(64, batch1Read.length());
            final NullableReadData data1Read = batch1Read.get(0);
            dataChecker.checkData(data1Read, 0, 64, 0);
            batch1Read.release();

            // Batch 2
            final ReadBatch batch2Read = reader.readRetained(1);
            assertEquals(13, batch2Read.length());
            final NullableReadData data2Read = batch2Read.get(0);
            dataChecker.checkData(data2Read, 0, 13, 1);
            batch2Read.release();
        }
        assertEquals(0, m_alloc.getAllocatedMemory());
    }

    /**
     * Test random access.
     *
     * @throws IOException
     */
    @Test
    public void testSimpleRandomAccessRead() throws IOException {
        final int numBatches = 10;
        final int numColumns = 5;
        final int capacity = 32;
        final int dataCount = 32;
        final OffHeapArrowColumnDataFactory[] factories = IntStream.range(0, numColumns).mapToObj(i -> new SimpleDataFactory())
            .toArray(OffHeapArrowColumnDataFactory[]::new);
        final SimpleDataChecker dataChecker = new SimpleDataChecker(false);

        // Create the data
        final ReadBatch[] batches = new ReadBatch[numBatches];
        for (int b = 0; b < numBatches; b++) {
            final NullableReadData[] data1 = new NullableReadData[numColumns];
            for (int c = 0; c < numColumns; c++) {
                final NullableWriteData d = createWrite(factories[c], capacity);
                data1[c] = dataChecker.fillData(d, c, dataCount, (long)b * c);
            }
            batches[b] = new DefaultReadBatch(data1);
        }

        // Write the data to a file
        try (final OffHeapArrowBatchWriter writer = new OffHeapArrowBatchWriter(m_path, factories, ARROW_NO_COMPRESSION, m_alloc)) {
            for (ReadBatch b : batches) {
                writer.write(b);
            }
        }
        Arrays.stream(batches).forEach(ReadBatch::release);

        // Read only one batch of the data
        try (final OffHeapArrowBatchReader reader =
            new OffHeapArrowBatchReader(m_path.asFile(), m_alloc, factories, new DefaultColumnSelection(numColumns))) {
            final int b = 7;
            final ReadBatch batch = reader.readRetained(b);
            for (int c = 0; c < numColumns; c++) {
                final NullableReadData data = batch.get(c);
                dataChecker.checkData(data, c, dataCount, (long)b * c);
            }
            batch.release();
        }
        assertEquals(0, m_alloc.getAllocatedMemory());
    }

    /**
     * Test reading a file twice and check that the results point to the same memory.
     *
     * @throws IOException
     */
    @Test
    public void testReadSameFileTwice() throws IOException {
        final int numBatches = 10;
        final int numColumns = 5;
        final int capacity = 32;
        final int dataCount = 32;
        final OffHeapArrowColumnDataFactory[] factories = IntStream.range(0, numColumns).mapToObj(i -> new SimpleDataFactory())
            .toArray(OffHeapArrowColumnDataFactory[]::new);
        final SimpleDataChecker dataChecker = new SimpleDataChecker(false);

        // Create the data
        final ReadBatch[] batches = new ReadBatch[numBatches];
        for (int b = 0; b < numBatches; b++) {
            final NullableReadData[] data1 = new NullableReadData[numColumns];
            for (int c = 0; c < numColumns; c++) {
                final NullableWriteData d = createWrite(factories[c], capacity);
                data1[c] = dataChecker.fillData(d, c, dataCount, (long)b * c);
            }
            batches[b] = new DefaultReadBatch(data1);
        }

        // Write the data to a file
        try (final OffHeapArrowBatchWriter writer = new OffHeapArrowBatchWriter(m_path, factories, ARROW_NO_COMPRESSION, m_alloc)) {
            for (ReadBatch b : batches) {
                writer.write(b);
            }
        }
        Arrays.stream(batches).forEach(ReadBatch::release);

        // Read one batch and keep it
        final int b = 7;
        ReadBatch batch0;
        try (final OffHeapArrowBatchReader reader =
            new OffHeapArrowBatchReader(m_path.asFile(), m_alloc, factories, new FilteredColumnSelection(numColumns, 1, 2))) {
            batch0 = reader.readRetained(b);
        }

        // Read another selection of the same batch
        ReadBatch batch1;
        try (final OffHeapArrowBatchReader reader =
            new OffHeapArrowBatchReader(m_path.asFile(), m_alloc, factories, new FilteredColumnSelection(numColumns, 1, 3))) {
            batch1 = reader.readRetained(b);

        }

        // Check the data
        final SimpleData data01 = (SimpleData)batch0.get(1);
        dataChecker.checkData(data01, 1, dataCount, b);
        dataChecker.checkData(batch0.get(2), 2, dataCount, (long)b * 2);

        // Check the data
        final SimpleData data11 = (SimpleData)batch1.get(1);
        dataChecker.checkData(data11, 1, dataCount, b);
        dataChecker.checkData(batch1.get(3), 3, dataCount, (long)b * 3);

        // Check that data01 and data11 point to the same memory
        // and have the same reference manager
        @SuppressWarnings("resource")
        final ArrowBuf db01 = data01.getVector().getDataBuffer();
        @SuppressWarnings("resource")
        final ArrowBuf db11 = data11.getVector().getDataBuffer();
        assertSame(db01.getReferenceManager(), db11.getReferenceManager());
        assertEquals(db01.memoryAddress(), db11.memoryAddress());

        batch0.release();
        batch1.release();

        assertEquals(0, m_alloc.getAllocatedMemory());
    }

    /**
     * Test read filtered columns.
     *
     * @throws IOException
     */
    @Test
    public void testFilteredRead() throws IOException {
        final int numBatches = 2;
        final int numColumns = 5;
        final int capacity = 32;
        final int dataCount = 32;
        final OffHeapArrowColumnDataFactory[] factories =
            new OffHeapArrowColumnDataFactory[]{new SimpleDataFactory(), new SimpleDataFactory(), new ComplexDataFactory(),
                new ComplexDataFactory(), new DictionaryEncodedDataFactory()};
        final DataChecker[] dataChecker = new DataChecker[]{new SimpleDataChecker(false), new SimpleDataChecker(false),
            new ComplexDataChecker(), new ComplexDataChecker(), new DictionaryEncodedDataChecker()};

        // Create the data
        final ReadBatch[] batches = new ReadBatch[numBatches];
        for (int b = 0; b < numBatches; b++) {
            final NullableReadData[] data1 = new NullableReadData[numColumns];
            for (int c = 0; c < numColumns; c++) {
                final NullableWriteData d = createWrite(factories[c], capacity);
                data1[c] = dataChecker[c].fillData(d, c, dataCount, (long)b * (c + 1));
            }
            batches[b] = new DefaultReadBatch(data1);
        }

        // Write the data to a file
        try (final OffHeapArrowBatchWriter writer = new OffHeapArrowBatchWriter(m_path, factories, ARROW_NO_COMPRESSION, m_alloc)) {
            for (ReadBatch b : batches) {
                writer.write(b);
            }
        }
        Arrays.stream(batches).forEach(ReadBatch::release);

        // Read only some columns
        try (final OffHeapArrowBatchReader reader =
            new OffHeapArrowBatchReader(m_path.asFile(), m_alloc, factories, new FilteredColumnSelection(numColumns, 0, 2))) {
            assertEquals(numBatches, reader.numBatches());
            for (int b = 0; b < numBatches; b++) {
                final ReadBatch batch = reader.readRetained(b);
                assertEquals(capacity, batch.length());

                NullableReadData data = batch.get(0);
                dataChecker[0].checkData(data, 0, dataCount, (long)b * 1);

                data = batch.get(2);
                dataChecker[2].checkData(data, 2, dataCount, (long)b * 3);

                assertThrows(NoSuchElementException.class, () -> batch.get(1));
                assertThrows(NoSuchElementException.class, () -> batch.get(3));
                assertThrows(NoSuchElementException.class, () -> batch.get(4));

                batch.release();
            }
        }
        assertEquals(0, m_alloc.getAllocatedMemory());
    }

    /**
     * Test read filtered columns of dictionary encoded data.
     *
     * @throws IOException
     */
    @Test
    public void testDictioaryFilteredRead() throws IOException {
        final int numBatches = 2;
        final int numColumns = 5;
        final int capacity = 32;
        final int dataCount = 32;
        final OffHeapArrowColumnDataFactory[] factories = IntStream.range(0, numColumns)
            .mapToObj(i -> new DictionaryEncodedDataFactory()).toArray(OffHeapArrowColumnDataFactory[]::new);
        final DictionaryEncodedDataChecker dataChecker = new DictionaryEncodedDataChecker();

        // Create the data
        final ReadBatch[] batches = new ReadBatch[numBatches];
        for (int b = 0; b < numBatches; b++) {
            final NullableReadData[] data1 = new NullableReadData[numColumns];
            for (int c = 0; c < numColumns; c++) {
                final NullableWriteData d = createWrite(factories[c], capacity);
                data1[c] = dataChecker.fillData(d, c, dataCount, (long)b * (c + 1));
            }
            batches[b] = new DefaultReadBatch(data1);
        }

        // Write the data to a file
        try (final OffHeapArrowBatchWriter writer = new OffHeapArrowBatchWriter(m_path, factories, ARROW_NO_COMPRESSION, m_alloc)) {
            for (ReadBatch b : batches) {
                writer.write(b);
            }
        }
        Arrays.stream(batches).forEach(ReadBatch::release);

        // Read only some columns
        try (final OffHeapArrowBatchReader reader =
            new OffHeapArrowBatchReader(m_path.asFile(), m_alloc, factories, new FilteredColumnSelection(numColumns, 0, 2))) {
            assertEquals(numBatches, reader.numBatches());
            for (int b = 0; b < numBatches; b++) {
                final ReadBatch batch = reader.readRetained(b);
                assertEquals(capacity, batch.length());
                NullableReadData data = batch.get(0);
                dataChecker.checkData(data, 0, dataCount, (long)b * 1);

                data = batch.get(2);
                dataChecker.checkData(data, 2, dataCount, (long)b * 3);

                assertThrows(NoSuchElementException.class, () -> batch.get(1));
                assertThrows(NoSuchElementException.class, () -> batch.get(3));
                assertThrows(NoSuchElementException.class, () -> batch.get(4));

                batch.release();
            }
        }
        assertEquals(0, m_alloc.getAllocatedMemory());
    }

    /**
     * Test writing and reading one column one batch of simple integer data with a given version
     *
     * @throws IOException
     */
    @Test
    public void testFactoryVersionReadWrite() throws IOException {
        final OffHeapArrowColumnDataFactory[] factories =
            new OffHeapArrowColumnDataFactory[]{new SimpleDataFactory(ArrowColumnDataFactoryVersion.version(15))};
        testReadWrite(64, 64, 1, 1, factories, new SimpleDataChecker(false), ARROW_NO_COMPRESSION);
    }

    /**
     * Test writing and reading one column one batch of simple integer data with a given version
     *
     * @throws IOException
     */
    @Test
    public void testFactoryComplexVersionReadWrite() throws IOException {
        final OffHeapArrowColumnDataFactory[] factories =
            new OffHeapArrowColumnDataFactory[]{new SimpleDataFactory(ArrowColumnDataFactoryVersion.version(1,
                ArrowColumnDataFactoryVersion.version(2, ArrowColumnDataFactoryVersion.version(3),
                    ArrowColumnDataFactoryVersion.version(4)),
                ArrowColumnDataFactoryVersion.version(6),
                ArrowColumnDataFactoryVersion.version(17, ArrowColumnDataFactoryVersion.version(7))))};
        testReadWrite(64, 64, 1, 1, factories, new SimpleDataChecker(false), ARROW_NO_COMPRESSION);
    }

    /**
     * Test writing and reading four columns three batches of different data.
     *
     * @throws IOException
     */
    @Test
    public void testReadWriteDifferentTypes() throws IOException {
        final OffHeapArrowColumnDataFactory[] factories = new OffHeapArrowColumnDataFactory[]{new SimpleDataFactory(),
            new SimpleDataFactory(), new DictionaryEncodedDataFactory(), new ComplexDataFactory()};
        testReadWrite(16, 16, 4, 3, factories, new DataChecker() {

            private final DataChecker[] m_checkers = new DataChecker[]{new SimpleDataChecker(false),
                new SimpleDataChecker(true), new DictionaryEncodedDataChecker(), new ComplexDataChecker()};

            @Override
            public NullableReadData fillData(final NullableWriteData data, final int columnIndex, final int count,
                final long seed) {
                return m_checkers[columnIndex].fillData(data, columnIndex, count, seed);
            }

            @Override
            public void checkData(final NullableReadData data, final int columnIndex, final int count,
                final long seed) {
                m_checkers[columnIndex].checkData(data, columnIndex, count, seed);
            }
        }, ARROW_NO_COMPRESSION);
    }

    /**
     * Test that the batch boundaries we find in the metadata when reading matches the size of batches we wrote
     *
     * @throws IOException
     */
    @Test
    public void testReadBatchBoundariesAfterWrite() throws IOException {
        // Write differently long batches
        final int numBatches = 4;
        final int numColumns = 5;
        int[] batchSizes = new int[]{4, 8, 16, 32};
        long[] batchBoundaries = new long[]{4, 4 + 8, 4 + 8 + 16, 4 + 8 + 16 + 32};
        final OffHeapArrowColumnDataFactory[] factories =
            new OffHeapArrowColumnDataFactory[]{new SimpleDataFactory(), new SimpleDataFactory(), new ComplexDataFactory(),
                new ComplexDataFactory(), new DictionaryEncodedDataFactory()};
        final DataChecker[] dataChecker = new DataChecker[]{new SimpleDataChecker(false), new SimpleDataChecker(false),
            new ComplexDataChecker(), new ComplexDataChecker(), new DictionaryEncodedDataChecker()};

        // Create the data
        final ReadBatch[] batches = new ReadBatch[numBatches];
        for (int b = 0; b < numBatches; b++) {
            final NullableReadData[] data1 = new NullableReadData[numColumns];
            for (int c = 0; c < numColumns; c++) {
                final NullableWriteData d = createWrite(factories[c], batchSizes[b]);
                data1[c] = dataChecker[c].fillData(d, c, batchSizes[b], (long)b * (c + 1));
            }
            batches[b] = new DefaultReadBatch(data1);
        }

        // Write the data to a file
        try (final OffHeapArrowBatchWriter writer = new OffHeapArrowBatchWriter(m_path, factories, ARROW_NO_COMPRESSION, m_alloc)) {
            for (ReadBatch b : batches) {
                writer.write(b);
            }
        }
        Arrays.stream(batches).forEach(ReadBatch::release);

        // Read metadata from reader
        try (final OffHeapArrowBatchReader reader =
            new OffHeapArrowBatchReader(m_path.asFile(), m_alloc, factories, new DefaultColumnSelection(numColumns))) {
            assertEquals(numBatches, reader.numBatches());
            assertTrue(reader.getMetadata().containsKey(ArrowReaderWriterUtils.ARROW_BATCH_BOUNDARIES_KEY));
            var readBatchBoundaries = ArrowReaderWriterUtils
                .stringToLongArray(reader.getMetadata().get(ArrowReaderWriterUtils.ARROW_BATCH_BOUNDARIES_KEY));
            assertArrayEquals(batchBoundaries, readBatchBoundaries);

            for (int b = 0; b < numBatches; b++) {
                final ReadBatch batch = reader.readRetained(b);
                assertEquals(batchSizes[b], batch.length());
                batch.release();
            }
        }

        try (var store = new OffHeapArrowBatchReadStore(m_path.asPath(), m_alloc)) {
            assertArrayEquals(batchBoundaries, store.getBatchBoundaries());
        }

        assertEquals(0, m_alloc.getAllocatedMemory());
    }

    /**
     * Test writing and reading the data with the given factories. The dataChecker is used to set the values of the data
     * and check them after reading.
     */
    private void testReadWrite(final int capacity, final int dataCount, final int numColumns, final int numBatches,
        final OffHeapArrowColumnDataFactory[] factories, final DataChecker dataChecker, final ArrowCompression compression)
        throws IOException {

        // Create the data
        final ReadBatch[] batches = new ReadBatch[numBatches];
        for (int b = 0; b < numBatches; b++) {
            final NullableReadData[] data1 = new NullableReadData[numColumns];
            for (int c = 0; c < numColumns; c++) {
                final NullableWriteData d = createWrite(factories[c], capacity);
                data1[c] = dataChecker.fillData(d, c, dataCount, (long)b * c);
            }
            batches[b] = new DefaultReadBatch(data1);
        }

        // Write the data to a file
        try (final OffHeapArrowBatchWriter writer = new OffHeapArrowBatchWriter(m_path, factories, compression, m_alloc)) {
            for (ReadBatch b : batches) {
                writer.write(b);
            }
        }
        Arrays.stream(batches).forEach(ReadBatch::release);

        // Read the data from the file
        try (final OffHeapArrowBatchReader reader =
            new OffHeapArrowBatchReader(m_path.asFile(), m_alloc, factories, new DefaultColumnSelection(numColumns))) {
            assertEquals(numBatches, reader.numBatches());
            for (int b = 0; b < numBatches; b++) {
                final ReadBatch batch = reader.readRetained(b);
                assertEquals(capacity, batch.length());
                for (int c = 0; c < numColumns; c++) {
                    final NullableReadData data = batch.get(c);
                    dataChecker.checkData(data, c, dataCount, (long)b * c);
                }
                batch.release();
            }
        }
        assertEquals(0, m_alloc.getAllocatedMemory());
    }

    /** Create a {@link NullableWriteData} with the given factory */
    @SuppressWarnings({"unchecked", "resource"})
    private final <T extends NullableWriteData> T createWrite(final OffHeapArrowColumnDataFactory factory,
        final int numValues) {
        final long firstDictId = new Random().nextInt(1000);
        final AtomicLong dictId1 = new AtomicLong(firstDictId);
        final AtomicLong dictId2 = new AtomicLong(firstDictId);
        final Field field = factory.getField("0", dictId1::getAndIncrement);
        final FieldVector vector = field.createVector(m_alloc);
        return (T)factory.createWrite(vector, dictId2::getAndIncrement, m_alloc, numValues);
    }

    /**
     * An interface for classes which can fill a {@link NullableWriteData} object with values and can check if a
     * {@link NullableReadData} object contains the expected values.
     */
    private static interface DataChecker {

        /**
         * Set the values of a {@link NullableWriteData} object and close it.
         *
         * @param data the data object
         * @param columnIndex the index of the column. Can be used to handle different kinds of data in different
         *            columns
         * @param count the number of values to write
         * @param seed the seed defining the values
         * @return the {@link NullableReadData} for this {@link NullableWriteData}
         */
        NullableReadData fillData(NullableWriteData data, final int columnIndex, final int count, final long seed);

        /**
         * Check the values in the {@link NullableReadData}.
         *
         * @param data the data object
         * @param columnIndex the index of the column. Can be used to handle different kinds of data in different
         *            columns
         * @param count the number of values to check
         * @param seed the seed defining the values
         */
        void checkData(NullableReadData data, final int columnIndex, final int count, final long seed);
    }

    private static final class SimpleDataChecker implements DataChecker {

        private final boolean m_checkMissing;

        private SimpleDataChecker(final boolean checkMissing) {
            m_checkMissing = checkMissing;
        }

        @Override
        public NullableReadData fillData(final NullableWriteData data, final int columnIndex, final int count,
            final long seed) {
            assertTrue(data instanceof SimpleData);
            return OffHeapTestData.fillData((SimpleData)data, m_checkMissing, count, seed);
        }

        @Override
        public void checkData(final NullableReadData data, final int columnIndex, final int count, final long seed) {
            assertTrue(data instanceof SimpleData);
            OffHeapTestData.checkData((SimpleData)data, m_checkMissing, count, seed);
        }
    }

    private static final class DictionaryEncodedDataChecker implements DataChecker {

        @Override
        public NullableReadData fillData(final NullableWriteData data, final int columnIndex, final int count,
            final long seed) {
            assertTrue(data instanceof DictionaryEncodedData);
            return OffHeapTestData.fillData((DictionaryEncodedData)data, count, seed);
        }

        @Override
        public void checkData(final NullableReadData data, final int columnIndex, final int count, final long seed) {
            assertTrue(data instanceof DictionaryEncodedData);
            OffHeapTestData.checkData((DictionaryEncodedData)data, count, seed);
        }
    }

    private static final class ComplexDataChecker implements DataChecker {

        @Override
        public NullableReadData fillData(final NullableWriteData data, final int coulumnIndex, final int count,
            final long seed) {
            assertTrue(data instanceof ComplexData);
            return OffHeapTestData.fillData((ComplexData)data, count, seed);
        }

        @Override
        public void checkData(final NullableReadData data, final int columnIndex, final int count, final long seed) {
            assertTrue(data instanceof ComplexData);
            OffHeapTestData.checkData((ComplexData)data, count, seed);
        }
    }
}
