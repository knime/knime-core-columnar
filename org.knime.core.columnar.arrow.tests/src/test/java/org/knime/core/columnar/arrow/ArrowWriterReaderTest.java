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
package org.knime.core.columnar.arrow;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.knime.core.columnar.arrow.ArrowTestUtils.createTmpKNIMEArrowFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.columnar.arrow.ArrowTestUtils.ComplexData;
import org.knime.core.columnar.arrow.ArrowTestUtils.ComplexDataFactory;
import org.knime.core.columnar.arrow.ArrowTestUtils.DataChecker;
import org.knime.core.columnar.arrow.ArrowTestUtils.DictionaryEncodedData;
import org.knime.core.columnar.arrow.ArrowTestUtils.DictionaryEncodedDataFactory;
import org.knime.core.columnar.arrow.ArrowTestUtils.SimpleData;
import org.knime.core.columnar.arrow.ArrowTestUtils.SimpleDataFactory;
import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.filter.DefaultColumnSelection;
import org.knime.core.columnar.filter.FilteredColumnSelection;

/**
 * Test the ArrowColumnDataWriter and ArrowColumnDataReader.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public class ArrowWriterReaderTest {

    private RootAllocator m_alloc;

    private File m_file;

    /**
     * Create the root allocator for each test
     *
     * @throws IOException
     */
    @Before
    public void before() throws IOException {
        m_alloc = new RootAllocator();
        m_file = createTmpKNIMEArrowFile();
    }

    /**
     * Close the root allocator for each test
     *
     * @throws IOException
     */
    @After
    public void after() throws IOException {
        m_alloc.close();
        Files.delete(m_file.toPath());
    }

    /**
     * Test writing and reading one column one batch of simple integer data.
     *
     * @throws IOException
     */
    @Test
    public void testSimpleReadWriteOneBatchOneColumn() throws IOException {
        final ArrowColumnDataFactory[] factories = new ArrowColumnDataFactory[]{new SimpleDataFactory()};
        testReadWrite(64, 64, 1, 1, factories, new SimpleDataChecker(false));
    }

    /**
     * Test writing and reading one column one batch of simple integer data with missing values.
     *
     * @throws IOException
     */
    @Test
    public void testSimpleReadWriteOneBatchOneColumnMissing() throws IOException {
        final ArrowColumnDataFactory[] factories = new ArrowColumnDataFactory[]{new SimpleDataFactory()};
        testReadWrite(64, 64, 1, 1, factories, new SimpleDataChecker(true));
    }

    /**
     * Test writing and reading 10 columns one batch of simple integer data.
     *
     * @throws IOException
     */
    @Test
    public void testSimpleReadWriteOneBatchTenColumn() throws IOException {
        final int numColumns = 10;
        final ArrowColumnDataFactory[] factories = IntStream.range(0, numColumns).mapToObj(i -> new SimpleDataFactory())
            .toArray(ArrowColumnDataFactory[]::new);
        testReadWrite(64, 64, numColumns, 1, factories, new SimpleDataChecker(false));
    }

    /**
     * Test writing and reading 10 columns, 10 batches of simple integer data.
     *
     * @throws IOException
     */
    @Test
    public void testSimpleReadWriteTenBatchTenColumn() throws IOException {
        final int numColumns = 10;
        final ArrowColumnDataFactory[] factories = IntStream.range(0, numColumns).mapToObj(i -> new SimpleDataFactory())
            .toArray(ArrowColumnDataFactory[]::new);
        testReadWrite(64, 64, numColumns, 10, factories, new SimpleDataChecker(false));
    }

    /**
     * Test writing and reading one column one batch of dictionary encoded data.
     *
     * @throws IOException
     */
    @Test
    public void testDictionaryReadWriteOneBatchOneColumn() throws IOException {
        final ArrowColumnDataFactory[] factory = new ArrowColumnDataFactory[]{new DictionaryEncodedDataFactory()};
        testReadWrite(16, 16, 1, 1, factory, new DictionaryEncodedDataChecker());
    }

    /**
     * Test writing and reading 10 columns one batch of dictionary encoded data.
     *
     * @throws IOException
     */
    @Test
    public void testDictionaryReadWriteOneBatchTenColumn() throws IOException {
        final int numColumns = 10;
        final ArrowColumnDataFactory[] factories = IntStream.range(0, numColumns)
            .mapToObj(i -> new DictionaryEncodedDataFactory()).toArray(ArrowColumnDataFactory[]::new);
        testReadWrite(16, 16, numColumns, 1, factories, new DictionaryEncodedDataChecker());
    }

    /**
     * Test writing and reading 10 columns, 10 batches of dictionary encoded data.
     *
     * @throws IOException
     */
    @Test
    public void testDictionaryReadWriteTenBatchTenColumn() throws IOException {
        final int numColumns = 10;
        final ArrowColumnDataFactory[] factories = IntStream.range(0, numColumns)
            .mapToObj(i -> new DictionaryEncodedDataFactory()).toArray(ArrowColumnDataFactory[]::new);
        testReadWrite(16, 16, numColumns, 10, factories, new DictionaryEncodedDataChecker());
    }

    /**
     * Test writing and reading one column one batch of very complex data.
     *
     * @throws IOException
     */
    @Test
    public void testComplexReadWriteOneBatchOneColumn() throws IOException {
        final ArrowColumnDataFactory[] factories = new ArrowColumnDataFactory[]{new ComplexDataFactory()};
        testReadWrite(64, 64, 1, 1, factories, new ComplexDataChecker());
    }

    /**
     * Test writing and reading 10 columns one batch of very complex data.
     *
     * @throws IOException
     */
    @Test
    public void testComplexReadWriteOneBatchTenColumn() throws IOException {
        final int numColumns = 10;
        final ArrowColumnDataFactory[] factories = IntStream.range(0, numColumns)
            .mapToObj(i -> new ComplexDataFactory()).toArray(ArrowColumnDataFactory[]::new);
        testReadWrite(64, 64, numColumns, 1, factories, new ComplexDataChecker());
    }

    /**
     * Test writing and reading 10 columns, 10 batches of very complex data.
     *
     * @throws IOException
     */
    @Test
    public void testComplexReadWrite3Batch5Column() throws IOException {
        final int numColumns = 5;
        final ArrowColumnDataFactory[] factories = IntStream.range(0, numColumns)
            .mapToObj(i -> new ComplexDataFactory()).toArray(ArrowColumnDataFactory[]::new);
        testReadWrite(8, 8, numColumns, 3, factories, new ComplexDataChecker());
    }

    /**
     * Test writing a shorter batch after a longer batch.
     *
     * @throws IOException
     */
    @Test
    public void testSimpleShortChunkReadWrite() throws IOException {
        final ArrowColumnDataFactory[] factories = new ArrowColumnDataFactory[]{new SimpleDataFactory()};
        final SimpleDataChecker dataChecker = new SimpleDataChecker(false);

        // Create the data
        final SimpleData data1 = createWrite(factories[0], 64);
        dataChecker.fillData(data1, 0, 64, 0);
        final ReadBatch batch1 = new DefaultReadBatch(new ColumnReadData[]{data1}, 64);

        final SimpleData data2 = createWrite(factories[0], 64);
        dataChecker.fillData(data2, 0, 13, 1);
        final ReadBatch batch2 = new DefaultReadBatch(new ColumnReadData[]{data2}, 13);

        // Write the data to a file
        try (final ArrowColumnDataWriter writer = new ArrowColumnDataWriter(m_file, 64, factories)) {
            writer.write(batch1);
            writer.write(batch2);
        }
        batch1.release();
        batch2.release();

        // Read the data from the file
        try (final ArrowColumnDataReader reader =
            new ArrowColumnDataReader(m_file, m_alloc, factories, new DefaultColumnSelection(1))) {
            assertEquals(64, reader.getMaxLength());

            // Batch 1
            final ReadBatch batch1Read = reader.readRetained(0);
            assertEquals(64, batch1Read.length());
            final ColumnReadData data1Read = batch1Read.get(0);
            dataChecker.checkData(data1Read, 0, 64, 0);
            batch1Read.release();

            // Batch 2
            final ReadBatch batch2Read = reader.readRetained(1);
            assertEquals(13, batch2Read.length());
            final ColumnReadData data2Read = batch2Read.get(0);
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
        final ArrowColumnDataFactory[] factories = IntStream.range(0, numColumns).mapToObj(i -> new SimpleDataFactory())
            .toArray(ArrowColumnDataFactory[]::new);
        final SimpleDataChecker dataChecker = new SimpleDataChecker(false);

        // Create the data
        final ReadBatch[] batches = new ReadBatch[numBatches];
        for (int b = 0; b < numBatches; b++) {
            final ColumnReadData[] data1 = new ColumnReadData[numColumns];
            for (int c = 0; c < numColumns; c++) {
                final ColumnWriteData d = createWrite(factories[c], capacity);
                data1[c] = dataChecker.fillData(d, c, dataCount, (long)b * c);
            }
            batches[b] = new DefaultReadBatch(data1, dataCount);
        }

        // Write the data to a file
        try (final ArrowColumnDataWriter writer = new ArrowColumnDataWriter(m_file, capacity, factories)) {
            for (ReadBatch b : batches) {
                writer.write(b);
            }
        }
        Arrays.stream(batches).forEach(ReadBatch::release);

        // Read only one batch of the data
        try (final ArrowColumnDataReader reader =
            new ArrowColumnDataReader(m_file, m_alloc, factories, new DefaultColumnSelection(numColumns))) {
            final int b = 7;
            final ReadBatch batch = reader.readRetained(b);
            for (int c = 0; c < numColumns; c++) {
                final ColumnReadData data = batch.get(c);
                dataChecker.checkData(data, c, dataCount, (long)b * c);
            }
            batch.release();
        }
        assertEquals(0, m_alloc.getAllocatedMemory());
    }

    /**
     * Test read filtered columns.
     *
     * @throws IOException
     */
    @Test
    public void testSimpleFilteredRead() throws IOException {
        final int numBatches = 2;
        final int numColumns = 5;
        final int capacity = 32;
        final int dataCount = 32;
        final ArrowColumnDataFactory[] factories = IntStream.range(0, numColumns).mapToObj(i -> new SimpleDataFactory())
            .toArray(ArrowColumnDataFactory[]::new);
        final SimpleDataChecker dataChecker = new SimpleDataChecker(false);

        // Create the data
        final ReadBatch[] batches = new ReadBatch[numBatches];
        for (int b = 0; b < numBatches; b++) {
            final ColumnReadData[] data1 = new ColumnReadData[numColumns];
            for (int c = 0; c < numColumns; c++) {
                final ColumnWriteData d = createWrite(factories[c], capacity);
                data1[c] = dataChecker.fillData(d, c, dataCount, (long)b * (c + 1));
            }
            batches[b] = new DefaultReadBatch(data1, dataCount);
        }

        // Write the data to a file
        try (final ArrowColumnDataWriter writer = new ArrowColumnDataWriter(m_file, capacity, factories)) {
            for (ReadBatch b : batches) {
                writer.write(b);
            }
        }
        Arrays.stream(batches).forEach(ReadBatch::release);

        // Read only some columns
        try (final ArrowColumnDataReader reader =
            new ArrowColumnDataReader(m_file, m_alloc, factories, new FilteredColumnSelection(numColumns, 0, 2))) {
            assertEquals(numBatches, reader.getNumBatches());
            assertEquals(capacity, reader.getMaxLength());
            for (int b = 0; b < numBatches; b++) {
                final ReadBatch batch = reader.readRetained(b);

                ColumnReadData data = batch.get(0);
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
        final ArrowColumnDataFactory[] factories = IntStream.range(0, numColumns)
            .mapToObj(i -> new DictionaryEncodedDataFactory()).toArray(ArrowColumnDataFactory[]::new);
        final DictionaryEncodedDataChecker dataChecker = new DictionaryEncodedDataChecker();

        // Create the data
        final ReadBatch[] batches = new ReadBatch[numBatches];
        for (int b = 0; b < numBatches; b++) {
            final ColumnReadData[] data1 = new ColumnReadData[numColumns];
            for (int c = 0; c < numColumns; c++) {
                final ColumnWriteData d = createWrite(factories[c], capacity);
                data1[c] = dataChecker.fillData(d, c, dataCount, (long)b * (c + 1));
            }
            batches[b] = new DefaultReadBatch(data1, dataCount);
        }

        // Write the data to a file
        try (final ArrowColumnDataWriter writer = new ArrowColumnDataWriter(m_file, capacity, factories)) {
            for (ReadBatch b : batches) {
                writer.write(b);
            }
        }
        Arrays.stream(batches).forEach(ReadBatch::release);

        // Read only some columns
        try (final ArrowColumnDataReader reader =
            new ArrowColumnDataReader(m_file, m_alloc, factories, new FilteredColumnSelection(numColumns, 0, 2))) {
            assertEquals(numBatches, reader.getNumBatches());
            assertEquals(capacity, reader.getMaxLength());
            for (int b = 0; b < numBatches; b++) {
                final ReadBatch batch = reader.readRetained(b);

                ColumnReadData data = batch.get(0);
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
        final ArrowColumnDataFactory[] factories =
            new ArrowColumnDataFactory[]{new SimpleDataFactory(ArrowColumnDataFactoryVersion.version(15))};
        testReadWrite(64, 64, 1, 1, factories, new SimpleDataChecker(false));
    }

    /**
     * Test writing and reading one column one batch of simple integer data with a given version
     *
     * @throws IOException
     */
    @Test
    public void testFactoryComplexVersionReadWrite() throws IOException {
        final ArrowColumnDataFactory[] factories = new ArrowColumnDataFactory[]{new SimpleDataFactory(
            ArrowColumnDataFactoryVersion.version(1, ArrowColumnDataFactoryVersion.version(2, ArrowColumnDataFactoryVersion.version(3), ArrowColumnDataFactoryVersion.version(4)),
                ArrowColumnDataFactoryVersion.version(6), ArrowColumnDataFactoryVersion.version(17, ArrowColumnDataFactoryVersion.version(7))))};
        testReadWrite(64, 64, 1, 1, factories, new SimpleDataChecker(false));
    }

    /**
     * Test writing and reading four columns three batches of different data.
     *
     * @throws IOException
     */
    @Test
    public void testReadWriteDifferentTypes() throws IOException {
        final ArrowColumnDataFactory[] factories = new ArrowColumnDataFactory[]{new SimpleDataFactory(),
            new SimpleDataFactory(), new DictionaryEncodedDataFactory(), new ComplexDataFactory()};
        testReadWrite(16, 16, 4, 3, factories, new DataChecker() {

            private final DataChecker[] m_checkers = new DataChecker[]{new SimpleDataChecker(false),
                new SimpleDataChecker(true), new DictionaryEncodedDataChecker(), new ComplexDataChecker()};

            @Override
            public ColumnReadData fillData(final ColumnWriteData data, final int columnIndex, final int count,
                final long seed) {
                return m_checkers[columnIndex].fillData(data, columnIndex, count, seed);
            }

            @Override
            public void checkData(final ColumnReadData data, final int columnIndex, final int count, final long seed) {
                m_checkers[columnIndex].checkData(data, columnIndex, count, seed);
            }
        });
    }

    /**
     * Test writing and reading the data with the given factories. The dataChecker is used to set the values of the data
     * and check them after reading.
     */
    private void testReadWrite(final int capacity, final int dataCount, final int numColumns, final int numBatches,
        final ArrowColumnDataFactory[] factories, final DataChecker dataChecker) throws IOException {

        // Create the data
        final ReadBatch[] batches = new ReadBatch[numBatches];
        for (int b = 0; b < numBatches; b++) {
            final ColumnReadData[] data1 = new ColumnReadData[numColumns];
            for (int c = 0; c < numColumns; c++) {
                final ColumnWriteData d = createWrite(factories[c], capacity);
                data1[c] = dataChecker.fillData(d, c, dataCount, (long)b * c);
            }
            batches[b] = new DefaultReadBatch(data1, dataCount);
        }

        // Write the data to a file
        try (final ArrowColumnDataWriter writer = new ArrowColumnDataWriter(m_file, capacity, factories)) {
            for (ReadBatch b : batches) {
                writer.write(b);
            }
        }
        Arrays.stream(batches).forEach(ReadBatch::release);

        // Read the data from the file
        try (final ArrowColumnDataReader reader =
            new ArrowColumnDataReader(m_file, m_alloc, factories, new DefaultColumnSelection(numColumns))) {
            assertEquals(numBatches, reader.getNumBatches());
            assertEquals(capacity, reader.getMaxLength());
            for (int b = 0; b < numBatches; b++) {
                final ReadBatch batch = reader.readRetained(b);
                for (int c = 0; c < numColumns; c++) {
                    final ColumnReadData data = batch.get(c);
                    dataChecker.checkData(data, c, dataCount, (long)b * c);
                }
                batch.release();
            }
        }
        assertEquals(0, m_alloc.getAllocatedMemory());
    }

    /** Create a {@link ColumnWriteData} with the given factory */
    @SuppressWarnings({"unchecked", "resource"})
    private final <T extends ColumnWriteData> T createWrite(final ArrowColumnDataFactory factory, final int numValues) {
        final long firstDictId = new Random().nextInt(1000);
        final AtomicLong dictId1 = new AtomicLong(firstDictId);
        final AtomicLong dictId2 = new AtomicLong(firstDictId);
        final Field field = factory.getField("0", dictId1::getAndIncrement);
        final FieldVector vector = field.createVector(m_alloc);
        return (T)factory.createWrite(vector, dictId2::getAndIncrement, m_alloc, numValues);
    }

    private static final class SimpleDataChecker implements DataChecker {

        private final boolean m_checkMissing;

        private SimpleDataChecker(final boolean checkMissing) {
            m_checkMissing = checkMissing;
        }

        @Override
        @SuppressWarnings("resource")
        public ColumnReadData fillData(final ColumnWriteData data, final int columnIndex, final int count,
            final long seed) {
            assertTrue(data instanceof SimpleData);
            final SimpleData d = (SimpleData)data;
            final Random random = new Random(seed);
            for (int i = 0; i < count; i++) {
                if (m_checkMissing && random.nextDouble() < 0.4) {
                    d.setMissing(i);
                } else {
                    d.getVector().set(i, random.nextInt());
                }
            }
            return data.close(count);
        }

        @Override
        @SuppressWarnings("resource")
        public void checkData(final ColumnReadData data, final int columnIndex, final int count, final long seed) {
            assertTrue(data instanceof SimpleData);
            final SimpleData d = (SimpleData)data;
            final Random random = new Random(seed);
            assertEquals(count, d.getVector().getValueCount());
            for (int i = 0; i < count; i++) {
                if (m_checkMissing && random.nextDouble() < 0.4) {
                    assertTrue(d.isMissing(i));
                } else {
                    assertFalse(d.isMissing(i));
                    assertEquals(random.nextInt(), d.getVector().get(i));
                }
            }
        }
    }

    private static final class DictionaryEncodedDataChecker implements DataChecker {

        @Override
        @SuppressWarnings("resource")
        public ColumnReadData fillData(final ColumnWriteData data, final int columnIndex, final int count,
            final long seed) {
            assertTrue(data instanceof DictionaryEncodedData);
            final DictionaryEncodedData d = (DictionaryEncodedData)data;
            final Random random = new Random(seed);
            final BigIntVector dictVector = (BigIntVector)d.getDictionary().getVector();
            final IntVector vector = d.getVector();

            // Set the dictionary
            final int numDistinct = random.nextInt(10) + 5;
            dictVector.allocateNew(numDistinct);
            for (int i = 0; i < numDistinct; i++) {
                dictVector.set(i, random.nextLong());
            }
            dictVector.setValueCount(numDistinct);

            // Set the values to indices
            for (int i = 0; i < count; i++) {
                vector.set(i, random.nextInt(numDistinct));
            }
            return data.close(count);
        }

        @Override
        @SuppressWarnings("resource")
        public void checkData(final ColumnReadData data, final int columnIndex, final int count, final long seed) {
            assertTrue(data instanceof DictionaryEncodedData);
            final DictionaryEncodedData d = (DictionaryEncodedData)data;
            final Random random = new Random(seed);

            // The vector
            final IntVector vector = d.getVector();
            assertNotNull(vector.getField().getDictionary());

            // The dictionary
            final Dictionary dictionary = d.getDictionary();
            assertNotNull(dictionary);
            final DictionaryEncoding encoding = dictionary.getEncoding();
            assertNotNull(encoding);
            assertEquals(vector.getField().getDictionary().getId(), encoding.getId());
            assertFalse(encoding.isOrdered());
            assertEquals(MinorType.INT.getType(), encoding.getIndexType());
            final FieldVector dictVectorNotCasted = dictionary.getVector();
            assertNotNull(dictVectorNotCasted);
            assertTrue(dictVectorNotCasted instanceof BigIntVector);
            final BigIntVector dictVector = (BigIntVector)dictVectorNotCasted;

            // Check the dictionary
            final int numDistinct = random.nextInt(10) + 5;
            assertEquals(numDistinct, dictVector.getValueCount());
            for (int i = 0; i < numDistinct; i++) {
                assertFalse(dictVector.isNull(i));
                assertEquals(random.nextLong(), dictVector.get(i));
            }

            // Set the values to indices
            for (int i = 0; i < count; i++) {
                assertFalse(vector.isNull(i));
                assertEquals(random.nextInt(numDistinct), vector.get(i));
            }
        }
    }

    private static final class ComplexDataChecker implements DataChecker {

        @SuppressWarnings("resource")
        @Override
        public ColumnReadData fillData(final ColumnWriteData data, final int coulumnIndex, final int count,
            final long seed) {
            assertTrue(data instanceof ComplexData);
            final ComplexData d = (ComplexData)data;
            final Random random = new Random(seed);

            // Fill dictionary G:
            final int numDistinctG = random.nextInt(5) + 5;
            final VarBinaryVector dictG = d.getDictionaryVectorG();
            dictG.allocateNew(numDistinctG);
            for (int i = 0; i < numDistinctG; i++) {
                dictG.set(i, nextBytes(random));
            }
            dictG.setValueCount(numDistinctG);

            // Fill dictionary E:
            final int numDistinctE = random.nextInt(5) + 5;
            final VarBinaryVector dictE = d.getDictionaryVectorE();
            dictE.allocateNew(numDistinctE);
            for (int i = 0; i < numDistinctE; i++) {
                dictE.set(i, nextBytes(random));
            }
            dictE.setValueCount(numDistinctE);

            // Fill dictionary B:
            final int numDistinctB = random.nextInt(5) + 5;
            final StructVector dictB = d.getDictionaryVectorB();
            final IntVector vectorG = d.getVectorG();
            final IntVector vectorH = d.getVectorH();
            vectorG.allocateNew(numDistinctB);
            vectorH.allocateNew(numDistinctB);
            for (int i = 0; i < numDistinctB; i++) {
                dictB.setIndexDefined(i);
                vectorG.set(i, random.nextInt(numDistinctG));
                vectorH.set(i, random.nextInt());
            }
            vectorG.setValueCount(numDistinctB);
            vectorH.setValueCount(numDistinctB);
            dictB.setValueCount(numDistinctB);

            // Fill the struct vector
            final StructVector vector = d.getVector();
            final BigIntVector vectorA = d.getVectorA();
            final IntVector vectorB = d.getVectorB();
            final ListVector vectorC = d.getVectorC();
            final UnionListWriter writerC = vectorC.getWriter();
            final StructVector vectorD = d.getVectorD();
            final IntVector vectorE = d.getVectorE();
            final BitVector vectorF = d.getVectorF();
            vectorA.allocateNew(count);
            vectorB.allocateNew(count);
            vectorE.allocateNew(count);
            vectorF.allocateNew(count);
            writerC.allocate();
            for (int i = 0; i < count; i++) {
                vectorA.set(i, random.nextLong());
                vectorB.set(i, random.nextInt(numDistinctB));
                writerC.setPosition(i);
                writerC.startList();
                for (int j = 0; j < random.nextInt(4); j++) {
                    writerC.integer().writeInt(random.nextInt());
                }
                writerC.endList();
                vectorE.set(i, random.nextInt(numDistinctE));
                vectorF.set(i, random.nextBoolean() ? 1 : 0);
                vectorD.setIndexDefined(i);
                vector.setIndexDefined(i);
            }
            writerC.setValueCount(count);
            return d.close(count);
        }

        private static final byte[] nextBytes(final Random random) {
            final byte[] bytes = new byte[random.nextInt(10) + 5];
            random.nextBytes(bytes);
            return bytes;
        }

        @SuppressWarnings("resource")
        @Override
        public void checkData(final ColumnReadData data, final int columnIndex, final int count, final long seed) {
            assertTrue(data instanceof ComplexData);
            final ComplexData d = (ComplexData)data;
            final Random random = new Random(seed);

            // Check dictionary G:
            final int numDistinctG = random.nextInt(5) + 5;
            final VarBinaryVector dictG = d.getDictionaryVectorG();
            assertNotNull(dictG);
            assertEquals(numDistinctG, dictG.getValueCount());
            for (int i = 0; i < numDistinctG; i++) {
                assertFalse(dictG.isNull(i));
                assertArrayEquals(nextBytes(random), dictG.get(i));
            }

            // Check dictionary E:
            final int numDistinctE = random.nextInt(5) + 5;
            final VarBinaryVector dictE = d.getDictionaryVectorE();
            assertNotNull(dictE);
            assertEquals(numDistinctE, dictE.getValueCount());
            for (int i = 0; i < numDistinctE; i++) {
                assertFalse(dictE.isNull(i));
                assertArrayEquals(nextBytes(random), dictE.get(i));
            }

            // Check dictionary B:
            final int numDistinctB = random.nextInt(5) + 5;
            final StructVector dictB = d.getDictionaryVectorB();
            assertNotNull(dictB);
            assertEquals(numDistinctB, dictB.getValueCount());
            final IntVector vectorG = d.getVectorG();
            assertNotNull(vectorG);
            assertEquals(numDistinctB, vectorG.getValueCount());
            final IntVector vectorH = d.getVectorH();
            assertNotNull(vectorH);
            assertEquals(numDistinctB, vectorH.getValueCount());
            for (int i = 0; i < numDistinctB; i++) {
                assertFalse(dictB.isNull(i));
                assertFalse(vectorG.isNull(i));
                assertFalse(vectorH.isNull(i));
                assertEquals(random.nextInt(numDistinctG), vectorG.get(i));
                assertEquals(random.nextInt(), vectorH.get(i));
            }

            // Fill the struct vector
            final StructVector vector = d.getVector();
            assertNotNull(vector);
            assertEquals(count, vector.getValueCount());
            final BigIntVector vectorA = d.getVectorA();
            assertNotNull(vectorA);
            assertEquals(count, vectorA.getValueCount());
            final IntVector vectorB = d.getVectorB();
            assertNotNull(vectorB);
            assertEquals(count, vectorB.getValueCount());
            final ListVector vectorC = d.getVectorC();
            assertNotNull(vectorC);
            assertEquals(count, vectorC.getValueCount());
            final StructVector vectorD = d.getVectorD();
            assertNotNull(vectorD);
            assertEquals(count, vectorD.getValueCount());
            final IntVector vectorE = d.getVectorE();
            assertNotNull(vectorE);
            assertEquals(count, vectorE.getValueCount());
            final BitVector vectorF = d.getVectorF();
            assertNotNull(vectorF);
            assertEquals(count, vectorF.getValueCount());
            for (int i = 0; i < count; i++) {
                assertFalse(vector.isNull(i));
                assertFalse(vectorA.isNull(i));
                assertEquals(random.nextLong(), vectorA.get(i));
                assertFalse(vectorB.isNull(i));
                assertEquals(random.nextInt(numDistinctB), vectorB.get(i));
                final Object valuesC = vectorC.getObject(i);
                assertTrue(valuesC instanceof List);
                @SuppressWarnings("unchecked")
                final List<Integer> valuesCList = (List<Integer>)valuesC;
                for (int j = 0; j < random.nextInt(4); j++) {
                    assertEquals(random.nextInt(), (int)valuesCList.get(j));
                }
                assertFalse(vectorD.isNull(i));
                assertFalse(vectorE.isNull(i));
                assertEquals(random.nextInt(numDistinctE), vectorE.get(i));
                assertFalse(vectorF.isNull(i));
                assertEquals(random.nextBoolean(), vectorF.get(i) == 1);
            }
        }
    }
}
