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
package org.knime.core.columnar.arrow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.columnar.arrow.compress.ArrowCompression;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.columnar.arrow.data.ArrowReadData;
import org.knime.core.columnar.arrow.data.ArrowWriteData;
import org.knime.core.columnar.arrow.mmap.MappedMessageSerializerTestUtil;
import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.ObjectData.ObjectDataSerializer;
import org.knime.core.columnar.filter.DefaultColumnSelection;

/**
 * Abstract test for simple Arrow {@link NullableReadData}, {@link NullableWriteData} implementations.
 *
 * @param <W> type of the {@link NullableWriteData} implementation
 * @param <R> type of the {@link NullableReadData} implementation
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractArrowDataTest<W extends ArrowWriteData, R extends ArrowReadData> {

    private static final ArrowCompression COMPRESSION_CONFIG = ArrowCompressionUtil.ARROW_NO_COMPRESSION;

    /** The factory of the data implementation */
    protected final ArrowColumnDataFactory m_factory;

    /** The allocator (initialized in {@link #before()} and closed in {@link #after()}) */
    protected RootAllocator m_alloc;

    /**
     * Create a test for Arrow data implementations.
     *
     * @param factory the factory which should be used to create the data objects
     */
    protected AbstractArrowDataTest(final ArrowColumnDataFactory factory) {
        m_factory = factory;
    }

    /**
     * Check if the object is of class W an cast.
     *
     * @param o an object
     * @return the cast object
     */
    protected abstract W castW(Object o);

    /**
     * Check if the object is of class R an cast.
     *
     * @param o an object
     * @return the cast object
     */
    protected abstract R castR(Object o);

    /**
     * Set the value at the given index using the seed.
     *
     * @param data the data object
     * @param index the index
     * @param seed the seed
     */
    protected abstract void setValue(W data, int index, int seed);

    /**
     * Check the value at the given index using the seed. Should be the same as set in
     * {@link #setValue(ArrowWriteData, int, int)}.
     *
     * @param data the data object
     * @param index the index
     * @param seed the seed
     */
    protected abstract void checkValue(R data, int index, int seed);

    /**
     * @param data the data object
     * @return if the data object has been released
     */
    protected abstract boolean isReleasedW(W data);

    /**
     * @param data the data object
     * @return if the data object has been released
     */
    protected abstract boolean isReleasedR(R data);

    /**
     * @param valueCount the count of values added to the data (with seeds 0..(valueCount-1))
     * @param capacity the total capacity of the data
     * @return the minimum size that should be allocated by data object to store the data
     */
    protected abstract long getMinSize(int valueCount, int capacity);

    /**
     * Simple serializer implementation which adds one to the bytes on serialization and removes 1 when deserializing
     * again.
     */
    protected static final class DummyByteArraySerializer implements ObjectDataSerializer<byte[]> {

        @SuppressWarnings("javadoc")
        public static final DummyByteArraySerializer INSTANCE = new DummyByteArraySerializer();

        private DummyByteArraySerializer() {
        }

        @Override
        public void serialize(final byte[] obj, final DataOutput output) throws IOException {
            final byte[] modifiedCopy = obj.clone();
            output.writeInt(obj.length);
            for (int i = 0; i < modifiedCopy.length; i++) {
                modifiedCopy[i]++;
            }
            output.write(modifiedCopy);
        }

        @Override
        public byte[] deserialize(final DataInput input) throws IOException {
            final byte[] obj = new byte[input.readInt()];
            input.readFully(obj);
            for (int i = 0; i < obj.length; i++) {
                obj[i]--;
            }
            return obj;
        }
    }

    /** Initialize the root allocator before running a test */
    @Before
    public void before() {
        final int segmentSize = 4;
        m_alloc = new RootAllocator(AllocationListener.NOOP, Long.MAX_VALUE,
            requestSize -> (requestSize + (segmentSize - 1)) / segmentSize * segmentSize);
    }

    /** Close the root allocator after running a test */
    @After
    public void after() {
        MappedMessageSerializerTestUtil.assertAllClosed();
        m_alloc.close();
    }

    /**
     * Create a ColumnWriteData using the saved factory.
     *
     * @param numValues the number of values to allocate
     * @return the {@link NullableWriteData}
     */
    protected W createWrite(final int numValues) {
        return castW(ArrowColumnDataFactory.createWrite(m_factory, "0", m_alloc, numValues));
    }

    /** Test allocating some memory and setting and getting some values */
    @Test
    public void testAlloc() {
        int numValues = 64;
        final W writeData = createWrite(numValues);
        for (int i = 0; i < numValues; i++) {
            setValue(writeData, i, i);
        }
        final R readData = castR(writeData.close(numValues));
        for (int i = 0; i < numValues; i++) {
            checkValue(readData, i, i);
        }
        readData.release();
    }

    /** Test {@link W#capacity()} */
    @Test
    public void testCapacity() {
        W data = createWrite(12);
        assertTrue(data.capacity() >= 12);
        data.release();

        data = createWrite(16000);
        assertTrue(data.capacity() >= 16000);
        data.release();

        data = createWrite(68);
        assertTrue(data.capacity() >= 68);
        data.release();
    }

    /** Test {@link W#expand(int)} */
    @Test
    public void testExpand() {
        int size1 = 12;
        int size2 = 33;

        // Allocate
        W data = createWrite(size1);
        assertTrue(data.capacity() >= size1);

        // Write some data
        for (int i = 0; i < size1; i++) {
            setValue(data, i, i);
        }

        // Expand
        data.expand(size2);
        assertTrue(data.capacity() >= size2);

        // Write some more data
        for (int i = size1; i < size2; i++) {
            setValue(data, i, i);
        }

        // Check values written before and after expand
        final R readData = castR(data.close(size2));
        for (int i = 0; i < size2; i++) {
            checkValue(readData, i, i);
        }
        readData.release();
    }

    /** Test reading and writing of sliced data */
    @Test
    public void testSlice() {
        final int numValues = 32;
        final int sliceStart = 5;
        final int sliceLength = 10;

        final W writeData = createWrite(numValues);
        // Write into a slice
        final W slicedWrite = castW(writeData.slice(sliceStart));
        for (int i = 0; i < sliceLength; i++) {
            setValue(slicedWrite, i, i);
        }

        // Read everything
        final R readData = castR(writeData.close(numValues));
        for (int i = 0; i < numValues; i++) {
            if (i >= sliceStart && i < sliceStart + sliceLength) {
                // Inside the written slice
                assertFalse(readData.isMissing(i));
                checkValue(readData, i, i - sliceStart);
            } else {
                // Outside the written slice
                assertTrue(readData.isMissing(i));
            }
        }

        // Read only the slice
        final R slicedRead = castR(readData.slice(sliceStart, sliceLength));
        assertEquals(sliceLength, slicedRead.length());
        for (int i = 0; i < sliceLength; i++) {
            assertFalse(slicedRead.isMissing(i));
            checkValue(slicedRead, i, i);
        }

        readData.release();
    }

    /** Test slice of slice and writing of sliced data */
    @Test
    public void testSliceOfSlice() {
        final int numValues = 32;
        final int slice1Start = 5;
        final int slice2Start = 3;
        final int sliceLength = 5;

        final W writeData = createWrite(numValues);
        final W slicedWrite1 = castW(writeData.slice(slice1Start));
        final W slicedWrite2 = castW(slicedWrite1.slice(slice2Start));

        // Write into a slice
        for (int i = 0; i < sliceLength; i++) {
            setValue(slicedWrite2, i, i);
        }

        // Read everything
        final R readData = castR(writeData.close(numValues));
        for (int i = 0; i < numValues; i++) {
            if (i >= slice1Start + slice2Start && i < slice1Start + slice2Start + sliceLength) {
                // Inside the written slice
                assertFalse(readData.isMissing(i));
                checkValue(readData, i, i - slice1Start - slice2Start);
            } else {
                // Outside the written slice
                assertTrue(readData.isMissing(i));
            }
        }

        // Read only the slice
        final R slicedRead1 = castR(readData.slice(slice1Start, 10));
        final R slicedRead2 = castR(slicedRead1.slice(slice2Start, sliceLength));
        assertEquals(sliceLength, slicedRead2.length());
        for (int i = 0; i < sliceLength; i++) {
            assertFalse(slicedRead2.isMissing(i));
            checkValue(slicedRead2, i, i);
        }

        readData.release();
    }

    /**
     * Test reading different indices in R at the same time.
     *
     * @throws Exception
     */
    @Test
    public void testSynchronousRead() throws Exception {
        final int numValues = 64;
        final int nThreads = 32;

        final W writeData = createWrite(numValues);
        for (int i = 0; i < numValues; i++) {
            setValue(writeData, i, i);
        }

        // Create the tasks to check the read data
        final R readData = castR(writeData.close(numValues));
        final List<CheckReadDataCallable> checkTasks = IntStream.range(0, nThreads)
            .mapToObj(i -> new CheckReadDataCallable(readData, i, numValues)).collect(Collectors.toList());

        // Run the tasks in parallel
        final ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        final List<Future<Void>> futures = pool.invokeAll(checkTasks);
        for (final Future<Void> f : futures) {
            f.get();
        }

        readData.release();
    }

    private final class CheckReadDataCallable implements Callable<Void> {

        private final R m_readData;

        private final int m_startIndex;

        private final int m_numValues;

        public CheckReadDataCallable(final R readData, final int startIndex, final int numValues) {
            m_readData = readData;
            m_startIndex = startIndex;
            m_numValues = numValues;
        }

        @Override
        public Void call() {
            for (int i = m_startIndex; i < m_numValues; i++) {
                checkValue(m_readData, i, i);
            }
            return null;
        }
    }

    /** Test #retain() and #release() for W and R */
    @Test
    public void testReferenceCounting() {
        int numValues = 8;
        final W wd = createWrite(numValues);
        for (int i = 0; i < numValues; i++) {
            setValue(wd, i, i);
        }
        // wd: Reference count should be 1
        assertFalse(isReleasedW(wd));

        wd.retain();
        // wd: Reference count should be 2
        assertFalse(isReleasedW(wd));

        wd.release();
        // wd: Reference count should be 1
        assertFalse(isReleasedW(wd));

        final R rd = castR(wd.close(numValues));
        // wd: Should be released now because close passes the resources to rd
        assertTrue(isReleasedW(wd));

        // NB: Releasing wd again should have no effect on rd
        wd.release();
        // rd: Reference count should be 1
        assertFalse(isReleasedR(rd));

        rd.retain();
        // rd: Reference count should be 2
        assertFalse(isReleasedR(rd));

        rd.release();
        // rd: Reference count should be 1
        assertFalse(isReleasedR(rd));

        rd.release();
        // rd: should be released
        assertTrue(isReleasedR(rd));
    }

    /** Test {@link W#toString()} and {@link R#toString()} */
    @Test
    public void testToString() {
        final W wd = createWrite(1);
        final String ws = wd.toString();
        assertNotNull(ws);
        assertFalse(ws.isEmpty());

        final R rd = castR(wd.close(1));
        final String rs = rd.toString();
        assertNotNull(rs);
        assertFalse(rs.isEmpty());

        rd.release();
    }

    /** Test setting some values to missing */
    @Test
    public void testMissing() {
        int numValues = 64;
        final W writeData = createWrite(numValues);
        for (int i = 0; i < numValues; i++) {
            if (i % 13 == 0) {
                writeData.setMissing(i);
            } else {
                setValue(writeData, i, i);
            }
        }
        final R readData = castR(writeData.close(numValues));
        for (int i = 0; i < numValues; i++) {
            if (i % 13 == 0) {
                assertTrue(readData.isMissing(i));
            } else {
                assertFalse(readData.isMissing(i));
                checkValue(readData, i, i);
            }
        }
        readData.release();
    }

    /** Test setting all values to missing */
    @Test
    public void testAllMissing() {
        int numValues = 17;
        final W writeData = createWrite(numValues);
        for (int i = 0; i < numValues; i++) {
            writeData.setMissing(i);
        }
        final R readData = castR(writeData.close(numValues));
        for (int i = 0; i < numValues; i++) {
            assertTrue(readData.isMissing(i));
        }
        readData.release();
    }

    /** Test {@link W#sizeOf()} and {@link R#sizeOf()} */
    @Test
    public void testSizeOf() {
        final int numValues = 8213;
        final W wd = createWrite(numValues);
        long minSize = getMinSize(0, numValues);
        assertTrue("Size to small. Got " + wd.sizeOf() + ", expected >= " + minSize, wd.sizeOf() >= minSize);
        for (int i = 0; i < numValues; i++) {
            setValue(wd, i, i);
            minSize = getMinSize(i, numValues);
            assertTrue("Size to small. Got " + wd.sizeOf() + ", expected >= " + minSize, wd.sizeOf() >= minSize);
        }
        final R rd = castR(wd.close(numValues));
        minSize = getMinSize(numValues, numValues);
        assertTrue("Size to small. Got " + rd.sizeOf() + ", expected >= " + minSize, rd.sizeOf() >= minSize);
        rd.release();
    }

    /** Test {@link R#length()} */
    @Test
    public void testLength() {
        // Test with length = capacity
        W writeData = createWrite(13);
        R readData = castR(writeData.close(13));
        assertEquals(13, readData.length());
        readData.release();

        // Test with length != capacity
        writeData = createWrite(64);
        readData = castR(writeData.close(7));
        assertEquals(7, readData.length());
        readData.release();
    }

    /**
     * Test writing and reading some data
     *
     * @throws IOException if writing or reading fails
     */
    @Test
    public void testWriteRead() throws IOException {
        // Fill the data with values
        int numValues = 64;
        final W dw = createWrite(numValues);
        for (int i = 0; i < numValues; i++) {
            setValue(dw, i, i);
        }
        R d = castR(dw.close(numValues));
        ReadBatch batch = new DefaultReadBatch(new NullableReadData[]{d});

        // Write
        final File tmp = ArrowTestUtils.createTmpKNIMEArrowPath().toFile();
        final ArrowColumnDataFactory[] factories = new ArrowColumnDataFactory[]{m_factory};
        try (final ArrowBatchWriter writer =
            new ArrowBatchWriter(tmp, factories, COMPRESSION_CONFIG, m_alloc)) {
            writer.write(batch);
            batch.release();
        }

        // Read
        try (final ArrowBatchReader reader =
            new ArrowBatchReader(tmp, m_alloc, factories, new DefaultColumnSelection(1))) {

            batch = reader.readRetained(0);
            assertEquals(numValues, batch.length());
            assertEquals(numValues, batch.get(0).length());
            d = castR(batch.get(0));

            for (int i = 0; i < numValues; i++) {
                checkValue(d, i, i);
            }
            batch.release();
        }
        Files.delete(tmp.toPath());
    }

    /**
     * Test writing and reading some data with missing values
     *
     * @throws IOException if writing or reading fails
     */
    @Test
    public void testWriteReadMissing() throws IOException {
        // Fill the data with values
        int numValues = 64;
        final W dw = createWrite(numValues);
        for (int i = 0; i < numValues; i++) {
            if (i % 13 == 0) {
                dw.setMissing(i);
            } else {
                setValue(dw, i, i);
            }
        }
        R d = castR(dw.close(numValues));
        ReadBatch batch = new DefaultReadBatch(new NullableReadData[]{d});

        // Write
        final File tmp = ArrowTestUtils.createTmpKNIMEArrowPath().toFile();
        final ArrowColumnDataFactory[] factories = new ArrowColumnDataFactory[]{m_factory};
        try (final ArrowBatchWriter writer =
            new ArrowBatchWriter(tmp, factories, COMPRESSION_CONFIG, m_alloc)) {
            writer.write(batch);
            batch.release();
        }

        // Read
        try (final ArrowBatchReader reader =
            new ArrowBatchReader(tmp, m_alloc, factories, new DefaultColumnSelection(1))) {

            batch = reader.readRetained(0);
            assertEquals(numValues, batch.length());
            assertEquals(numValues, batch.get(0).length());
            d = castR(batch.get(0));

            for (int i = 0; i < numValues; i++) {
                if (i % 13 == 0) {
                    assertTrue(d.isMissing(i));
                } else {
                    assertFalse(d.isMissing(i));
                    checkValue(d, i, i);
                }
            }
            batch.release();
        }
        Files.delete(tmp.toPath());
    }

    /**
     * Test writing and reading some data with only missing values
     *
     * @throws IOException if writing or reading fails
     */
    @Test
    public void testWriteReadAllMissing() throws IOException {
        // Fill the data with values
        int numValues = 17;
        final W dw = createWrite(numValues);
        for (int i = 0; i < numValues; i++) {
            dw.setMissing(i);
        }
        R d = castR(dw.close(numValues));
        ReadBatch batch = new DefaultReadBatch(new NullableReadData[]{d});

        // Write
        final File tmp = ArrowTestUtils.createTmpKNIMEArrowPath().toFile();
        final ArrowColumnDataFactory[] factories = new ArrowColumnDataFactory[]{m_factory};
        try (final ArrowBatchWriter writer =
            new ArrowBatchWriter(tmp, factories, COMPRESSION_CONFIG, m_alloc)) {
            writer.write(batch);
            batch.release();
        }

        // Read
        try (final ArrowBatchReader reader =
            new ArrowBatchReader(tmp, m_alloc, factories, new DefaultColumnSelection(1))) {

            batch = reader.readRetained(0);
            assertEquals(numValues, batch.length());
            assertEquals(numValues, batch.get(0).length());
            d = castR(batch.get(0));

            for (int i = 0; i < numValues; i++) {
                assertTrue(d.isMissing(i));
            }
            batch.release();
        }
        Files.delete(tmp.toPath());
    }
}
