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
package org.knime.core.columnar.arrow.onheap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.knime.core.columnar.arrow.ReferenceAssertions.assertHasArrayReference;
import static org.knime.core.columnar.arrow.ReferenceAssertions.assertNoNonNullReferences;

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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.knime.core.columnar.arrow.ArrowTestUtils;
import org.knime.core.columnar.arrow.compress.ArrowCompression;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.columnar.arrow.mmap.MappedMessageSerializerTestUtil;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowReadData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowWriteData;
import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.filter.DefaultColumnSelection;

/**
 * Abstract test for simple Arrow {@link NullableReadData}, {@link NullableWriteData} implementations.
 *
 * @param <W> type of the {@link NullableWriteData} implementation
 * @param <R> type of the {@link NullableReadData} implementation
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractOnHeapArrowDataTest<W extends OnHeapArrowWriteData, R extends OnHeapArrowReadData> {

    private static final ArrowCompression COMPRESSION_CONFIG = ArrowCompressionUtil.ARROW_NO_COMPRESSION;

    /** The factory of the data implementation */
    protected final OnHeapArrowColumnDataFactory m_factory;

    /** The allocator (initialized in {@link #before()} and closed in {@link #after()}) */
    protected RootAllocator m_alloc;

    /**
     * Create a test for Arrow data implementations.
     *
     * @param factory the factory which should be used to create the data objects
     */
    protected AbstractOnHeapArrowDataTest(final OnHeapArrowColumnDataFactory factory) {
        m_factory = factory;
    }

    /**
     * Check if the object is of class W and cast.
     *
     * @param o an object
     * @return the cast object
     */
    protected abstract W castW(Object o);

    /**
     * Check if the object is of class R and cast.
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
     * Check the value at the given index using the seed.
     *
     * @param data the data object
     * @param index the index
     * @param seed the seed
     */
    protected abstract void checkValue(R data, int index, int seed);

    /**
     * @param valueCount the count of values added to the data (with seeds 0..(valueCount-1))
     * @param capacity the total capacity of the data
     * @return the minimum size that should be allocated by data object to store the data
     */
    protected abstract long getMinSize(int valueCount, int capacity);

    /**
     * Overwrite, to provide a different minimum size (from {@link #getMinSize(int, int)}) for the write data object.
     *
     * @param valueCount the count of values added to the data (with seeds 0..(valueCount-1))
     * @param capacity the total capacity of the data
     * @return the minimum size that should be allocated by the write data object to store the data
     */
    protected long getMinSizeW(final int valueCount, final int capacity) {
        return getMinSize(valueCount, capacity);
    }

    /** Initialize the root allocator before running a test */
    @BeforeEach
    public void before() {
        final int segmentSize = 1;
        m_alloc = new RootAllocator(AllocationListener.NOOP, Long.MAX_VALUE,
            requestSize -> (requestSize + (segmentSize - 1)) / segmentSize * segmentSize);
    }

    /** Close the root allocator after running a test */
    @AfterEach
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
        return castW(m_factory.createWrite(numValues));
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
        assertTrue(data.capacity() >= 12, "Expected capacity to be at least 12, but was " + data.capacity());
        data.release();

        data = createWrite(16000);
        assertTrue(data.capacity() >= 16000, "Expected capacity to be at least 16000, but was " + data.capacity());
        data.release();

        data = createWrite(68);
        assertTrue(data.capacity() >= 68, "Expected capacity to be at least 68, but was " + data.capacity());
        data.release();
    }

    /** Test {@link W#expand(int)} */
    @Test
    public void testExpand() {
        int size1 = 12;
        int size2 = 33;

        // Allocate
        W data = createWrite(size1);
        assertTrue(data.capacity() >= size1,
            "Initial capacity should be at least " + size1 + ", but was " + data.capacity());

        // Write some data
        for (int i = 0; i < size1; i++) {
            setValue(data, i, i);
        }

        // Expand
        data.expand(size2);
        assertTrue(data.capacity() >= size2,
            "Capacity after expand should be at least " + size2 + ", but was " + data.capacity());

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
                assertFalse(readData.isMissing(i), "Value at index " + i + " should not be missing");
                checkValue(readData, i, i - sliceStart);
            } else {
                // Outside the written slice
                assertTrue(readData.isMissing(i), "Value at index " + i + " should be missing");
            }
        }

        // Read only the slice
        final R slicedRead = castR(readData.slice(sliceStart, sliceLength));
        assertEquals(sliceLength, slicedRead.length(),
            "Sliced read length should be " + sliceLength + ", but was " + slicedRead.length());
        for (int i = 0; i < sliceLength; i++) {
            assertFalse(slicedRead.isMissing(i), "Value at index " + i + " in sliced read should not be missing");
            checkValue(slicedRead, i, i);
        }

        readData.release();
    }

    /** Test reading and writing missing values in sliced data */
    @Test
    public void testSlicedSetMissing() {
        final int numValues = 32;
        final int sliceStart = 20;
        final int sliceLength = 12;

        final W writeData = createWrite(numValues);
        // Write into a slice
        final W slicedWrite = castW(writeData.slice(sliceStart));
        for (int i = 0; i < sliceLength; i++) {
            if (i % 2 == 0) {
                setValue(slicedWrite, i, i);
            } else {
                slicedWrite.setMissing(i);
            }
        }

        // Read the slice
        final R readData = castR(writeData.close(numValues));
        final R slicedRead = castR(readData.slice(sliceStart, sliceLength));
        for (int i = 0; i < sliceLength; i++) {
            if (i % 2 == 0) {
                assertFalse(slicedRead.isMissing(i), "Value at index " + i + " should not be missing");
            } else {
                assertTrue(slicedRead.isMissing(i), "Value at index " + i + " should be missing");
            }
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
                assertFalse(readData.isMissing(i), "Value at index " + i + " should not be missing");
                checkValue(readData, i, i - slice1Start - slice2Start);
            } else {
                // Outside the written slice
                assertTrue(readData.isMissing(i), "Value at index " + i + " should be missing");
            }
        }

        // Read only the slice
        final R slicedRead1 = castR(readData.slice(slice1Start, 10));
        final R slicedRead2 = castR(slicedRead1.slice(slice2Start, sliceLength));
        assertEquals(sliceLength, slicedRead2.length(),
            "Sliced read length should be " + sliceLength + ", but was " + slicedRead2.length());
        for (int i = 0; i < sliceLength; i++) {
            assertFalse(slicedRead2.isMissing(i), "Value at index " + i + " in sliced read should not be missing");
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
        assertHasArrayReference(wd, "Write data should not be released after setting values");

        wd.retain();
        // wd: Reference count should be 2
        assertHasArrayReference(wd, "Write data should not be released after retain");

        wd.release();
        // wd: Reference count should be 1
        assertHasArrayReference(wd, "Write data should not be released after release");

        final R rd = castR(wd.close(numValues));
        // wd: Should be released now because close passes the resources to rd
        assertNoNonNullReferences(wd, "Write data should be released after close");

        // NB: Releasing wd again should have no effect on rd
        wd.release();
        // rd: Reference count should be 1
        assertHasArrayReference(rd, "Read data should not be released after closing write data");

        rd.retain();
        // rd: Reference count should be 2
        assertHasArrayReference(rd, "Read data should not be released after retain");

        rd.release();
        // rd: Reference count should be 1
        assertHasArrayReference(rd, "Read data should not be released after release");

        rd.release();
        // rd: should be released
        assertNoNonNullReferences(rd, "Read data should be released after final release");
    }

    /** Test {@link W#toString()} and {@link R#toString()} */
    @Test
    public void testToString() {
        final W wd = createWrite(1);
        final String ws = wd.toString();
        assertNotNull(ws, "Write data toString() should not return null");
        assertFalse(ws.isEmpty(), "Write data toString() should not return empty string");

        final R rd = castR(wd.close(1));
        final String rs = rd.toString();
        assertNotNull(rs, "Read data toString() should not return null");
        assertFalse(rs.isEmpty(), "Read data toString() should not return empty string");

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
                assertTrue(readData.isMissing(i), "Value at index " + i + " should be missing");
            } else {
                assertFalse(readData.isMissing(i), "Value at index " + i + " should not be missing");
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
            assertTrue(readData.isMissing(i), "Value at index " + i + " should be missing");
        }
        readData.release();
    }

    /** Test {@link W#sizeOf()} and {@link R#sizeOf()} */
    @Test
    public void testSizeOf() {
        final int numValues = 8213;
        final W wd = createWrite(numValues);
        long minSize = getMinSizeW(0, numValues);
        assertTrue(wd.sizeOf() >= minSize, "Size too small. Got " + wd.sizeOf() + ", expected >= " + minSize);
        for (int i = 0; i < numValues; i++) {
            setValue(wd, i, i);
            minSize = getMinSizeW(i, numValues);
            assertTrue(wd.sizeOf() >= minSize,
                "Size too small after adding " + i + " elements. Got " + wd.sizeOf() + ", expected >= " + minSize);
        }
        final R rd = castR(wd.close(numValues));
        minSize = getMinSize(numValues, numValues);
        assertTrue(rd.sizeOf() >= minSize, "Size too small. Got " + rd.sizeOf() + ", expected >= " + minSize);
        rd.release();
    }

    /** Test {@link R#length()} */
    @Test
    public void testLength() {
        // Test with length = capacity
        W writeData = createWrite(13);
        R readData = castR(writeData.close(13));
        assertEquals(13, readData.length(), "Read data length should be 13");
        readData.release();

        // Test with length != capacity
        writeData = createWrite(64);
        readData = castR(writeData.close(7));
        assertEquals(7, readData.length(), "Read data length should be 7");
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
        final var tmp = ArrowTestUtils.createTmpKNIMEArrowFileSupplier();
        final var factories = new OnHeapArrowColumnDataFactory[]{m_factory};
        try (final OnHeapArrowBatchWriter writer =
            new OnHeapArrowBatchWriter(tmp, factories, COMPRESSION_CONFIG, m_alloc)) {
            writer.write(batch);
            batch.release();
        }

        // Read
        try (final OnHeapArrowBatchReader reader =
            new OnHeapArrowBatchReader(tmp.asFile(), m_alloc, factories, new DefaultColumnSelection(1))) {

            batch = reader.readRetained(0);
            assertEquals(numValues, batch.length(), "Batch length should be " + numValues);
            assertEquals(numValues, batch.get(0).length(), "First data length should be " + numValues);
            d = castR(batch.get(0));

            for (int i = 0; i < numValues; i++) {
                checkValue(d, i, i);
            }
            batch.release();
        }
        Files.delete(tmp.asPath());
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
        var tmp = ArrowTestUtils.createTmpKNIMEArrowFileSupplier();
        var factories = new OnHeapArrowColumnDataFactory[]{m_factory};
        try (final OnHeapArrowBatchWriter writer =
            new OnHeapArrowBatchWriter(tmp, factories, COMPRESSION_CONFIG, m_alloc)) {
            writer.write(batch);
            batch.release();
        }

        // Read
        try (final OnHeapArrowBatchReader reader =
            new OnHeapArrowBatchReader(tmp.asFile(), m_alloc, factories, new DefaultColumnSelection(1))) {

            batch = reader.readRetained(0);
            assertEquals(numValues, batch.length(), "Batch length should be " + numValues);
            assertEquals(numValues, batch.get(0).length(), "First data length should be " + numValues);
            d = castR(batch.get(0));

            for (int i = 0; i < numValues; i++) {
                if (i % 13 == 0) {
                    assertTrue(d.isMissing(i), "Value at index " + i + " should be missing");
                } else {
                    assertFalse(d.isMissing(i), "Value at index " + i + " should not be missing");
                    checkValue(d, i, i);
                }
            }
            batch.release();
        }
        Files.delete(tmp.asPath());
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
        var tmp = ArrowTestUtils.createTmpKNIMEArrowFileSupplier();
        var factories = new OnHeapArrowColumnDataFactory[]{m_factory};
        try (final OnHeapArrowBatchWriter writer =
            new OnHeapArrowBatchWriter(tmp, factories, COMPRESSION_CONFIG, m_alloc)) {
            writer.write(batch);
            batch.release();
        }

        // Read
        try (final OnHeapArrowBatchReader reader =
            new OnHeapArrowBatchReader(tmp.asFile(), m_alloc, factories, new DefaultColumnSelection(1))) {

            batch = reader.readRetained(0);
            assertEquals(numValues, batch.length(), "Batch length should be " + numValues);
            assertEquals(numValues, batch.get(0).length(), "First data length should be " + numValues);
            d = castR(batch.get(0));

            for (int i = 0; i < numValues; i++) {
                assertTrue(d.isMissing(i), "Value at index " + i + " should be missing");
            }
            batch.release();
        }
        Files.delete(tmp.asPath());
    }

    /**
     * Test that {@link OnHeapArrowColumnDataFactory#initialNumBytesPerElement()} returns a reasonable estimate for the
     * allocated size.
     */
    @Test
    public void testInitialNumBytesPerElement() {
        var initialNumBytes = m_factory.initialNumBytesPerElement();
        assertTrue(initialNumBytes > 0, "Initial num bytes per element should be greater than zero");

        checkInitialNumBytesPerElementFor(4);
        checkInitialNumBytesPerElementFor(200);
    }

    private void checkInitialNumBytesPerElementFor(final int num_values) {
        var data = createWrite(num_values);
        try {
            var size = data.sizeOf();
            var expectedSize = m_factory.initialNumBytesPerElement() * num_values;

            // Make sure we are not under-estimating the size by much
            // (for small data types and few elements we might under-estimate but still end up with 16 bytes)
            var upperLimit = Math.max((int)(expectedSize * 1.2), 16);
            assertTrue(size <= upperLimit,
                "Initial size for %d elements was %d bytes, expected: %d bytes (max %d bytes)".formatted(num_values,
                    size, expectedSize, upperLimit));

            // Make sure we are not over-estimating the size by much
            // (exclude boolean because it only uses 1 bit per element but we estimate 1 byte per element)
            if (expectedSize > num_values) {
                var lowerLimit = (int)(expectedSize * 0.5);
                assertTrue(size >= lowerLimit,
                    "Initial size for %d elements was %d bytes, expected: %d bytes (at least %d bytes)"
                        .formatted(num_values, size, expectedSize, lowerLimit));
            }
        } finally {
            data.close(num_values).release();
        }
    }
}
