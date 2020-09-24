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

import java.io.File;
import java.io.IOException;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.filter.DefaultColumnSelection;

/**
 * Abstract test for simple Arrow {@link ColumnReadData}, {@link ColumnWriteData} implementations.
 *
 * @param <C> type of the data implementation
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractArrowDataTest<C extends ColumnWriteData & ColumnReadData> {

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
     * Check if the object is of class C an cast.
     *
     * @param o an object
     * @return the cast object
     */
    protected abstract C cast(Object o);

    /**
     * Set the value at the given index using the seed.
     *
     * @param data the data object
     * @param index the index
     * @param seed the seed
     */
    protected abstract void setValue(C data, int index, int seed);

    /**
     * Check the value at the given index using the seed. Should be the same as set in
     * {@link #setValue(ColumnWriteData, int, int)}.
     *
     * @param data the data object
     * @param index the index
     * @param seed the seed
     */
    protected abstract void checkValue(C data, int index, int seed);

    /**
     * @param data the data object
     * @return if the data object has been released
     */
    protected abstract boolean isReleased(C data);

    /**
     * @param valueCount the count of values added to the data (with seeds 0..(valueCount-1))
     * @param capacity the total capacity of the data
     * @return the minimum size that should be allocated by data object to store the data
     */
    protected abstract int getMinSize(int valueCount, int capacity);

    /** Initialize the root allocator before running a test */
    @Before
    public void before() {
        m_alloc = new RootAllocator(AllocationListener.NOOP, Long.MAX_VALUE, requestSize -> (requestSize + 1) / 2 * 2);
        //        m_alloc = new RootAllocator();
    }

    /** Close the root allocator after running a test */
    @After
    public void after() {
        m_alloc.close();
    }

    /** Test allocating some memory and setting and getting some values */
    @Test
    public void testAlloc() {
        int numValues = 64;
        final C writeData = cast(m_factory.createWrite(m_alloc, numValues));
        for (int i = 0; i < numValues; i++) {
            setValue(writeData, i, i);
        }
        final C readData = cast(writeData.close(numValues));
        // assertEquals(numValues, readData.length());
        for (int i = 0; i < numValues; i++) {
            checkValue(readData, i, i);
        }
        readData.release();
    }

    /** Test {@link C#capacity()} */
    @Test
    public void testCapacity() {
        C data = cast(m_factory.createWrite(m_alloc, 12));
        assertTrue(data.capacity() >= 12);
        data.release();

        data = cast(m_factory.createWrite(m_alloc, 16000));
        assertTrue(data.capacity() >= 16000);
        data.release();

        data = cast(m_factory.createWrite(m_alloc, 68));
        assertTrue(data.capacity() >= 68);
        data.release();
    }

    /** Test {@link C#retain()} and {@link C#release()} */
    @Test
    public void testReferenceCounting() {
        int numValues = 8;
        final C wd = cast(m_factory.createWrite(m_alloc, numValues));
        for (int i = 0; i < numValues; i++) {
            setValue(wd, i, i);
        }

        assertFalse(isReleased(wd));

        // Increase reference counter
        wd.retain();
        assertFalse(isReleased(wd));

        // On reference should still be there
        wd.release();
        assertFalse(isReleased(wd));

        // All references should be gone
        wd.release();
        assertTrue(isReleased(wd));
    }

    /** Test {@link C#toString()} */
    @Test
    public void testToString() {
        final C d = cast(m_factory.createWrite(m_alloc, 1));
        final String s = d.toString();
        assertNotNull(s);
        assertFalse(s.isEmpty());
        d.release();
    }

    /** Test setting some values to missing */
    @Test
    public void testMissing() {
        int numValues = 64;
        final C writeData = cast(m_factory.createWrite(m_alloc, numValues));
        for (int i = 0; i < numValues; i++) {
            if (i % 13 == 0) {
                writeData.setMissing(i);
            } else {
                setValue(writeData, i, i);
            }
        }
        final C readData = cast(writeData.close(numValues));
        // assertEquals(numValues, readData.length());
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

    /** Test {@link C#sizeOf()} */
    @Test
    public void testSizeOf() {
        final int numValues = 8213;
        final C d = cast(m_factory.createWrite(m_alloc, numValues));
        int minSize = getMinSize(0, numValues);
        assertTrue("Size to small. Got " + d.sizeOf() + ", expected >= " + minSize, d.sizeOf() >= minSize);
        for (int i = 0; i < numValues; i++) {
            setValue(d, i, i);
            minSize = getMinSize(i, numValues);
            assertTrue("Size to small. Got " + d.sizeOf() + ", expected >= " + minSize, d.sizeOf() >= minSize);
        }
        d.close(numValues);
        minSize = getMinSize(numValues, numValues);
        assertTrue("Size to small. Got " + d.sizeOf() + ", expected >= " + minSize, d.sizeOf() >= minSize);
        d.release();
    }

    /** Test {@link C#length()} */
    @Test
    public void testLength() {
        // Test with length = capacity
        C writeData = cast(m_factory.createWrite(m_alloc, 13));
        C readData = cast(writeData.close(13));
        assertEquals(13, readData.length());
        readData.release();

        // Test with length != capacity
        writeData = cast(m_factory.createWrite(m_alloc, 64));
        readData = cast(writeData.close(7));
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
        C d = cast(m_factory.createWrite(m_alloc, numValues));
        for (int i = 0; i < numValues; i++) {
            setValue(d, i, i);
        }
        d = cast(d.close(numValues));
        ReadBatch batch = new DefaultReadBatch(new ColumnReadData[]{d}, numValues);

        // Write
        final File tmp = ArrowTestUtils.createTmpKNIMEArrowFile();
        final ArrowColumnDataFactory[] factories = new ArrowColumnDataFactory[]{m_factory};
        try (final ArrowColumnDataWriter writer = new ArrowColumnDataWriter(tmp, numValues, factories)) {
            writer.write(batch);
            batch.release();
        }

        // Read
        try (final ArrowColumnDataReader reader =
            new ArrowColumnDataReader(tmp, m_alloc, factories, new DefaultColumnSelection(1))) {

            batch = reader.readRetained(0);
            assertEquals(numValues, batch.length());
            assertEquals(numValues, batch.get(0).length());
            assertEquals(numValues, batch.get(0).length());
            d = cast(batch.get(0));

            for (int i = 0; i < numValues; i++) {
                checkValue(d, i, i);
            }
            batch.release();
        }
        tmp.delete();
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
        C d = cast(m_factory.createWrite(m_alloc, numValues));
        for (int i = 0; i < numValues; i++) {
            if (i % 13 == 0) {
                d.setMissing(i);
            } else {
                setValue(d, i, i);
            }
        }
        d = cast(d.close(numValues));
        ReadBatch batch = new DefaultReadBatch(new ColumnReadData[]{d}, numValues);

        // Write
        final File tmp = ArrowTestUtils.createTmpKNIMEArrowFile();
        final ArrowColumnDataFactory[] factories = new ArrowColumnDataFactory[]{m_factory};
        try (final ArrowColumnDataWriter writer = new ArrowColumnDataWriter(tmp, numValues, factories)) {
            writer.write(batch);
            batch.release();
        }

        // Read
        try (final ArrowColumnDataReader reader =
            new ArrowColumnDataReader(tmp, m_alloc, factories, new DefaultColumnSelection(1))) {

            batch = reader.readRetained(0);
            assertEquals(numValues, batch.length());
            assertEquals(numValues, batch.get(0).length());
            assertEquals(numValues, batch.get(0).length());
            d = cast(batch.get(0));

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
        tmp.delete();
    }
}
