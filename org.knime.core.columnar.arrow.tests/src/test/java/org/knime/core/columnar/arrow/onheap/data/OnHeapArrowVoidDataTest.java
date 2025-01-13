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
 *   Sep 30, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow.onheap.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.knime.core.columnar.arrow.onheap.AbstractOnHeapArrowDataTest;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowVoidData.ArrowVoidDataFactory;

/**
 * Test {@link OnHeapArrowVoidData}
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class OnHeapArrowVoidDataTest extends AbstractOnHeapArrowDataTest<OnHeapArrowVoidData, OnHeapArrowVoidData> {

    /** Create the test for {@link OnHeapArrowVoidData} */
    public OnHeapArrowVoidDataTest() {
        super(ArrowVoidDataFactory.INSTANCE);
    }

    private static OnHeapArrowVoidData cast(final Object o) {
        assertTrue(o instanceof OnHeapArrowVoidData);
        return (OnHeapArrowVoidData)o;
    }

    @Override
    protected OnHeapArrowVoidData castW(final Object o) {
        return cast(o);
    }

    @Override
    protected OnHeapArrowVoidData castR(final Object o) {
        return cast(o);
    }

    @Override
    protected void setValue(final OnHeapArrowVoidData data, final int index, final int seed) {
        // No value to set
    }

    @Override
    protected void checkValue(final OnHeapArrowVoidData data, final int index, final int seed) {
        // No value to check
    }

    @Override
    protected long getMinSize(final int valueCount, final int capacity) {
        return 0;
    }

    @Override
    public void testSlice() {
        final int numValues = 32;
        final int sliceStart = 5;
        final int sliceLength = 10;

        final OnHeapArrowVoidData writeData = createWrite(numValues);
        final OnHeapArrowVoidData readData = castR(writeData.close(numValues));
        final OnHeapArrowVoidData slicedData = readData.slice(sliceStart, sliceLength);
        assertEquals(sliceLength, slicedData.length());

        writeData.release();
        readData.release();
    }

    @Override
    public void testSliceOfSlice() { // NOSONAR
    }

    @Override
    public void testReferenceCounting() { // NOSONAR
        // The void vector is holding no data so we don't care about releasing it
    }

    @Override
    public void testMissing() { // NOSONAR
        // Void data does not support missing values
    }

    @Override
    public void testSlicedSetMissing() { // NOSONAR
        // Void data does not support missing values
    }

    @Override
    public void testWriteReadMissing() { // NOSONAR
        // Void data does not support missing values
    }

    @Override
    public void testInitialNumBytesPerElement() { // NOSONAR
        // Void data is special - it does not allocate any memory
        var initialNumBytes = m_factory.initialNumBytesPerElement();
        assertEquals("Initial num bytes per element should be greater than zero", 0, initialNumBytes);
    }
}
