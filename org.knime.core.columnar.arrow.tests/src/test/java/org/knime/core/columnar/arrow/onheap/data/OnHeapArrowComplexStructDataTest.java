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
 *   Oct 14, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow.onheap.data;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;

import org.knime.core.columnar.arrow.onheap.AbstractOnHeapArrowDataTest;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowDoubleData.ArrowDoubleDataFactory;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowDoubleData.ArrowDoubleReadData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowDoubleData.ArrowDoubleWriteData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowIntData.ArrowIntDataFactory;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowIntData.ArrowIntReadData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowIntData.ArrowIntWriteData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowSmartStringData.ArrowStringDataFactory;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowSmartStringData.ArrowStringReadData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowSmartStringData.ArrowStringWriteData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowStructData.ArrowStructDataFactory;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowStructData.ArrowStructReadData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowStructData.ArrowStructWriteData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowVarBinaryData.ArrowVarBinaryDataFactory;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowVarBinaryData.ArrowVarBinaryReadData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowVarBinaryData.ArrowVarBinaryWriteData;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;

/**
 * Test {@link OnHeapArrowStructData} with a struct consisting of a string, integer, and struct (of varbinary and
 * double).
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class OnHeapArrowComplexStructDataTest
    extends AbstractOnHeapArrowDataTest<ArrowStructWriteData, ArrowStructReadData> {

    private static final int MAX_LENGTH = 100;

    private static ArrowStructDataFactory createFactory() {
        var child1 = ArrowStringDataFactory.INSTANCE;
        var child2 = ArrowIntDataFactory.INSTANCE;
        var child3 = new ArrowStructDataFactory(ArrowVarBinaryDataFactory.INSTANCE, ArrowDoubleDataFactory.INSTANCE);
        return new ArrowStructDataFactory(child1, child2, child3);
    }

    /** Create the test for {@link OnHeapArrowStructData} */
    public OnHeapArrowComplexStructDataTest() {
        super(createFactory());
    }

    @Override
    protected ArrowStructWriteData castW(final Object o) {
        assertTrue(o instanceof ArrowStructWriteData, "Object is not an instance of ArrowStructWriteData");
        return (ArrowStructWriteData)o;
    }

    @Override
    protected ArrowStructReadData castR(final Object o) {
        assertTrue(o instanceof ArrowStructReadData, "Object is not an instance of ArrowStructReadData");
        return (ArrowStructReadData)o;
    }

    @Override
    protected void setValue(final ArrowStructWriteData data, final int index, final int seed) {
        final StringWriteData data0 = data.getWriteDataAt(0);
        assertTrue(data0 instanceof ArrowStringWriteData, "Object is not an instance of ArrowStringWriteData");
        final IntWriteData data1 = data.getWriteDataAt(1);
        assertTrue(data1 instanceof ArrowIntWriteData, "Object is not an instance of ArrowIntWriteData");
        final StructWriteData data2 = data.getWriteDataAt(2);
        assertTrue(data2 instanceof ArrowStructWriteData, "Object is not an instance of ArrowStructWriteData");
        final VarBinaryWriteData data20 = data2.getWriteDataAt(0);
        assertTrue(data20 instanceof ArrowVarBinaryWriteData, "Object is not an instance of ArrowVarBinaryWriteData");
        final DoubleWriteData data21 = data2.getWriteDataAt(1);
        assertTrue(data21 instanceof ArrowDoubleWriteData, "Object is not an instance of ArrowDoubleWriteData");

        data0.setString(index, Integer.toString(seed));
        data1.setInt(index, seed);
        data20.setBytes(index, valueFor20(seed));
        data21.setDouble(index, seed);
    }

    @Override
    protected void checkValue(final ArrowStructReadData data, final int index, final int seed) {
        final StringReadData data0 = data.getReadDataAt(0);
        assertTrue(data0 instanceof ArrowStringReadData, "Object is not an instance of ArrowStringReadData");
        final IntReadData data1 = data.getReadDataAt(1);
        assertTrue(data1 instanceof ArrowIntReadData, "Object is not an instance of ArrowIntReadData");
        final StructReadData data2 = data.getReadDataAt(2);
        assertTrue(data2 instanceof ArrowStructReadData, "Object is not an instance of ArrowStructReadData");
        final VarBinaryReadData data20 = data2.getReadDataAt(0);
        assertTrue(data20 instanceof ArrowVarBinaryReadData, "Object is not an instance of ArrowVarBinaryReadData");
        final DoubleReadData data21 = data2.getReadDataAt(1);
        assertTrue(data21 instanceof ArrowDoubleReadData, "Object is not an instance of ArrowDoubleReadData");

        assertEquals(Integer.toString(seed), data0.getString(index),
            "String value does not match expected value at index " + index);
        assertEquals(seed, data1.getInt(index), "Int value does not match expected value at index " + index);
        assertArrayEquals(valueFor20(seed), data20.getBytes(index),
            "Byte array value does not match expected value at index " + index);
        assertEquals(seed, data21.getDouble(index), 1e-5,
            "Double value does not match expected value at index " + index);
    }

    @Override
    protected long getMinSize(final int valueCount, final int capacity) {
        // Number of bytes for the object data
        int numBytesObjectData = 0;
        for (int i = 0; i < valueCount; i++) {
            numBytesObjectData += new Random(i).nextInt(MAX_LENGTH);
        }

        // NB: The dictionary is not allocated
        return 6 * (long)Math.ceil(capacity / 8.0) // Validity buffers
            + capacity * 4 // Dictionary encoded data
            + capacity * 4 // Integer data
            + (capacity + 1) * 4 + numBytesObjectData // Object data (offset array + data)
            + capacity * 4; // Dictionary encoded data
    }

    private static byte[] valueFor20(final int seed) {
        final Random random = new Random(seed);
        final byte[] bytes = new byte[random.nextInt(MAX_LENGTH)];
        random.nextBytes(bytes);
        return bytes;
    }
}
