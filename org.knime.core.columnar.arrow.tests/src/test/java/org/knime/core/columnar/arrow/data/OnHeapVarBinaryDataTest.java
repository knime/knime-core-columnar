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
package org.knime.core.columnar.arrow.data;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.knime.core.columnar.arrow.AbstractArrowDataTest;
import org.knime.core.columnar.arrow.data.OnHeapVarBinaryData.OnHeapVarBinaryReadData;
import org.knime.core.columnar.arrow.data.OnHeapVarBinaryData.OnHeapVarBinaryWriteData;
import org.knime.core.table.io.ReadableDataInputStream;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 * Test {@link OnHeapVarBinaryData}
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class OnHeapVarBinaryDataTest extends AbstractArrowDataTest<OnHeapVarBinaryWriteData, OnHeapVarBinaryReadData> {

    private static final int MAX_LENGTH = 100;

    /** Create the test for {@link OnHeapVarBinaryData} */
    public OnHeapVarBinaryDataTest() {
        super(OnHeapVarBinaryData.FACTORY);
    }

    @Override
    protected OnHeapVarBinaryWriteData castW(final Object o) {
        assertTrue(o instanceof OnHeapVarBinaryWriteData, "Object is not an instance of OnHeapVarBinaryWriteData");
        return (OnHeapVarBinaryWriteData)o;
    }

    @Override
    protected OnHeapVarBinaryReadData castR(final Object o) {
        assertTrue(o instanceof OnHeapVarBinaryReadData, "Object is not an instance of OnHeapVarBinaryReadData");
        return (OnHeapVarBinaryReadData)o;
    }

    @Override
    protected void setValue(final OnHeapVarBinaryWriteData data, final int index, final int seed) {
        data.setBytes(index, valueFor(seed));
    }

    @Override
    protected void checkValue(final OnHeapVarBinaryReadData data, final int index, final int seed) {
        assertArrayEquals(valueFor(seed), data.getBytes(index),
            "Byte array at index " + index + " does not match expected value for seed " + seed);
    }

    @Override
    protected boolean isReleasedW(final OnHeapVarBinaryWriteData data) {
        // On-heap data does not have resources that need explicit release
        return false;
    }

    @Override
    protected boolean isReleasedR(final OnHeapVarBinaryReadData data) {
        // On-heap data does not have resources that need explicit release
        return false;
    }

    @Override
    protected long getMinSize(final int valueCount, final int capacity) {
        long numBytes = 0;
        for (int i = 0; i < valueCount; i++) {
            numBytes += new Random(i).nextInt(MAX_LENGTH);
        }
        return numBytes // data buffer
            + 4L * (capacity + 1) // offset buffer (one extra for the last offset)
            + (long)Math.ceil(capacity / 8.0); // validity buffer
    }

    private static byte[] valueFor(final int seed) {
        final Random random = new Random(seed);
        final byte[] bytes = new byte[random.nextInt(MAX_LENGTH)];
        random.nextBytes(bytes);
        return bytes;
    }

    /**
     * Test three different ways to serialize and deserialize bytes:
     * <ol>
     * <li>using the setBytes()/getBytes() methods,</li>
     * <li>using setObject/getObject with simple serializers,</li>
     * <li>by serializing from/to a byte buffer and writing those bytes to the OnHeapVarBinaryWriteData.</li>
     * </ol>
     *
     * @throws IOException if an I/O error occurs
     */
    @Test
    public void testByteArraySerialization() throws IOException {
        final int numValues = 3;
        OnHeapVarBinaryWriteData data = createWrite(numValues);
        byte[] blob = "TestData".getBytes();

        ObjectSerializer<byte[]> serializer = (out, v) -> out.write(v);
        ObjectDeserializer<byte[]> deserializer = (in) -> in.readBytes();

        data.setBytes(0, blob);
        data.setObject(1, blob, serializer);
        var outStream = new ByteArrayOutputStream();
        serializer.serialize(new DataOutputStream(outStream), blob);
        data.setBytes(2, outStream.toByteArray());

        OnHeapVarBinaryReadData readData = data.close(numValues);

        for (int i = 0; i < numValues; i++) {
            assertArrayEquals(blob, readData.getBytes(i), "Byte array at index " + i + " does not match expected blob");
            assertArrayEquals(blob, readData.getObject(i, deserializer),
                "Deserialized object at index " + i + " does not match expected blob");
            byte[] deserializedFromBytes = deserializer
                    .deserialize(new ReadableDataInputStream(new ByteArrayInputStream(readData.getBytes(i))));
            assertArrayEquals(blob, deserializedFromBytes,
                "Deserialized from bytes at index " + i + " does not match expected blob");
        }
    }
}
