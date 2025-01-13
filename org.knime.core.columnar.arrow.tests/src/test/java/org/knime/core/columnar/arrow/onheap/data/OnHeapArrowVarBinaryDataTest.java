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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.knime.core.columnar.arrow.onheap.AbstractOnHeapArrowDataTest;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowVarBinaryData.ArrowVarBinaryDataFactory;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowVarBinaryData.ArrowVarBinaryReadData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowVarBinaryData.ArrowVarBinaryWriteData;
import org.knime.core.table.io.ReadableDataInputStream;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 * Test {@link OnHeapArrowVarBinaryData}
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class OnHeapArrowVarBinaryDataTest
    extends AbstractOnHeapArrowDataTest<ArrowVarBinaryWriteData, ArrowVarBinaryReadData> {

    private static final int MAX_LENGTH = 100;

    /** Create the test for {@link OnHeapArrowVarBinaryData} */
    public OnHeapArrowVarBinaryDataTest() {
        super(ArrowVarBinaryDataFactory.INSTANCE);
    }

    @Override
    protected ArrowVarBinaryWriteData castW(final Object o) {
        assertTrue(o instanceof ArrowVarBinaryWriteData, "Object is not an instance of ArrowVarBinaryWriteData");
        return (ArrowVarBinaryWriteData)o;
    }

    @Override
    protected ArrowVarBinaryReadData castR(final Object o) {
        assertTrue(o instanceof ArrowVarBinaryReadData, "Object is not an instance of ArrowVarBinaryReadData");
        return (ArrowVarBinaryReadData)o;
    }

    @Override
    protected void setValue(final ArrowVarBinaryWriteData data, final int index, final int seed) {
        data.setBytes(index, valueFor(seed));
    }

    @Override
    protected void checkValue(final ArrowVarBinaryReadData data, final int index, final int seed) {
        assertArrayEquals(valueFor(seed), data.getBytes(index),
            "Byte array at index " + index + " does not match expected value for seed " + seed);
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
     * <li>by serializing from/to a byte buffer and writing those bytes to the ArrowVarBinaryWriteData.</li>
     * </ol>
     *
     * @throws IOException if an I/O error occurs
     */
    @Test
    public void testByteArraySerialization() throws IOException {
        final int numValues = 3;
        ArrowVarBinaryWriteData data = createWrite(numValues);
        byte[][] blobs = new byte[][]{"TestData".getBytes(), "Foo".getBytes(), "Bar".getBytes()};

        ObjectSerializer<byte[]> serializer = (out, v) -> out.write(v);
        ObjectDeserializer<byte[]> deserializer = (in) -> in.readBytes();

        data.setBytes(0, blobs[0]);
        data.setObject(1, blobs[1], serializer);
        var outStream = new ByteArrayOutputStream();
        serializer.serialize(new DataOutputStream(outStream), blobs[2]);
        data.setBytes(2, outStream.toByteArray());

        ArrowVarBinaryReadData readData = data.close(numValues);

        for (int i = 0; i < numValues; i++) {
            assertArrayEquals(blobs[i], readData.getBytes(i),
                "Byte array at index " + i + " does not match expected blob");
            assertArrayEquals(blobs[i], readData.getObject(i, deserializer),
                "Deserialized object at index " + i + " does not match expected blob");
            byte[] deserializedFromBytes =
                deserializer.deserialize(new ReadableDataInputStream(new ByteArrayInputStream(readData.getBytes(i))));
            assertArrayEquals(blobs[i], deserializedFromBytes,
                "Deserialized from bytes at index " + i + " does not match expected blob");
        }
    }
}
