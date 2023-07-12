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
 *   10 Nov 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.arrow.data;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.knime.core.columnar.arrow.data.ArrowBufIO.deserialize;
import static org.knime.core.columnar.arrow.data.ArrowBufIO.serialize;

import java.io.EOFException;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ArrowBufIOTest {

    private static void failOnUnthrownEOFException() {
        fail("Expected EOFException not thrown.");
    }

    private RootAllocator m_alloc;

    @Before
    public void before() {
        final int segmentSize = 4;
        m_alloc = new RootAllocator(AllocationListener.NOOP, Long.MAX_VALUE,
            requestSize -> (requestSize + (segmentSize - 1)) / segmentSize * segmentSize);
    }

    @After
    public void after() {
        m_alloc.close();
    }

    private void serdeNull(final ObjectSerializer<?> ser, final ObjectDeserializer<?> de) {
        try (final LargeVarBinaryVector vector = new LargeVarBinaryVector("test", m_alloc)) {
            vector.allocateNew(0, 0);
            serialize(0, null, vector, ser);
            deserialize(0, vector, de);
        }
    }

    private void serdeString(final String s, final String expect, final ObjectSerializer<String> ser,
        final ObjectDeserializer<String> de) {
        try (final LargeVarBinaryVector vector = new LargeVarBinaryVector("test", m_alloc)) {
            vector.allocateNew(0, 0);
            serialize(0, s, vector, ser);
            assertEquals(expect, deserialize(0, vector, de));
        }
    }

    private <T> void serdeT(final T obj, final ObjectSerializer<T> ser, final ObjectDeserializer<T> de) {
        try (final LargeVarBinaryVector vector = new LargeVarBinaryVector("test", m_alloc)) {
            vector.allocateNew(0, 0);
            serialize(0, obj, vector, ser);
            assertEquals(obj, deserialize(0, vector, de));
        }
    }

    private void serdeByteArray(final byte[] arr, final ObjectSerializer<byte[]> ser,
        final ObjectDeserializer<byte[]> de) {
        try (final LargeVarBinaryVector vector = new LargeVarBinaryVector("test", m_alloc)) {
            vector.allocateNew(0, 0);
            serialize(0, arr, vector, ser);
            assertArrayEquals(arr, deserialize(0, vector, de));
        }
    }

    // ##### Tests for write(byte[]) and readFully(byte[])

    @Test(expected = NullPointerException.class)
    public void testNPEOnWrite() {
        serdeNull((output, object) -> output.write(null), input -> new byte[0]);
    }

    @Test(expected = NullPointerException.class)
    public void testNPEOnReadFully() {
        serdeNull((output, object) -> {
        }, input -> {
            input.readFully(null);
            return new byte[0];
        });
    }

    @Test
    public void testEOFOnReadFully() {
        serdeNull((output, object) -> {
        }, input -> {
            try {
                input.readFully(new byte[1]);
                failOnUnthrownEOFException();
            } catch (EOFException e) { // NOSONAR
            }
            return new byte[0];
        });
    }

    @Test
    public void testWriteReadFully() {
        final ObjectSerializer<byte[]> ser = (output, object) -> {
            output.writeInt(object.length);
            output.write(object);
        };
        final ObjectDeserializer<byte[]> de = input -> {
            final byte[] result = new byte[input.readInt()];
            input.readFully(result);
            return result;
        };

        serdeByteArray(new byte[0], ser, de);
        serdeByteArray(new byte[]{Byte.MIN_VALUE, Byte.valueOf((byte)0), Byte.MAX_VALUE}, ser, de);
    }

    // ##### Tests for skipBytes(int)

    @Test
    public void testSkipBytes() {
        serdeNull((output, object) -> output.write(new byte[]{(byte)0, (byte)1, (byte)2}), input -> {
            assertEquals(1, input.skipBytes(1));
            final byte[] b = new byte[2];
            input.readFully(b);
            assertArrayEquals(new byte[]{(byte)1, (byte)2}, b);
            assertEquals(0, input.skipBytes(1));
            return b;
        });
    }

    // ##### Tests for write(byte[], int, int), readFully(byte[], int, int)

    @Test(expected = NullPointerException.class)
    public void testNPEOnWriteWithOffset() {
        serdeNull((output, object) -> output.write(null, 0, 0), input -> new byte[0]);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testOOBOnWriteWithOffset() {
        serdeNull((output, object) -> output.write(new byte[0], 0, 1), input -> new byte[0]);
    }

    @Test(expected = NullPointerException.class)
    public void testNPEOnReadFullyWithOffset() {
        serdeNull((output, object) -> {
        }, input -> {
            input.readFully(null, 0, 0);
            return new byte[0];
        });
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testOOBOnReadFullyWithOffset() {
        serdeNull((output, object) -> {
        }, input -> {
            input.readFully(new byte[0], 0, 1);
            return new byte[0];
        });
    }

    @Test
    public void testEOFOnReadFullyWithOffset() {
        serdeNull((output, object) -> {
        }, input -> {
            try {
                input.readFully(new byte[1], 0, 1);
                failOnUnthrownEOFException();
            } catch (EOFException e) { // NOSONAR
            }
            return new byte[0];
        });
    }

    @Test
    public void testWriteWithOffsetReadFullyWithOffset() {
        serdeByteArray(new byte[]{Byte.MIN_VALUE, Byte.valueOf((byte)0), Byte.MAX_VALUE}, (output, object) -> {
            output.writeInt(object.length);
            output.write(object, object.length - 1, 1);
            output.write(object, 0, object.length - 1);
        }, input -> {
            final byte[] result = new byte[input.readInt()];
            input.readFully(result, result.length - 1, 1);
            input.readFully(result, 0, result.length - 1);
            return result;
        });
    }

    // ##### Tests for writeBoolean(boolean) and readBoolean()

    @Test
    public void testEOFOnReadBoolean() {
        serdeNull((output, object) -> {
        }, input -> {
            try {
                input.readBoolean();
                failOnUnthrownEOFException();
            } catch (EOFException e) { // NOSONAR
            }
            return Boolean.FALSE;
        });
    }

    @Test
    public void testWriteBooleanReadBoolean() {
        final ObjectSerializer<Boolean> ser = (output, object) -> output.writeBoolean(object.booleanValue());
        final ObjectDeserializer<Boolean> de = input -> Boolean.valueOf(input.readBoolean());

        serdeT(Boolean.FALSE, ser, de);
        serdeT(Boolean.TRUE, ser, de);
    }

    // ##### Tests for writeByte(int) and readByte()

    @Test
    public void testEOFOnReadByte() {
        serdeNull((output, object) -> {
        }, input -> {
            try {
                input.readByte();
                failOnUnthrownEOFException();
            } catch (EOFException e) { // NOSONAR
            }
            return -1;
        });
    }

    @Test
    public void testWriteByteReadByte() {
        final ObjectSerializer<Byte> ser = (output, object) -> output.writeByte(object.byteValue());
        final ObjectDeserializer<Byte> de = input -> Byte.valueOf(input.readByte());

        serdeT(Byte.MIN_VALUE, ser, de);
        serdeT(Byte.valueOf((byte)-1), ser, de);
        serdeT(Byte.valueOf((byte)0), ser, de);
        serdeT(Byte.valueOf((byte)1), ser, de);
        serdeT(Byte.MAX_VALUE, ser, de);
    }

    // ##### Tests for write(int) and readUnsignedByte()

    @Test
    public void testEOFOnReadUnsignedByte() {
        serdeNull((output, object) -> {
        }, input -> {
            try {
                input.readUnsignedByte();
                failOnUnthrownEOFException();
            } catch (EOFException e) { // NOSONAR
            }
            return -1;
        });
    }

    @Test
    public void testWriteReadUnsignedByte() {
        final ObjectSerializer<Integer> ser = (output, object) -> output.write(object.byteValue());
        final ObjectDeserializer<Integer> de = input -> Integer.valueOf(input.readUnsignedByte());

        serdeT(Integer.valueOf(0), ser, de);
        serdeT(Integer.valueOf(Byte.MAX_VALUE), ser, de);
        serdeT(Integer.valueOf((1 << 8) - 1), ser, de);
    }

    // ##### Tests for writeShort(int) and readShort()

    @Test
    public void testEOFOnReadShort() {
        serdeNull((output, object) -> {
        }, input -> {
            try {
                input.readShort();
                failOnUnthrownEOFException();
            } catch (EOFException e) { // NOSONAR
            }
            return -1;
        });
    }

    @Test
    public void testWriteShortReadShort() {
        final ObjectSerializer<Short> ser = (output, object) -> output.writeShort(object.shortValue());
        final ObjectDeserializer<Short> de = input -> Short.valueOf(input.readShort());

        serdeT(Short.MIN_VALUE, ser, de);
        serdeT((short)-1, ser, de);
        serdeT((short)0, ser, de);
        serdeT((short)1, ser, de);
        serdeT(Short.MAX_VALUE, ser, de);
    }

    // ##### Tests for readUnsignedShort()

    @Test
    public void testEOFOnReadUnsignedShort() {
        serdeNull((output, object) -> {
        }, input -> {
            try {
                input.readUnsignedShort();
                failOnUnthrownEOFException();
            } catch (EOFException e) { // NOSONAR
            }
            return -1;
        });
    }

    @Test
    public void testReadUnsignedShort() {
        final ObjectSerializer<Integer> ser = (output, object) -> output.writeShort(object.shortValue());
        final ObjectDeserializer<Integer> de = input -> Integer.valueOf(input.readUnsignedShort());

        serdeT(Integer.valueOf(0), ser, de);
        serdeT(Integer.valueOf(Short.MAX_VALUE), ser, de);
        serdeT(Integer.valueOf((1 << 16) - 1), ser, de);
    }

    // ##### Tests for writeChar(int) and readChar()

    @Test
    public void testEOFOnReadChar() {
        serdeNull((output, object) -> {
        }, input -> {
            try {
                input.readChar();
                failOnUnthrownEOFException();
            } catch (EOFException e) { // NOSONAR
            }
            return (char)-1;
        });
    }

    @Test
    public void testWriteCharReadChar() {
        final ObjectSerializer<Character> ser = (output, object) -> output.writeChar(object.charValue());
        final ObjectDeserializer<Character> de = input -> Character.valueOf(input.readChar());

        serdeT((char)Short.MIN_VALUE, ser, de);
        serdeT((char)0, ser, de);
        serdeT((char)Short.MAX_VALUE, ser, de);
    }

    // ##### Tests for writeInt(int) and readInt()

    @Test
    public void testEOFOnReadInt() {
        serdeNull((output, object) -> {
        }, input -> {
            try {
                input.readInt();
                failOnUnthrownEOFException();
            } catch (EOFException e) { // NOSONAR
            }
            return -1;
        });
    }

    @Test
    public void testWriteIntReadInt() {
        final ObjectSerializer<Integer> ser = (output, object) -> output.writeInt(object.intValue());
        final ObjectDeserializer<Integer> de = input -> Integer.valueOf(input.readInt());

        serdeT(Integer.MIN_VALUE, ser, de);
        serdeT(Integer.valueOf(-1), ser, de);
        serdeT(Integer.valueOf(0), ser, de);
        serdeT(Integer.valueOf(1), ser, de);
        serdeT(Integer.MAX_VALUE, ser, de);
    }

    // ##### Tests for writeLong(long) and readLong()

    @Test
    public void testEOFOnReadLong() {
        serdeNull((output, object) -> {
        }, input -> {
            try {
                input.readLong();
                failOnUnthrownEOFException();
            } catch (EOFException e) { // NOSONAR
            }
            return -1L;
        });
    }

    @Test
    public void testWriteLongReadLong() {
        final ObjectSerializer<Long> ser = (output, object) -> output.writeLong(object.longValue());
        final ObjectDeserializer<Long> de = input -> Long.valueOf(input.readLong());

        serdeT(Long.MIN_VALUE, ser, de);
        serdeT(Long.valueOf(-1L), ser, de);
        serdeT(Long.valueOf(0L), ser, de);
        serdeT(Long.valueOf(1L), ser, de);
        serdeT(Long.MAX_VALUE, ser, de);
    }

    // ##### Tests for writeFloat(float) and readFloat()

    @Test
    public void testEOFOnReadFloat() {
        serdeNull((output, object) -> {
        }, input -> {
            try {
                input.readFloat();
                failOnUnthrownEOFException();
            } catch (EOFException e) { // NOSONAR
            }
            return -1F;
        });
    }

    @Test
    public void testWriteFloatReadFloat() {
        final ObjectSerializer<Float> ser = (output, object) -> output.writeFloat(object.floatValue());
        final ObjectDeserializer<Float> de = input -> Float.valueOf(input.readFloat());

        serdeT(Float.MIN_VALUE, ser, de);
        serdeT(Float.valueOf(0F), ser, de);
        serdeT(Float.MAX_VALUE, ser, de);
    }

    // ##### Tests for writeDouble(double) and readDouble()

    @Test
    public void testEOFOnReadDouble() {
        serdeNull((output, object) -> {
        }, input -> {
            try {
                input.readDouble();
                failOnUnthrownEOFException();
            } catch (EOFException e) { // NOSONAR
            }
            return -1D;
        });
    }

    @Test
    public void testWriteDoubleReadDouble() {
        final ObjectSerializer<Double> ser = (output, object) -> output.writeDouble(object.doubleValue());
        final ObjectDeserializer<Double> de = input -> Double.valueOf(input.readDouble());

        serdeT(Double.MIN_VALUE, ser, de);
        serdeT(Double.valueOf(0D), ser, de);
        serdeT(Double.MAX_VALUE, ser, de);
    }

    // ##### Tests for writeBytes(String)

    @Test(expected = NullPointerException.class)
    public void testNPEOnWriteBytes() {
        serdeNull((output, object) -> output.writeBytes(null), input -> null);
    }

    @Test
    public void testWriteBytes() {
        final ObjectSerializer<String> ser = (output, object) -> {
            output.writeInt(object.length());
            output.writeBytes(object);
        };
        final ObjectDeserializer<String> de = input -> {
            final byte[] b = new byte[input.readInt()];
            input.readFully(b);
            final char[] c = new char[b.length];
            for (int i = 0; i < b.length; i++) {
                c[i] = (char)(b[i] & 0xFF);
            }
            return String.valueOf(c);
        };

        serdeString("foo", "foo", ser, de);
        serdeString(String.valueOf(new char[]{'\u0000', '\u00ff', '\uffff', '\uff00'}),
            String.valueOf(new char[]{'\u0000', '\u00ff', '\u00ff', '\u0000'}), ser, de);
    }

    // ##### Tests for writeChars(String) and readLine()

    @Test(expected = NullPointerException.class)
    public void testNPEOnWriteChars() {
        serdeNull((output, object) -> output.writeChars(null), input -> null);
    }

    @Test
    public void testWriteBytesReadLine() {
        final ObjectSerializer<String> ser = (output, object) -> output.writeBytes(object);
        final ObjectDeserializer<String> de = input -> input.readLine();

        serdeString("", null, ser, de); // NOSONAR
        serdeString("foo", "foo", ser, de); // NOSONAR
        serdeString("foo\rbar", "foobar", ser, de);
        serdeString("foo\r\nbar", "foo", ser, de);
        serdeString("foo\nbar", "foo", ser, de);
    }

    // TODO: write test for writeChars(String)

    // ##### Tests for writeUTF(String) and readUTF()

    @Test(expected = NullPointerException.class)
    public void testNPEOnWriteUTF() {
        serdeNull((output, object) -> output.writeUTF(null), input -> null);
    }

    @Test
    public void testEOFOnReadUTF() {
        serdeNull((output, object) -> output.writeInt(1), input -> {
            try {
                input.readUTF();
                failOnUnthrownEOFException();
            } catch (EOFException e) { // NOSONAR
            }
            return new byte[0];
        });
    }

    @Test
    public void testWriteUTFReadUTF() {
        final ObjectSerializer<String> ser = (output, object) -> output.writeUTF(object);
        final ObjectDeserializer<String> de = input -> input.readUTF();

        serdeT("", ser, de);
        serdeT(String.valueOf(Character.toChars(0x0000)), ser, de);
        serdeT(String.valueOf(Character.toChars(0x0001)), ser, de);
        serdeT(String.valueOf(Character.toChars(0x007F)), ser, de);
        serdeT(String.valueOf(Character.toChars(0x0080)), ser, de);
        serdeT(String.valueOf(Character.toChars(0x07FF)), ser, de);
        serdeT(String.valueOf(Character.toChars(0x0800)), ser, de);
        serdeT(String.valueOf(Character.toChars(0xFFFF)), ser, de);
        serdeT(String.valueOf(Character.toChars(0x10000)), ser, de);
        serdeT(String.valueOf(Character.toChars(0x10FFFF)), ser, de);
        serdeT(String.valueOf(new char[65535]), ser, de);
        serdeT(String.valueOf(new char[65536]), ser, de);
    }

    // ##### Tests for serialize(int, Object) and deserialize(int)

    @Test
    public void testSerializeDeserialize() {
        try (final LargeVarBinaryVector vector = new LargeVarBinaryVector("test", m_alloc)) {
            vector.allocateNew(0, 0);
            final ObjectSerializer<Integer> ser = (output, object) -> output.writeInt(object.intValue());
            final ObjectDeserializer<Integer> de = input -> Integer.valueOf(input.readInt());

            serialize(1, 1, vector, ser);
            serialize(3, 3, vector, ser);

            assertTrue(vector.isNull(0));
            assertFalse(vector.isNull(1));
            assertTrue(vector.isNull(2));
            assertFalse(vector.isNull(3));
            assertTrue(vector.isNull(4));

            assertEquals(Integer.valueOf(1), deserialize(1, vector, de));
            assertEquals(Integer.valueOf(3), deserialize(3, vector, de));
        }
    }

    @Test
    public void testRead() {
        ObjectSerializer<byte[]> simpleWrite = (o, d) -> o.write(d);
        var data = new byte[]{10, 20, 30};

        // Read once into buffer that is big enough
        serdeByteArray(data, simpleWrite, (in) -> {
            var buffer = new byte[10];
            var read = in.read(buffer, 0, buffer.length);
            assertEquals(data.length, read);
            var output = new byte[read];
            System.arraycopy(buffer, 0, output, 0, read);
            return output;
        });

        // Read twice into buffer that is not big enough
        serdeByteArray(data, simpleWrite, (in) -> {
            var output = new byte[data.length];
            var buffer = new byte[2];
            var read = in.read(buffer, 0, buffer.length);
            assertEquals(2, read);
            System.arraycopy(buffer, 0, output, 0, read);
            read = in.read(buffer, 0, buffer.length);
            assertEquals(1, read);
            System.arraycopy(buffer, 0, output, 2, read);
            return output;
        });

        // Read into buffer exactly big enough
        serdeByteArray(data, simpleWrite, (in) -> {
            var buffer = new byte[3];
            var read = in.read(buffer, 0, buffer.length);
            assertEquals(3, read);
            read = in.read(new byte[10], 0, 10); // No data available anymore -> should return -1
            assertEquals(-1, read);
            return buffer;
        });

        // Read empty bytes
        serdeByteArray(new byte[0], simpleWrite, (in) -> {
            var buffer = new byte[10];
            var read = in.read(buffer, 0, buffer.length);
            assertEquals(-1, read); // No data available anymore -> should return -1
            return new byte[0];
        });

        // Read into empty buffer
        serdeByteArray(data, simpleWrite, (in) -> {
            var buffer = new byte[0];
            var read = in.read(buffer, 0, buffer.length);
            assertEquals(0, read); // No space available

            // Just return what we expect
            return data;
        });

        // Try to read more than the buffer size
        assertThrows(IndexOutOfBoundsException.class, () -> {
            serdeByteArray(data, simpleWrite, (in) -> {
                var buffer = new byte[10];
                // Try to read more than we have buffer space
                in.read(buffer, 0, 20);
                return null;
            });
        });

        // Read with offsets
        serdeByteArray(data, simpleWrite, (in) -> {
            var output = new byte[data.length];
            var buffer = new byte[10];
            var read = in.read(buffer, 5, 2);
            assertEquals(2, read);
            System.arraycopy(buffer, 5, output, 0, read);

            read = in.read(buffer, 2, 7);
            assertEquals(1, read);
            System.arraycopy(buffer, 2, output, 2, read);
            return output;
        });
    }

    @Test
    public void testReadBytes() {
        ObjectSerializer<byte[]> simpleWrite = (o, d) -> o.write(d);
        ObjectDeserializer<byte[]> simpleReadBytes = (in) -> in.readBytes();

        // Simple
        serdeByteArray(new byte[]{10, 20, 30}, simpleWrite, simpleReadBytes);

        // Empty data
        serdeByteArray(new byte[]{}, simpleWrite, simpleReadBytes);
    }
}
