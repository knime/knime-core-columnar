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
package org.knime.core.columnar.arrow.onheap.data;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.EOFException;
import java.io.IOException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.knime.core.columnar.arrow.offheap.data.ArrowBufIOTest;
import org.knime.core.table.io.ReadableDataInput;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

import it.unimi.dsi.fastutil.bytes.ByteBigArrayBigList;

/**
 * Mostly copied from {@link ArrowBufIOTest} but working testing ByteBigListDataOutput and ByteBigArrayDataInput.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
class ByteBigInputOutputTest {

    private static void serdeNull(final ObjectDeserializer<?> de) throws IOException {
        serdeNull((output, object) -> {
        }, de);
    }

    private static void serdeNull(final ObjectSerializer<?> ser, final ObjectDeserializer<?> de) throws IOException {
        var data = new ByteBigArrayBigList();
        var output = new ByteBigListDataOutput(data, 0);
        ser.serialize(output, null);

        var input = new ByteBigArrayDataInput(data.elements(), 0, data.size64());
        de.deserialize(input);
    }

    private static void serdeString(final String s, final String expect, final ObjectSerializer<String> ser,
        final ObjectDeserializer<String> de) throws IOException {
        var data = new ByteBigArrayBigList();
        var output = new ByteBigListDataOutput(data, 0);
        ser.serialize(output, s);

        var input = new ByteBigArrayDataInput(data.elements(), 0, data.size64());
        Assertions.assertEquals(expect, de.deserialize(input));
    }

    private static <T> void serdeT(final T obj, final ObjectSerializer<T> ser, final ObjectDeserializer<T> de)
        throws IOException {
        var data = new ByteBigArrayBigList();
        var output = new ByteBigListDataOutput(data, 0);
        ser.serialize(output, obj);

        var input = new ByteBigArrayDataInput(data.elements(), 0, data.size64());
        Assertions.assertEquals(obj, de.deserialize(input));
    }

    private static void serdeByteArray(final byte[] arr, final ObjectSerializer<byte[]> ser,
        final ObjectDeserializer<byte[]> de) throws IOException {
        var data = new ByteBigArrayBigList();
        var output = new ByteBigListDataOutput(data, 0);
        ser.serialize(output, arr);

        var input = new ByteBigArrayDataInput(data.elements(), 0, data.size64());
        assertArrayEquals(arr, de.deserialize(input));
    }

    // ##### Tests for write(byte[]) and readFully(byte[])

    @Test
    void testNPEOnWrite() throws IOException {
        assertThrows(NullPointerException.class,
            () -> serdeNull((output, object) -> output.write(null), input -> new byte[0]));
    }

    @Test
    void testNPEOnReadFully() throws IOException {
        assertThrows(NullPointerException.class, () -> serdeNull(input -> {
            input.readFully(null);
            return new byte[0];
        }));
    }

    @Test
    void testEOFOnReadFully() throws IOException {
        assertThrows(EOFException.class, () -> serdeNull(input -> {
            input.readFully(new byte[1]);
            return new byte[0];
        }));
    }

    @Test
    void testWriteReadFully() throws IOException {
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
    void testSkipBytes() throws IOException {
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

    @Test
    void testNPEOnWriteWithOffset() throws IOException {
        assertThrows(NullPointerException.class,
            () -> serdeNull((output, object) -> output.write(null, 0, 0), input -> new byte[0]));
    }

    @Test
    void testOOBOnWriteWithOffset() throws IOException {
        assertThrows(IndexOutOfBoundsException.class,
            () -> serdeNull((output, object) -> output.write(new byte[0], 0, 1), input -> new byte[0]));
    }

    @Test
    void testNPEOnReadFullyWithOffset() throws IOException {
        assertThrows(NullPointerException.class, () -> serdeNull(input -> {
            input.readFully(null, 0, 0);
            return new byte[0];
        }));
    }

    @Test
    void testOOBOnReadFullyWithOffset() throws IOException {
        assertThrows(IndexOutOfBoundsException.class, () -> serdeNull(input -> {
            input.readFully(new byte[0], 0, 1);
            return new byte[0];
        }));
    }

    @Test
    void testEOFOnReadFullyWithOffset() throws IOException {
        assertThrows(EOFException.class, () -> serdeNull(input -> {
            input.readFully(new byte[1], 0, 1);
            return new byte[0];
        }));
    }

    @Test
    void testWriteWithOffsetReadFullyWithOffset() throws IOException {
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
    void testEOFOnReadBoolean() throws IOException {
        assertThrows(EOFException.class, () -> serdeNull(ReadableDataInput::readBoolean));
    }

    @Test
    void testWriteBooleanReadBoolean() throws IOException {
        final ObjectSerializer<Boolean> ser = (output, object) -> output.writeBoolean(object.booleanValue());
        final ObjectDeserializer<Boolean> de = input -> Boolean.valueOf(input.readBoolean());

        serdeT(Boolean.FALSE, ser, de);
        serdeT(Boolean.TRUE, ser, de);
    }

    // ##### Tests for writeByte(int) and readByte()

    @Test
    void testEOFOnReadByte() throws IOException {
        assertThrows(EOFException.class, () -> serdeNull(ReadableDataInput::readByte));
    }

    @Test
    void testWriteByteReadByte() throws IOException {
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
    void testEOFOnReadUnsignedByte() throws IOException {
        assertThrows(EOFException.class, () -> serdeNull(ReadableDataInput::readUnsignedByte));
    }

    @Test
    void testWriteReadUnsignedByte() throws IOException {
        final ObjectSerializer<Integer> ser = (output, object) -> output.write(object.byteValue());
        final ObjectDeserializer<Integer> de = input -> Integer.valueOf(input.readUnsignedByte());

        serdeT(Integer.valueOf(0), ser, de);
        serdeT(Integer.valueOf(Byte.MAX_VALUE), ser, de);
        serdeT(Integer.valueOf((1 << 8) - 1), ser, de);
    }

    // ##### Tests for writeShort(int) and readShort()

    @Test
    void testEOFOnReadShort() throws IOException {
        assertThrows(EOFException.class, () -> serdeNull(ReadableDataInput::readShort));
    }

    @Test
    void testWriteShortReadShort() throws IOException {
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
    void testEOFOnReadUnsignedShort() throws IOException {
        assertThrows(EOFException.class, () -> serdeNull(ReadableDataInput::readUnsignedShort));
    }

    @Test
    void testReadUnsignedShort() throws IOException {
        final ObjectSerializer<Integer> ser = (output, object) -> output.writeShort(object.shortValue());
        final ObjectDeserializer<Integer> de = input -> Integer.valueOf(input.readUnsignedShort());

        serdeT(Integer.valueOf(0), ser, de);
        serdeT(Integer.valueOf(Short.MAX_VALUE), ser, de);
        serdeT(Integer.valueOf((1 << 16) - 1), ser, de);
    }

    // ##### Tests for writeChar(int) and readChar()

    @Test
    void testEOFOnReadChar() throws IOException {
        assertThrows(EOFException.class, () -> serdeNull(ReadableDataInput::readChar));
    }

    @Test
    void testWriteCharReadChar() throws IOException {
        final ObjectSerializer<Character> ser = (output, object) -> output.writeChar(object.charValue());
        final ObjectDeserializer<Character> de = input -> Character.valueOf(input.readChar());

        serdeT((char)Short.MIN_VALUE, ser, de);
        serdeT((char)0, ser, de);
        serdeT((char)Short.MAX_VALUE, ser, de);
    }

    // ##### Tests for writeInt(int) and readInt()

    @Test
    void testEOFOnReadInt() throws IOException {
        assertThrows(EOFException.class, () -> serdeNull(ReadableDataInput::readInt));
    }

    @Test
    void testWriteIntReadInt() throws IOException {
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
    void testEOFOnReadLong() throws IOException {
        assertThrows(EOFException.class, () -> serdeNull(ReadableDataInput::readLong));
    }

    @Test
    void testWriteLongReadLong() throws IOException {
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
    void testEOFOnReadFloat() throws IOException {
        assertThrows(EOFException.class, () -> serdeNull(ReadableDataInput::readFloat));
    }

    @Test
    void testWriteFloatReadFloat() throws IOException {
        final ObjectSerializer<Float> ser = (output, object) -> output.writeFloat(object.floatValue());
        final ObjectDeserializer<Float> de = input -> Float.valueOf(input.readFloat());

        serdeT(Float.MIN_VALUE, ser, de);
        serdeT(Float.valueOf(0F), ser, de);
        serdeT(Float.MAX_VALUE, ser, de);
    }

    // ##### Tests for writeDouble(double) and readDouble()

    @Test
    void testEOFOnReadDouble() throws IOException {
        assertThrows(EOFException.class, () -> serdeNull(ReadableDataInput::readDouble));
    }

    @Test
    void testWriteDoubleReadDouble() throws IOException {
        final ObjectSerializer<Double> ser = (output, object) -> output.writeDouble(object.doubleValue());
        final ObjectDeserializer<Double> de = input -> Double.valueOf(input.readDouble());

        serdeT(Double.MIN_VALUE, ser, de);
        serdeT(Double.valueOf(0D), ser, de);
        serdeT(Double.MAX_VALUE, ser, de);
    }

    // ##### Tests for writeBytes(String)

    @Test
    void testNPEOnWriteBytes() throws IOException {
        assertThrows(NullPointerException.class,
            () -> serdeNull((output, object) -> output.writeBytes(null), input -> null));
    }

    @Test
    void testWriteBytes() throws IOException {
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

    @Test
    void testNPEOnWriteChars() throws IOException {
        assertThrows(NullPointerException.class,
            () -> serdeNull((output, object) -> output.writeChars(null), input -> null));
    }

    @Test
    void testWriteBytesReadLine() throws IOException {
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

    @Test
    void testNPEOnWriteUTF() throws IOException {
        assertThrows(NullPointerException.class,
            () -> serdeNull((output, object) -> output.writeUTF(null), input -> null));
    }

    @Test
    void testEOFOnReadUTF() throws IOException {
        assertThrows(EOFException.class,
            () -> serdeNull((output, object) -> output.writeInt(1), ReadableDataInput::readUTF));
    }

    @Test
    void testWriteUTFReadUTF() throws IOException {
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
    void testSerializeDeserialize() throws IOException {
        final ObjectSerializer<Integer> ser = (output, object) -> output.writeInt(object.intValue());
        final ObjectDeserializer<Integer> de = input -> Integer.valueOf(input.readInt());

        var data = new ByteBigArrayBigList();
        var output = new ByteBigListDataOutput(data, 0);

        ser.serialize(output, 1);
        ser.serialize(output, 3);

        var input = new ByteBigArrayDataInput(data.elements(), 0, data.size64());
        assertEquals(Integer.valueOf(1), de.deserialize(input));
        assertEquals(Integer.valueOf(3), de.deserialize(input));
    }

    @Test
    void testRead() throws IOException {
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
    void testReadBytes() throws IOException {
        ObjectSerializer<byte[]> simpleWrite = (o, d) -> o.write(d);
        ObjectDeserializer<byte[]> simpleReadBytes = (in) -> in.readBytes();

        // Simple
        serdeByteArray(new byte[]{10, 20, 30}, simpleWrite, simpleReadBytes);

        // Empty data
        serdeByteArray(new byte[]{}, simpleWrite, simpleReadBytes);
    }
}
