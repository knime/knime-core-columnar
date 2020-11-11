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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.columnar.data.ObjectData.ObjectDataSerializer;

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

    private void serdeNull(final ObjectDataSerializer<?> ser) {
        try (final LargeVarBinaryVector vector = new LargeVarBinaryVector("test", m_alloc)) {
            vector.allocateNew(0, 0);
            final ArrowBufIO<?> bio = new ArrowBufIO<>(vector, ser);
            bio.serialize(0, null);
            bio.deserialize(0);
        }
    }

    private void serdeString(final String s, final String expect, final ObjectDataSerializer<String> ser) {
        try (final LargeVarBinaryVector vector = new LargeVarBinaryVector("test", m_alloc)) {
            vector.allocateNew(0, 0);
            final ArrowBufIO<String> bio = new ArrowBufIO<>(vector, ser);
            bio.serialize(0, s);
            assertEquals(expect, bio.deserialize(0));
        }
    }

    private <T> void serdeT(final T obj, final ObjectDataSerializer<T> ser) {
        try (final LargeVarBinaryVector vector = new LargeVarBinaryVector("test", m_alloc)) {
            vector.allocateNew(0, 0);
            final ArrowBufIO<T> bio = new ArrowBufIO<>(vector, ser);
            bio.serialize(0, obj);
            assertEquals(obj, bio.deserialize(0));
        }
    }

    private void serdeByteArray(final byte[] arr, final ObjectDataSerializer<byte[]> ser) {
        try (final LargeVarBinaryVector vector = new LargeVarBinaryVector("test", m_alloc)) {
            vector.allocateNew(0, 0);
            final ArrowBufIO<byte[]> bio = new ArrowBufIO<>(vector, ser);
            bio.serialize(0, arr);
            assertArrayEquals(arr, bio.deserialize(0));
        }
    }

    // ##### Tests for write(byte[]) and readFully(byte[])

    @Test(expected = NullPointerException.class)
    public void testNPEOnWrite() {
        serdeNull(new ObjectDataSerializer<byte[]>() {
            @Override
            public void serialize(final byte[] obj, final DataOutput output) throws IOException {
                output.write(null);
            }

            @Override
            public byte[] deserialize(final DataInput input) throws IOException {
                return new byte[0];
            }
        });
    }

    @Test(expected = NullPointerException.class)
    public void testNPEOnReadFully() {
        serdeNull(new ObjectDataSerializer<byte[]>() {
            @Override
            public void serialize(final byte[] obj, final DataOutput output) throws IOException { // NOSONAR
            }

            @Override
            public byte[] deserialize(final DataInput input) throws IOException {
                input.readFully(null);
                return new byte[0];
            }
        });
    }

    @Test
    public void testEOFOnReadFully() {
        serdeNull(new ObjectDataSerializer<byte[]>() {
            @Override
            public void serialize(final byte[] obj, final DataOutput output) throws IOException { // NOSONAR
            }

            @Override
            public byte[] deserialize(final DataInput input) throws IOException {
                try {
                    input.readFully(new byte[1]);
                    failOnUnthrownEOFException();
                } catch (EOFException e) { // NOSONAR
                }
                return new byte[0];
            }
        });
    }

    @Test
    public void testWriteReadFully() {
        final ObjectDataSerializer<byte[]> ser = new ObjectDataSerializer<byte[]>() {
            @Override
            public void serialize(final byte[] obj, final DataOutput output) throws IOException {
                output.writeInt(obj.length);
                output.write(obj);
            }

            @Override
            public byte[] deserialize(final DataInput input) throws IOException {
                final byte[] result = new byte[input.readInt()];
                input.readFully(result);
                return result;
            }
        };

        serdeByteArray(new byte[0], ser);
        serdeByteArray(new byte[]{Byte.MIN_VALUE, Byte.valueOf((byte)0), Byte.MAX_VALUE}, ser);
    }

    // ##### Tests for skipBytes(int)

    @Test
    public void testSkipBytes() {
        serdeNull(new ObjectDataSerializer<byte[]>() {
            @Override
            public void serialize(final byte[] obj, final DataOutput output) throws IOException {
                output.write(new byte[]{(byte)0, (byte)1, (byte)2});
            }

            @Override
            public byte[] deserialize(final DataInput input) throws IOException {
                assertEquals(1, input.skipBytes(1));
                final byte[] b = new byte[2];
                input.readFully(b);
                assertArrayEquals(new byte[]{(byte)1, (byte)2}, b);
                assertEquals(0, input.skipBytes(1));
                return b;
            }
        });
    }

    // ##### Tests for write(byte[], int, int), readFully(byte[], int, int)

    @Test(expected = NullPointerException.class)
    public void testNPEOnWriteWithOffset() {
        serdeNull(new ObjectDataSerializer<byte[]>() {
            @Override
            public void serialize(final byte[] obj, final DataOutput output) throws IOException {
                output.write(null, 0, 0);
            }

            @Override
            public byte[] deserialize(final DataInput input) throws IOException {
                return new byte[0];
            }
        });
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testOOBOnWriteWithOffset() {
        serdeNull(new ObjectDataSerializer<byte[]>() {
            @Override
            public void serialize(final byte[] obj, final DataOutput output) throws IOException {
                output.write(new byte[0], 0, 1);
            }

            @Override
            public byte[] deserialize(final DataInput input) throws IOException {
                return new byte[0];
            }
        });
    }

    @Test(expected = NullPointerException.class)
    public void testNPEOnReadFullyWithOffset() {
        serdeNull(new ObjectDataSerializer<byte[]>() {
            @Override
            public void serialize(final byte[] obj, final DataOutput output) throws IOException { // NOSONAR
            }

            @Override
            public byte[] deserialize(final DataInput input) throws IOException {
                input.readFully(null, 0, 0);
                return new byte[0];
            }
        });
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testOOBOnReadFullyWithOffset() {
        serdeNull(new ObjectDataSerializer<byte[]>() {
            @Override
            public void serialize(final byte[] obj, final DataOutput output) throws IOException { // NOSONAR
            }

            @Override
            public byte[] deserialize(final DataInput input) throws IOException {
                input.readFully(new byte[0], 0, 1);
                return new byte[0];
            }
        });
    }

    @Test
    public void testEOFOnReadFullyWithOffset() {
        serdeNull(new ObjectDataSerializer<byte[]>() {
            @Override
            public void serialize(final byte[] obj, final DataOutput output) throws IOException { // NOSONAR
            }

            @Override
            public byte[] deserialize(final DataInput input) throws IOException {
                try {
                    input.readFully(new byte[1], 0, 1);
                    failOnUnthrownEOFException();
                } catch (EOFException e) { // NOSONAR
                }
                return new byte[0];
            }
        });
    }

    @Test
    public void testWriteWithOffsetReadFullyWithOffset() {
        serdeByteArray(new byte[]{Byte.MIN_VALUE, Byte.valueOf((byte)0), Byte.MAX_VALUE},
            new ObjectDataSerializer<byte[]>() {
                @Override
                public void serialize(final byte[] obj, final DataOutput output) throws IOException {
                    output.writeInt(obj.length);
                    output.write(obj, obj.length - 1, 1);
                    output.write(obj, 0, obj.length - 1);
                }

                @Override
                public byte[] deserialize(final DataInput input) throws IOException {
                    final byte[] result = new byte[input.readInt()];
                    input.readFully(result, result.length - 1, 1);
                    input.readFully(result, 0, result.length - 1);
                    return result;
                }
            });
    }

    // ##### Tests for writeBoolean(boolean) and readBoolean()

    @Test
    public void testEOFOnReadBoolean() {
        serdeNull(new ObjectDataSerializer<Boolean>() {
            @Override
            public void serialize(final Boolean obj, final DataOutput output) throws IOException { // NOSONAR
            }

            @Override
            public Boolean deserialize(final DataInput input) throws IOException {
                try {
                    input.readBoolean();
                    failOnUnthrownEOFException();
                } catch (EOFException e) { // NOSONAR
                }
                return Boolean.FALSE;
            }
        });
    }

    @Test
    public void testWriteBooleanReadBoolean() {
        final ObjectDataSerializer<Boolean> ser = new ObjectDataSerializer<Boolean>() {
            @Override
            public void serialize(final Boolean obj, final DataOutput output) throws IOException {
                output.writeBoolean(obj.booleanValue());
            }

            @Override
            public Boolean deserialize(final DataInput input) throws IOException {
                return Boolean.valueOf(input.readBoolean());
            }
        };

        serdeT(Boolean.FALSE, ser);
        serdeT(Boolean.TRUE, ser);
    }

    // ##### Tests for writeByte(int) and readByte()

    @Test
    public void testEOFOnReadByte() {
        serdeNull(new ObjectDataSerializer<Byte>() {
            @Override
            public void serialize(final Byte obj, final DataOutput output) throws IOException { // NOSONAR
            }

            @Override
            public Byte deserialize(final DataInput input) throws IOException {
                try {
                    input.readByte();
                    failOnUnthrownEOFException();
                } catch (EOFException e) { // NOSONAR
                }
                return -1;
            }
        });
    }

    @Test
    public void testWriteByteReadByte() {
        final ObjectDataSerializer<Byte> ser = new ObjectDataSerializer<Byte>() {
            @Override
            public void serialize(final Byte obj, final DataOutput output) throws IOException {
                output.writeByte(obj.byteValue());
            }

            @Override
            public Byte deserialize(final DataInput input) throws IOException {
                return Byte.valueOf(input.readByte());
            }
        };

        serdeT(Byte.MIN_VALUE, ser);
        serdeT(Byte.valueOf((byte)-1), ser);
        serdeT(Byte.valueOf((byte)0), ser);
        serdeT(Byte.valueOf((byte)1), ser);
        serdeT(Byte.MAX_VALUE, ser);
    }

    // ##### Tests for write(int) and readUnsignedByte()

    @Test
    public void testEOFOnReadUnsignedByte() {
        serdeNull(new ObjectDataSerializer<Byte>() {
            @Override
            public void serialize(final Byte obj, final DataOutput output) throws IOException { // NOSONAR
            }

            @Override
            public Byte deserialize(final DataInput input) throws IOException {
                try {
                    input.readUnsignedByte();
                    failOnUnthrownEOFException();
                } catch (EOFException e) { // NOSONAR
                }
                return -1;
            }
        });
    }

    @Test
    public void testWriteReadUnsignedByte() {
        final ObjectDataSerializer<Integer> ser = new ObjectDataSerializer<Integer>() {
            @Override
            public void serialize(final Integer obj, final DataOutput output) throws IOException {
                output.write(obj.byteValue());
            }

            @Override
            public Integer deserialize(final DataInput input) throws IOException {
                return Integer.valueOf(input.readUnsignedByte());
            }
        };

        serdeT(Integer.valueOf(0), ser);
        serdeT(Integer.valueOf(Byte.MAX_VALUE), ser);
        serdeT(Integer.valueOf((1 << 8) - 1), ser);
    }

    // ##### Tests for writeShort(int) and readShort()

    @Test
    public void testEOFOnReadShort() {
        serdeNull(new ObjectDataSerializer<Short>() {
            @Override
            public void serialize(final Short obj, final DataOutput output) throws IOException { // NOSONAR
            }

            @Override
            public Short deserialize(final DataInput input) throws IOException {
                try {
                    input.readShort();
                    failOnUnthrownEOFException();
                } catch (EOFException e) { // NOSONAR
                }
                return -1;
            }
        });
    }

    @Test
    public void testWriteShortReadShort() {
        final ObjectDataSerializer<Short> ser = new ObjectDataSerializer<Short>() {
            @Override
            public void serialize(final Short obj, final DataOutput output) throws IOException {
                output.writeShort(obj.shortValue());
            }

            @Override
            public Short deserialize(final DataInput input) throws IOException {
                return Short.valueOf(input.readShort());
            }
        };

        serdeT(Short.MIN_VALUE, ser);
        serdeT((short)-1, ser);
        serdeT((short)0, ser);
        serdeT((short)1, ser);
        serdeT(Short.MAX_VALUE, ser);
    }

    // ##### Tests for readUnsignedShort()

    @Test
    public void testEOFOnReadUnsignedShort() {
        serdeNull(new ObjectDataSerializer<Short>() {
            @Override
            public void serialize(final Short obj, final DataOutput output) throws IOException { // NOSONAR
            }

            @Override
            public Short deserialize(final DataInput input) throws IOException {
                try {
                    input.readUnsignedShort();
                    failOnUnthrownEOFException();
                } catch (EOFException e) { // NOSONAR
                }
                return -1;
            }
        });
    }

    @Test
    public void testReadUnsignedShort() {
        final ObjectDataSerializer<Integer> ser = new ObjectDataSerializer<Integer>() {
            @Override
            public void serialize(final Integer obj, final DataOutput output) throws IOException {
                output.writeShort(obj.shortValue());
            }

            @Override
            public Integer deserialize(final DataInput input) throws IOException {
                return Integer.valueOf(input.readUnsignedShort());
            }
        };

        serdeT(Integer.valueOf(0), ser);
        serdeT(Integer.valueOf(Short.MAX_VALUE), ser);
        serdeT(Integer.valueOf((1 << 16) - 1), ser);
    }

    // ##### Tests for writeChar(int) and readChar()

    @Test
    public void testEOFOnReadChar() {
        serdeNull(new ObjectDataSerializer<Character>() {
            @Override
            public void serialize(final Character obj, final DataOutput output) throws IOException { // NOSONAR
            }

            @Override
            public Character deserialize(final DataInput input) throws IOException {
                try {
                    input.readChar();
                    failOnUnthrownEOFException();
                } catch (EOFException e) { // NOSONAR
                }
                return (char)-1;
            }
        });
    }

    @Test
    public void testWriteCharReadChar() {
        final ObjectDataSerializer<Character> ser = new ObjectDataSerializer<Character>() {
            @Override
            public void serialize(final Character obj, final DataOutput output) throws IOException {
                output.writeChar(obj.charValue());
            }

            @Override
            public Character deserialize(final DataInput input) throws IOException {
                return Character.valueOf(input.readChar());
            }
        };

        serdeT((char)Short.MIN_VALUE, ser);
        serdeT((char)0, ser);
        serdeT((char)Short.MAX_VALUE, ser);
    }

    // ##### Tests for writeInt(int) and readInt()

    @Test
    public void testEOFOnReadInt() {
        serdeNull(new ObjectDataSerializer<Integer>() {
            @Override
            public void serialize(final Integer obj, final DataOutput output) throws IOException { // NOSONAR
            }

            @Override
            public Integer deserialize(final DataInput input) throws IOException {
                try {
                    input.readInt();
                    failOnUnthrownEOFException();
                } catch (EOFException e) { // NOSONAR
                }
                return -1;
            }
        });
    }

    @Test
    public void testWriteIntReadInt() {
        final ObjectDataSerializer<Integer> ser = new ObjectDataSerializer<Integer>() {
            @Override
            public void serialize(final Integer obj, final DataOutput output) throws IOException {
                output.writeInt(obj.intValue());
            }

            @Override
            public Integer deserialize(final DataInput input) throws IOException {
                return Integer.valueOf(input.readInt());
            }
        };

        serdeT(Integer.MIN_VALUE, ser);
        serdeT(Integer.valueOf(-1), ser);
        serdeT(Integer.valueOf(0), ser);
        serdeT(Integer.valueOf(1), ser);
        serdeT(Integer.MAX_VALUE, ser);
    }

    // ##### Tests for writeLong(long) and readLong()

    @Test
    public void testEOFOnReadLong() {
        serdeNull(new ObjectDataSerializer<Long>() {
            @Override
            public void serialize(final Long obj, final DataOutput output) throws IOException { // NOSONAR
            }

            @Override
            public Long deserialize(final DataInput input) throws IOException {
                try {
                    input.readLong();
                    failOnUnthrownEOFException();
                } catch (EOFException e) { // NOSONAR
                }
                return -1L;
            }
        });
    }

    @Test
    public void testWriteLongReadLong() {
        final ObjectDataSerializer<Long> ser = new ObjectDataSerializer<Long>() {
            @Override
            public void serialize(final Long obj, final DataOutput output) throws IOException {
                output.writeLong(obj.longValue());
            }

            @Override
            public Long deserialize(final DataInput input) throws IOException {
                return Long.valueOf(input.readLong());
            }
        };

        serdeT(Long.MIN_VALUE, ser);
        serdeT(Long.valueOf(-1L), ser);
        serdeT(Long.valueOf(0L), ser);
        serdeT(Long.valueOf(1L), ser);
        serdeT(Long.MAX_VALUE, ser);
    }

    // ##### Tests for writeFloat(float) and readFloat()

    @Test
    public void testEOFOnReadFloat() {
        serdeNull(new ObjectDataSerializer<Float>() {
            @Override
            public void serialize(final Float obj, final DataOutput output) throws IOException { // NOSONAR
            }

            @Override
            public Float deserialize(final DataInput input) throws IOException {
                try {
                    input.readFloat();
                    failOnUnthrownEOFException();
                } catch (EOFException e) { // NOSONAR
                }
                return -1F;
            }
        });
    }

    @Test
    public void testWriteFloatReadFloat() {
        final ObjectDataSerializer<Float> ser = new ObjectDataSerializer<Float>() {
            @Override
            public void serialize(final Float obj, final DataOutput output) throws IOException {
                output.writeFloat(obj.floatValue());
            }

            @Override
            public Float deserialize(final DataInput input) throws IOException {
                return Float.valueOf(input.readFloat());
            }
        };

        serdeT(Float.MIN_VALUE, ser);
        serdeT(Float.valueOf(0F), ser);
        serdeT(Float.MAX_VALUE, ser);
    }

    // ##### Tests for writeDouble(double) and readDouble()

    @Test
    public void testEOFOnReadDouble() {
        serdeNull(new ObjectDataSerializer<Double>() {
            @Override
            public void serialize(final Double obj, final DataOutput output) throws IOException { // NOSONAR
            }

            @Override
            public Double deserialize(final DataInput input) throws IOException {
                try {
                    input.readDouble();
                    failOnUnthrownEOFException();
                } catch (EOFException e) { // NOSONAR
                }
                return -1D;
            }
        });
    }

    @Test
    public void testWriteDoubleReadDouble() {
        final ObjectDataSerializer<Double> ser = new ObjectDataSerializer<Double>() {
            @Override
            public void serialize(final Double obj, final DataOutput output) throws IOException {
                output.writeDouble(obj.doubleValue());
            }

            @Override
            public Double deserialize(final DataInput input) throws IOException {
                return Double.valueOf(input.readDouble());
            }
        };

        serdeT(Double.MIN_VALUE, ser);
        serdeT(Double.valueOf(0D), ser);
        serdeT(Double.MAX_VALUE, ser);
    }

    // ##### Tests for writeBytes(String)

    @Test(expected = NullPointerException.class)
    public void testNPEOnWriteBytes() {
        serdeNull(new ObjectDataSerializer<String>() {
            @Override
            public void serialize(final String obj, final DataOutput output) throws IOException {
                output.writeBytes(null);
            }

            @Override
            public String deserialize(final DataInput input) throws IOException {
                return null;
            }
        });
    }

    @Test
    public void testWriteBytes() {
        final ObjectDataSerializer<String> ser = new ObjectDataSerializer<String>() {
            @Override
            public void serialize(final String obj, final DataOutput output) throws IOException {
                output.writeInt(obj.length());
                output.writeBytes(obj);
            }

            @Override
            public String deserialize(final DataInput input) throws IOException {
                final byte[] b = new byte[input.readInt()];
                input.readFully(b);
                final char[] c = new char[b.length];
                for (int i = 0; i < b.length; i++) {
                    c[i] = (char)(b[i] & 0xFF);
                }
                return String.valueOf(c);
            }
        };

        serdeString("foo", "foo", ser);
        serdeString(String.valueOf(new char[]{'\u0000', '\u00ff', '\uffff', '\uff00'}),
            String.valueOf(new char[]{'\u0000', '\u00ff', '\u00ff', '\u0000'}), ser);
    }

    // ##### Tests for writeChars(String) and readLine()

    @Test(expected = NullPointerException.class)
    public void testNPEOnWriteChars() {
        serdeNull(new ObjectDataSerializer<String>() {
            @Override
            public void serialize(final String obj, final DataOutput output) throws IOException {
                output.writeChars(null);
            }

            @Override
            public String deserialize(final DataInput input) throws IOException {
                return null;
            }
        });
    }

    @Test
    public void testWriteCharsReadLine() {
        final ObjectDataSerializer<String> ser = new ObjectDataSerializer<String>() {
            @Override
            public void serialize(final String obj, final DataOutput output) throws IOException {
                output.writeChars(obj);
            }

            @Override
            public String deserialize(final DataInput input) throws IOException {
                return input.readLine();
            }
        };

        serdeString("", null, ser); // NOSONAR
        serdeString("foo", "foo", ser); // NOSONAR
        serdeString("foo\rbar", "foobar", ser);
        serdeString("foo\r\nbar", "foo", ser);
        serdeString("foo\nbar", "foo", ser);
    }

    // ##### Tests for writeUTF(String) and readUTF()

    @Test(expected = NullPointerException.class)
    public void testNPEOnWriteUTF() {
        serdeNull(new ObjectDataSerializer<String>() {
            @Override
            public void serialize(final String obj, final DataOutput output) throws IOException {
                output.writeUTF(null);
            }

            @Override
            public String deserialize(final DataInput input) throws IOException {
                return null;
            }
        });
    }

    @Test
    public void testEOFOnReadUTF() {
        serdeNull(new ObjectDataSerializer<byte[]>() {
            @Override
            public void serialize(final byte[] obj, final DataOutput output) throws IOException { // NOSONAR
                output.writeInt(1);
            }

            @Override
            public byte[] deserialize(final DataInput input) throws IOException {
                try {
                    input.readUTF();
                    failOnUnthrownEOFException();
                } catch (EOFException e) { // NOSONAR
                }
                return new byte[0];
            }
        });
    }

    @Test
    public void testWriteUTFReadUTF() {
        final ObjectDataSerializer<String> ser = new ObjectDataSerializer<String>() {
            @Override
            public void serialize(final String obj, final DataOutput output) throws IOException {
                output.writeUTF(obj);
            }

            @Override
            public String deserialize(final DataInput input) throws IOException {
                return input.readUTF();
            }
        };

        serdeT("", ser);
        serdeT(String.valueOf(Character.toChars(0x0000)), ser);
        serdeT(String.valueOf(Character.toChars(0x0001)), ser);
        serdeT(String.valueOf(Character.toChars(0x007F)), ser);
        serdeT(String.valueOf(Character.toChars(0x0080)), ser);
        serdeT(String.valueOf(Character.toChars(0x07FF)), ser);
        serdeT(String.valueOf(Character.toChars(0x0800)), ser);
        serdeT(String.valueOf(Character.toChars(0xFFFF)), ser);
        serdeT(String.valueOf(Character.toChars(0x10000)), ser);
        serdeT(String.valueOf(Character.toChars(0x10FFFF)), ser);
        serdeT(String.valueOf(new char[65535]), ser);
        serdeT(String.valueOf(new char[65536]), ser);
    }

    // ##### Tests for serialize(int, Object) and deserialize(int)

    @Test
    public void testSerializeDeserialize() {
        try (final LargeVarBinaryVector vector = new LargeVarBinaryVector("test", m_alloc)) {
            vector.allocateNew(0, 0);
            final ArrowBufIO<Integer> bio = new ArrowBufIO<>(vector, new ObjectDataSerializer<Integer>() {
                @Override
                public void serialize(final Integer obj, final DataOutput output) throws IOException {
                    output.writeInt(obj.intValue());
                }

                @Override
                public Integer deserialize(final DataInput input) throws IOException {
                    return Integer.valueOf(input.readInt());
                }
            });

            bio.serialize(1, 1);
            bio.serialize(3, 3);

            assertTrue(vector.isNull(0));
            assertFalse(vector.isNull(1));
            assertTrue(vector.isNull(2));
            assertFalse(vector.isNull(3));
            assertTrue(vector.isNull(4));

            assertEquals(Integer.valueOf(1), bio.deserialize(1));
            assertEquals(Integer.valueOf(3), bio.deserialize(3));
        }
    }

}
