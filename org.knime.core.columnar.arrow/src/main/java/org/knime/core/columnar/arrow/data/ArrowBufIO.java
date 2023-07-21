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
 *   Oct 17, 2020 (dietzc): created
 */
package org.knime.core.columnar.arrow.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.knime.core.table.io.ReadableDataInput;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;
import org.knime.core.table.util.StringEncoder;

/**
 * Serializes and deserializes an object of type <T> to a {@link DataOutput} / {@link DataInput}. The {@link DataOutput}
 * / {@link DataInput} are directly writing into the value buffer of the {@link LargeVarBinaryVector} with zero copy.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
final class ArrowBufIO {

    private static ThreadLocal<StringEncoder> ENCODER_FACTORY = ThreadLocal.withInitial(StringEncoder::new);

    private static final int OFFSET_WIDTH = BaseLargeVariableWidthVector.OFFSET_WIDTH;

    /**
     * Looks for an object at the given index and deserializes it. The contract is that this method is never invoked for
     * an index that is larger than the largest index among serialized objects.
     *
     * @param index the index at which to look for the to-be-deserialized object
     * @return the deserialized object
     */
    @SuppressWarnings("resource") // Buffers closed by the vector
    static final <T> T deserialize(final int index, final LargeVarBinaryVector vector,
        final ObjectDeserializer<T> deserializer) {
        // Get the offset for this index
        final long bufferIndex = getOffset(vector, index);
        final long nextIndex = getOffset(vector, index + 1);

        // Deserialize from the data buffer
        final ArrowBufDataInput buf =
            new ArrowBufDataInput(vector.getDataBuffer(), bufferIndex, nextIndex, ENCODER_FACTORY.get());
        try {
            return deserializer.deserialize(buf);
        } catch (IOException ex) {
            // TODO should the deserialize method just throw the IOException?
            throw new IllegalStateException("Error during deserialization", ex);
        }
    }

    /**
     * Serializes an object and places it at a given index. The contract is that this method is only ever invoked for
     * ascending indices. Indices can be skipped entirely however.
     *
     * @param index the index at which to place the serialized object
     * @param obj the object to serialize
     */
    @SuppressWarnings("resource") // Buffers closed by the vector
    static final <T> void serialize(final int index, final T obj, final LargeVarBinaryVector vector,
        final ObjectSerializer<T> serializer) {
        // Set the value to not null
        BitVectorHelper.setBit(vector.getValidityBuffer(), index);

        // Get the start of the the new value
        vector.fillEmpties(index);
        final long startOffset = getOffset(vector, index);
        vector.getDataBuffer().writerIndex(startOffset);

        // Serialize the value to the data buffer
        try {
            serializer.serialize(new ArrowBufDataOutput(vector, ENCODER_FACTORY.get()), obj);
        } catch (IOException ex) {
            // TODO should the serialize method just throw the IOException?
            throw new IllegalStateException("Error during serialization", ex);
        }

        // Set the offset for the next element
        final long nextOffset = vector.getDataBuffer().writerIndex();
        setOffset(vector, index + 1, nextOffset);

        // Update the lastSet value of the vector
        vector.setLastSet(index);
    }

    @SuppressWarnings("resource")
    private static long getOffset(final LargeVarBinaryVector vector, final int index) {
        return vector.getOffsetBuffer().getLong((long)index * OFFSET_WIDTH);
    }

    @SuppressWarnings("resource")
    private static void setOffset(final LargeVarBinaryVector vector, final int index, final long offset) {
        vector.getOffsetBuffer().setLong((long)index * OFFSET_WIDTH, offset);
    }

    private static final class ArrowBufDataInput implements ReadableDataInput {

        private final ArrowBuf m_buffer;

        private final StringEncoder m_encoder;

        private final long m_nextBufferIndex;

        private long m_bufferIndex;

        public ArrowBufDataInput(final ArrowBuf buffer, final long index, final long nextIndex,
            final StringEncoder encoder) {
            m_buffer = buffer;
            m_bufferIndex = index;
            m_nextBufferIndex = nextIndex;
            m_encoder = encoder;
        }

        @Override
        public void readFully(final byte[] b) throws EOFException {
            if (m_bufferIndex + b.length > m_nextBufferIndex) {
                throw new EOFException();
            }
            m_buffer.getBytes(m_bufferIndex, b);
            m_bufferIndex += b.length;
        }

        @Override
        public void readFully(final byte[] b, final int off, final int len) throws EOFException {
            if (off + len > b.length) {
                throw new IndexOutOfBoundsException();
            }
            if (m_bufferIndex + len > m_nextBufferIndex) {
                throw new EOFException();
            }
            m_buffer.getBytes(m_bufferIndex, b, off, len);
            m_bufferIndex += len;
        }

        @Override
        public byte[] readBytes() {
            if (m_bufferIndex == m_nextBufferIndex) {
                return new byte[0];
            }

            long numBytesAvailable = m_nextBufferIndex - m_bufferIndex;
            if (numBytesAvailable > Integer.MAX_VALUE) {
                throw new IndexOutOfBoundsException();
            }
            byte[] buffer = new byte[(int)numBytesAvailable];
            m_buffer.getBytes(m_bufferIndex, buffer);
            m_bufferIndex += numBytesAvailable;
            return buffer;
        }

        @Override
        public int read(final byte[] b, final int off, final int len) {
            if (off + len > b.length) {
                throw new IndexOutOfBoundsException();
            }
            if (len == 0) {
                return 0;
            }
            if (m_bufferIndex >= m_nextBufferIndex) {
                return -1;
            }
            int actualLen = m_bufferIndex + len > m_nextBufferIndex ? ((int)(m_nextBufferIndex - m_bufferIndex)) : len;
            m_buffer.getBytes(m_bufferIndex, b, off, actualLen);
            m_bufferIndex += actualLen;
            return actualLen;
        }

        @Override
        public int skipBytes(final int n) {
            final int skip = Math.min(n, (int)(m_nextBufferIndex - m_bufferIndex));
            m_bufferIndex += skip;
            return skip;
        }

        @Override
        public boolean readBoolean() throws EOFException {
            if (m_bufferIndex + Byte.BYTES > m_nextBufferIndex) {
                throw new EOFException();
            }
            final byte out = m_buffer.getByte(m_bufferIndex);
            m_bufferIndex += Byte.BYTES;
            return out != 0;
        }

        @Override
        public byte readByte() throws EOFException {
            if (m_bufferIndex + Byte.BYTES > m_nextBufferIndex) {
                throw new EOFException();
            }
            final byte out = m_buffer.getByte(m_bufferIndex);
            m_bufferIndex += Byte.BYTES;
            return out;
        }

        @Override
        public int readUnsignedByte() throws EOFException {
            if (m_bufferIndex + Byte.BYTES > m_nextBufferIndex) {
                throw new EOFException();
            }
            final byte signed = m_buffer.getByte(m_bufferIndex);
            m_bufferIndex += Byte.BYTES;
            return signed & 0xFF;

        }

        @Override
        public short readShort() throws EOFException {
            if (m_bufferIndex + Short.BYTES > m_nextBufferIndex) {
                throw new EOFException();
            }
            final short out = m_buffer.getShort(m_bufferIndex);
            m_bufferIndex += Short.BYTES;
            return out;
        }

        @Override
        public int readUnsignedShort() throws EOFException {
            if (m_bufferIndex + Short.BYTES > m_nextBufferIndex) {
                throw new EOFException();
            }
            final short signed = m_buffer.getShort(m_bufferIndex);
            m_bufferIndex += Short.BYTES;
            return signed & 0xFFFF;
        }

        @Override
        public char readChar() throws EOFException {
            if (m_bufferIndex + Character.BYTES > m_nextBufferIndex) {
                throw new EOFException();
            }
            final char out = m_buffer.getChar(m_bufferIndex);
            m_bufferIndex += Character.BYTES;
            return out;
        }

        @Override
        public int readInt() throws EOFException {
            if (m_bufferIndex + Integer.BYTES > m_nextBufferIndex) {
                throw new EOFException();
            }
            final int out = m_buffer.getInt(m_bufferIndex);
            m_bufferIndex += Integer.BYTES;
            return out;
        }

        @Override
        public long readLong() throws EOFException {
            if (m_bufferIndex + Long.BYTES > m_nextBufferIndex) {
                throw new EOFException();
            }
            final long out = m_buffer.getLong(m_bufferIndex);
            m_bufferIndex += Long.BYTES;
            return out;
        }

        @Override
        public float readFloat() throws EOFException {
            if (m_bufferIndex + Float.BYTES > m_nextBufferIndex) {
                throw new EOFException();
            }
            final float out = m_buffer.getFloat(m_bufferIndex);
            m_bufferIndex += Float.BYTES;
            return out;
        }

        @Override
        public double readDouble() throws EOFException {
            if (m_bufferIndex + Double.BYTES > m_nextBufferIndex) {
                throw new EOFException();
            }
            double out = m_buffer.getDouble(m_bufferIndex);
            m_bufferIndex += Double.BYTES;
            return out;
        }

        @Override
        public String readLine() {
            final long maxIndex = m_nextBufferIndex - Byte.BYTES;
            if (m_bufferIndex > maxIndex) {
                return null;
            }
            final StringBuilder sb = new StringBuilder();
            do { // NOSONAR
                final char c = (char)(m_buffer.getByte(m_bufferIndex) & 0xFF);
                m_bufferIndex += Byte.BYTES;
                if (c == '\r') {
                    continue;
                }
                if (c == '\n') {
                    break;
                }
                sb.append(c);
            } while (m_bufferIndex <= maxIndex);
            return sb.toString();
        }

        /**
         * Reads in a string that has been encoded using UTF-8 format. The general contract of {@code readUTF} is that
         * it reads a representation of a Unicode character string encoded in UTF-8 format; this string of characters is
         * then returned as a String.
         * <p>
         *
         * First, four bytes are read and used to construct a 32-bit integer in exactly the manner of the
         * {@code readInt} method. This integer value is called the <i>UTF length</i> and specifies the number of
         * additional bytes to be read. These bytes are then converted to a string via the
         * {@link Charset#decode(ByteBuffer) decode} method of {@link StandardCharsets#UTF_8}.
         * <p>
         *
         * If end of file is encountered at any time during this entire process, then an {@code EOFException} is thrown.
         * <p>
         *
         * The implementation of this method differs from the contract of {@link DataInput#readUTF()} in two ways:
         * <ol>
         * <li>It expects to-be-decoded strings to be encoded not in modified UTF-8 format, but in standard UTF-8
         * format.</li>
         * <li>It allows reading encoded strings with a length of up to {@link Integer#MAX_VALUE} bytes.</li>
         * </ol>
         *
         * The {@code writeUTF} method of {@code ArrowBufDataOutput} may be used to write data that is suitable for
         * reading by this method.
         *
         * @return a Unicode string.
         * @exception EOFException if this stream reaches the end before reading all the bytes.
         */
        @Override
        public String readUTF() throws EOFException {
            final byte[] out = new byte[readInt()];
            readFully(out);
            return m_encoder.decode(out);
        }
    }

    private static final class ArrowBufDataOutput implements DataOutput {

        private ArrowBuf m_buffer;

        private StringEncoder m_encoder;

        private long m_capacity;

        private LargeVarBinaryVector m_vector;

        private ArrowBufDataOutput(final LargeVarBinaryVector vector, final StringEncoder encoder) {
            m_encoder = encoder;
            m_buffer = vector.getDataBuffer();
            m_vector = vector;
            m_capacity = m_buffer.capacity();
        }

        @Override
        public void write(final int b) {
            ensureCapacity(Byte.BYTES);
            m_buffer.writeByte(b);
        }

        @Override
        public void write(final byte[] b) {
            ensureCapacity(b.length * Byte.BYTES);
            m_buffer.writeBytes(b);
        }

        @Override
        public void write(final byte[] b, final int off, final int len) {
            if (off + len > b.length) {
                throw new IndexOutOfBoundsException();
            }
            ensureCapacity(len * Byte.BYTES);
            m_buffer.writeBytes(b, off, len);
        }

        @Override
        public void writeBoolean(final boolean v) {
            ensureCapacity(Byte.BYTES);
            m_buffer.writeByte(v ? 1 : 0);
        }

        @Override
        public void writeByte(final int v) {
            ensureCapacity(Byte.BYTES);
            m_buffer.writeByte(v);
        }

        @Override
        public void writeShort(final int v) {
            ensureCapacity(Short.BYTES);
            m_buffer.writeShort(v);
        }

        @Override
        public void writeChar(final int v) {
            ensureCapacity(Character.BYTES);
            m_buffer.writeShort(v);
        }

        @Override
        public void writeInt(final int v) {
            ensureCapacity(Integer.BYTES);
            m_buffer.writeInt(v);
        }

        @Override
        public void writeLong(final long v) {
            ensureCapacity(Long.BYTES);
            m_buffer.writeLong(v);
        }

        @Override
        public void writeFloat(final float v) {
            ensureCapacity(Float.BYTES);
            m_buffer.writeFloat(v);
        }

        @Override
        public void writeDouble(final double v) {
            ensureCapacity(Double.BYTES);
            m_buffer.writeDouble(v);
        }

        @Override
        public void writeBytes(final String s) {
            int len = s.length();
            ensureCapacity(len * Byte.BYTES);
            for (int i = 0; i < len; i++) {
                m_buffer.writeByte((byte)s.charAt(i));
            }
        }

        @Override
        public void writeChars(final String s) {
            final int len = s.length();
            ensureCapacity(len * Character.BYTES);
            for (int i = 0; i < s.length(); i++) {
                m_buffer.writeShort(s.charAt(i));
            }
        }

        /**
         * Writes four bytes of length information to the output stream, followed by the UTF-8 representation of every
         * character in the string <code>s</code>. If <code>s</code> is <code>null</code>, a
         * <code>NullPointerException</code> is thrown. The length of the information represents the number of bytes of
         * the encoded string and is written to the output stream in exactly the manner of the <code>writeInt</code>
         * method.
         * <p>
         *
         * The implementation of this method differs from the contract of {@link DataOutput#writeUTF(String)} in two
         * ways:
         * <ol>
         * <li>It encodes strings not in modified UTF-8 format, but in standard UTF-8 format.</li>
         * <li>It allows writing encoded strings with a length of up to {@link Integer#MAX_VALUE} bytes.</li>
         * </ol>
         *
         * The bytes written by this method may be read by the <code>readUTF</code> method of
         * <code>ArrowBufDataInput</code>, which will then return a <code>String</code> equal to <code>s</code>.
         *
         * @param s the string value to be written.
         */
        @Override
        public void writeUTF(final String s) throws UTFDataFormatException {
            final ByteBuffer encoded = m_encoder.encode(s);
            final int limit = encoded.limit();
            ensureCapacity(limit + Integer.BYTES);
            m_buffer.writeInt(limit);
            m_buffer.writeBytes(encoded.array(), 0, limit);
        }

        @SuppressWarnings("resource")
        private void ensureCapacity(final int additional) {
            while (m_buffer.writerIndex() + additional >= m_capacity) {
                final long prev = m_buffer.writerIndex();
                m_vector.reallocDataBuffer();
                m_buffer = m_vector.getDataBuffer();
                m_buffer.writerIndex(prev);
                m_capacity = m_buffer.capacity();
            }
        }
    }

    private ArrowBufIO() {
    }

}
