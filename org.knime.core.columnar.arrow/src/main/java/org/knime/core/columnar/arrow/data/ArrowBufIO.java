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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.knime.core.columnar.data.ObjectData.ObjectDataSerializer;

/**
 * Serializes and deserializes an object of type <T> to a {@link DataOutput} / {@link DataInput}. The {@link DataOutput}
 * / {@link DataInput} are directly writing into the value buffer of the {@link LargeVarBinaryVector} with zero copy.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
final class ArrowBufIO<T> {

    private static ThreadLocal<StringEncoder> ENCODER_FACTORY = ThreadLocal.withInitial(StringEncoder::new);

    private static final int OFFSET_WIDTH = BaseLargeVariableWidthVector.OFFSET_WIDTH;

    private final ObjectDataSerializer<T> m_serializer;

    private final LargeVarBinaryVector m_vector;

    ArrowBufIO(final LargeVarBinaryVector vector, final ObjectDataSerializer<T> serializer) {
        m_serializer = serializer;
        m_vector = vector;
    }

    @SuppressWarnings("resource") // Buffers closed by the vector
    final T deserialize(final int index) {
        try {
            // Get the offset for this index
            final long bufferIndex = getOffset(m_vector, index);

            // Deserialize from the data buffer
            final ArrowBufDataInput buf =
                new ArrowBufDataInput(m_vector.getDataBuffer(), bufferIndex, ENCODER_FACTORY.get());
            return m_serializer.deserialize(buf);
        } catch (final IOException ex) {
            // TODO should the deserialize method just throw the IOException?
            throw new IllegalStateException("Error during deserialization", ex);
        }
    }

    @SuppressWarnings("resource") // Buffers closed by the vector
    final void serialize(final int index, final T obj) {
        try {
            // Set the value to not null
            BitVectorHelper.setBit(m_vector.getValidityBuffer(), index);

            // Get the start of the the new value
            m_vector.fillEmpties(index);
            final long startOffset = getOffset(m_vector, index);
            m_vector.getDataBuffer().writerIndex(startOffset);

            // Serialize the value to the data buffer
            m_serializer.serialize(obj, new ArrowBufDataOutput(m_vector, ENCODER_FACTORY.get()));

            // Set the offset for the next element
            final long nextOffset = m_vector.getDataBuffer().writerIndex();
            setOffset(m_vector, index + 1, nextOffset);

            // Update the lastSet value of the vector
            m_vector.setLastSet(index);
        } catch (final IOException ex) {
            // TODO should the serialize method just throw the IOException?
            throw new IllegalStateException("Error during serialization", ex);
        }
    }

    @SuppressWarnings("resource")
    private static long getOffset(final LargeVarBinaryVector vector, final int index) {
        return vector.getOffsetBuffer().getLong((long)index * OFFSET_WIDTH);
    }

    @SuppressWarnings("resource")
    private static void setOffset(final LargeVarBinaryVector vector, final int index, final long offset) {
        vector.getOffsetBuffer().setLong((long)index * OFFSET_WIDTH, offset);
    }

    private static final class ArrowBufDataInput implements DataInput {

        private final ArrowBuf m_buffer;

        private final StringEncoder m_encoder;

        private long m_bufferIndex;

        public ArrowBufDataInput(final ArrowBuf buffer, final long index, final StringEncoder encoder) {
            m_buffer = buffer;
            m_bufferIndex = index;
            m_encoder = encoder;
        }

        @Override
        public void readFully(final byte[] b) throws IOException {
            m_buffer.getBytes(m_bufferIndex, b);
            m_bufferIndex += b.length;
        }

        @Override
        public void readFully(final byte[] b, final int off, final int len) throws IOException {
            m_buffer.getBytes(m_bufferIndex, b, off, len);
            m_bufferIndex += len;
        }

        @Override
        public int skipBytes(final int n) throws IOException {
            m_bufferIndex += n;
            return n;

        }

        @Override
        public boolean readBoolean() throws IOException {
            final byte out = m_buffer.getByte(m_bufferIndex);
            m_bufferIndex += Byte.BYTES;
            return out != 0;
        }

        @Override
        public byte readByte() throws IOException {
            final byte out = m_buffer.getByte(m_bufferIndex);
            m_bufferIndex += Byte.BYTES;
            return out;
        }

        @Override
        public int readUnsignedByte() throws IOException {
            // TODO this implementation is wrong! Make unsigned
            final byte out = m_buffer.getByte(m_bufferIndex);
            m_bufferIndex += Byte.BYTES;
            return out;

        }

        @Override
        public short readShort() throws IOException {
            final short out = m_buffer.getShort(m_bufferIndex);
            m_bufferIndex += Short.BYTES;
            return out;
        }

        @Override
        public int readUnsignedShort() throws IOException {
            // TODO this implementation is wrong! Make unsigned
            final short out = m_buffer.getShort(m_bufferIndex);
            m_bufferIndex += Short.BYTES;
            return out;
        }

        @Override
        public char readChar() throws IOException {
            final char out = m_buffer.getChar(m_bufferIndex);
            m_bufferIndex += Byte.BYTES;
            return out;
        }

        @Override
        public int readInt() throws IOException {
            final int out = m_buffer.getInt(m_bufferIndex);
            m_bufferIndex += Integer.BYTES;
            return out;
        }

        @Override
        public long readLong() throws IOException {
            final long out = m_buffer.getLong(m_bufferIndex);
            m_bufferIndex += Long.BYTES;
            return out;
        }

        @Override
        public float readFloat() throws IOException {
            final float out = m_buffer.getFloat(m_bufferIndex);
            m_bufferIndex += Float.BYTES;
            return out;
        }

        @Override
        public double readDouble() throws IOException {
            double out = m_buffer.getDouble(m_bufferIndex);
            m_bufferIndex += Double.BYTES;
            return out;
        }

        @Override
        public String readLine() throws IOException {
            // TODO implement: Loop byte by byte and make sure not to overflow the buffer
            throw new UnsupportedOperationException("NYI.");
        }

        @Override
        public String readUTF() throws IOException {
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
        public void write(final int b) throws IOException {
            ensureCapacity(Byte.BYTES);
            m_buffer.writeByte(b);
        }

        @Override
        public void write(final byte[] b) throws IOException {
            ensureCapacity(b.length);
            m_buffer.writeBytes(b);
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            ensureCapacity(len);
            m_buffer.writeBytes(b, off, len);
        }

        @Override
        public void writeBoolean(final boolean v) throws IOException {
            ensureCapacity(Byte.BYTES);
            m_buffer.writeByte(v ? 1 : 0);
        }

        @Override
        public void writeByte(final int v) throws IOException {
            ensureCapacity(Byte.BYTES);
            m_buffer.writeByte(v);
        }

        @Override
        public void writeShort(final int v) throws IOException {
            ensureCapacity(Short.BYTES);
            m_buffer.writeShort(v);
        }

        @Override
        public void writeChar(final int v) throws IOException {
            ensureCapacity(2 * Byte.BYTES);
            m_buffer.writeByte((v >>> 8) & 0xFF);
            m_buffer.writeByte((v >>> 0) & 0xFF);
        }

        @Override
        public void writeInt(final int v) throws IOException {
            ensureCapacity(Integer.BYTES);
            m_buffer.writeInt(v);
        }

        @Override
        public void writeLong(final long v) throws IOException {
            ensureCapacity(Long.BYTES);
            m_buffer.writeLong(v);
        }

        @Override
        public void writeFloat(final float v) throws IOException {
            ensureCapacity(Float.BYTES);
            m_buffer.writeFloat(v);
        }

        @Override
        public void writeDouble(final double v) throws IOException {
            ensureCapacity(Double.BYTES);
            m_buffer.writeDouble(v);
        }

        @Override
        public void writeBytes(final String s) throws IOException {
            int len = s.length();
            ensureCapacity(len * Byte.BYTES);
            for (int i = 0; i < len; i++) {
                m_buffer.writeByte((byte)s.charAt(i));
            }
        }

        @Override
        public void writeChars(final String s) throws IOException {
            final int len = s.length();
            ensureCapacity(len * Byte.BYTES * 2);
            for (int i = 0; i < len; i++) {
                int v = s.charAt(i);
                m_buffer.writeByte((v >>> 8) & 0xFF);
                m_buffer.writeByte((v >>> 0) & 0xFF);
            }
        }

        @Override
        public void writeUTF(final String s) throws IOException {
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

    /**
     * A helper class for data implementations that need to encode and decode Strings.
     *
     * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
     * @since 4.3
     */
    private static class StringEncoder {

        private final CharsetDecoder m_decoder = StandardCharsets.UTF_8.newDecoder()
            .onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);

        private final CharsetEncoder m_encoder = StandardCharsets.UTF_8.newEncoder()
            .onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);

        final String decode(final ByteBuffer buffer) {
            try {
                synchronized (m_decoder) {
                    return m_decoder.decode(buffer).toString();
                }
            } catch (final CharacterCodingException e) {
                // This cannot happen because the CodingErrorAction is not REPORT
                throw new IllegalStateException(e);
            }
        }

        final String decode(final byte[] bytes) {
            return decode(ByteBuffer.wrap(bytes));
        }

        ByteBuffer encode(final CharBuffer values) {
            try {
                synchronized (m_encoder) {
                    return m_encoder.encode(values);
                }
            } catch (final CharacterCodingException e) {
                // This cannot happen because the CodingErrorAction is not REPORT
                throw new IllegalStateException(e);
            }
        }

        ByteBuffer encode(final String value) {
            return encode(CharBuffer.wrap(value));
        }
    }
}
