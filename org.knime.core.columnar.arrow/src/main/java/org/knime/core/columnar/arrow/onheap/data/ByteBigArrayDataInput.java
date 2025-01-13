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
 *   Dec 4, 2024 (benjamin): created
 */
package org.knime.core.columnar.arrow.onheap.data;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.knime.core.table.io.ReadableDataInput;

import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.bytes.ByteBigArrays;

/**
 * Custom DataInput that reads directly from a {@link ByteBigArrays byte big array}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
class ByteBigArrayDataInput implements ReadableDataInput {
    private final byte[][] m_data;

    private final long m_end;

    private long m_position;

    public ByteBigArrayDataInput(final byte[][] data, final long start, final long end) {
        m_data = data;
        m_end = end;
        m_position = start;
    }

    @Override
    public void readFully(final byte[] b) throws IOException {
        int len = b.length;
        if (m_position + len > m_end) {
            throw new EOFException();
        }
        BigArrays.copyFromBig(m_data, m_position, b, 0, len);
        m_position += len;
    }

    @Override
    public void readFully(final byte[] b, final int off, final int len) throws IOException {
        if (off + len > b.length) {
            throw new IndexOutOfBoundsException();
        }
        if (m_position + len > m_end) {
            throw new EOFException();
        }
        BigArrays.copyFromBig(m_data, m_position, b, off, len);
        m_position += len;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (b == null) {
            throw new NullPointerException("Byte array is null");
        }
        if (off < 0 || len < 0 || off + len > b.length) {
            throw new IndexOutOfBoundsException("Offset and length out of bounds");
        }
        if (len == 0) {
            return 0;
        }
        if (m_position >= m_end) {
            return -1; // End of stream reached
        }
        long available = m_end - m_position;
        int toRead = (int)Math.min(len, available); // len is an int, so this is safe
        BigArrays.copyFromBig(m_data, m_position, b, off, toRead);
        m_position += toRead;
        return toRead;
    }

    @Override
    public byte[] readBytes() throws IOException {
        long remaining = m_end - m_position;
        if (remaining > Integer.MAX_VALUE) {
            throw new IndexOutOfBoundsException("Data too large to read into byte array");
        }
        if (remaining < 0) {
            throw new EOFException();
        }
        byte[] result = new byte[(int)remaining];
        BigArrays.copyFromBig(m_data, m_position, result, 0, (int)remaining);
        m_position = m_end;
        return result;
    }

    @Override
    public int skipBytes(final int n) throws IOException {
        int k = (int)Math.min(n, m_end - m_position);
        m_position += k;
        return k;
    }

    @Override
    public boolean readBoolean() throws IOException {
        return readByte() != 0;
    }

    @Override
    public byte readByte() throws IOException {
        if (m_position >= m_end) {
            throw new EOFException();
        }
        return BigArrays.get(m_data, m_position++);
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return readByte() & 0xFF;
    }

    @Override
    public short readShort() throws IOException {
        int ch1 = readUnsignedByte();
        int ch2 = readUnsignedByte();
        return (short)((ch2 << 8) + ch1);
    }

    @Override
    public int readUnsignedShort() throws IOException {
        int ch1 = readUnsignedByte();
        int ch2 = readUnsignedByte();
        return (ch2 << 8) + ch1;
    }

    @Override
    public char readChar() throws IOException {
        return (char)readShort();
    }

    @Override
    public int readInt() throws IOException {
        int ch1 = readUnsignedByte();
        int ch2 = readUnsignedByte();
        int ch3 = readUnsignedByte();
        int ch4 = readUnsignedByte();
        return ((ch4 << 24) + (ch3 << 16) + (ch2 << 8) + ch1);
    }

    @Override
    public long readLong() throws IOException {
        long ch1 = (readUnsignedByte());
        long ch2 = ((long)readUnsignedByte()) << 8;
        long ch3 = ((long)readUnsignedByte()) << 16;
        long ch4 = ((long)readUnsignedByte()) << 24;
        long ch5 = ((long)readUnsignedByte()) << 32;
        long ch6 = ((long)readUnsignedByte()) << 40;
        long ch7 = ((long)readUnsignedByte()) << 48;
        long ch8 = ((long)readUnsignedByte()) << 56;
        return ch8 + ch7 + ch6 + ch5 + ch4 + ch3 + ch2 + ch1;
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public String readLine() throws IOException {
        final long maxIndex = m_end - Byte.BYTES;
        if (m_position > maxIndex) {
            return null;
        }
        final StringBuilder sb = new StringBuilder();
        do { // NOSONAR
            final char c = (char)(BigArrays.get(m_data, m_position) & 0xFF);
            m_position++;
            if (c == '\r') {
                continue;
            }
            if (c == '\n') {
                break;
            }
            sb.append(c);
        } while (m_position <= maxIndex);
        return sb.toString();
    }

    /**
     * Reads in a string that has been encoded using UTF-8 format. The general contract of {@code readUTF} is that it
     * reads a representation of a Unicode character string encoded in UTF-8 format; this string of characters is then
     * returned as a String.
     * <p>
     *
     * First, four bytes are read and used to construct a 32-bit integer in exactly the manner of the {@code readInt}
     * method. This integer value is called the <i>UTF length</i> and specifies the number of additional bytes to be
     * read. These bytes are then converted to a string via the {@link Charset#decode(ByteBuffer) decode} method of
     * {@link StandardCharsets#UTF_8}.
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
     * {@code ByteBigListDataOutput#writeUTF(String)} may be used to write data that is suitable for reading by this
     * method.
     *
     * @return a Unicode string.
     * @exception EOFException if this stream reaches the end before reading all the bytes.
     */
    @Override
    public String readUTF() throws IOException {
        // TODO(AP-23858) Is CharsetDecoder faster?
        // TODO(AP-23858) Can decode directly from the BigArray without a temporary byte[]?
        final byte[] out = new byte[readInt()];
        readFully(out);
        return new String(out, StandardCharsets.UTF_8);
    }
}