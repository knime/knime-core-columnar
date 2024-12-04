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
package org.knime.core.columnar.arrow.data;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.knime.core.table.io.ReadableDataInput;

/**
 * Custom DataInput that reads directly from a byte array without copying.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
class ByteArrayDataInput implements ReadableDataInput {
    private final byte[] m_data;

    private final int m_start;

    private final int m_end;

    private int m_position;

    public ByteArrayDataInput(final byte[] data, final int start, final int end) {
        m_data = data;
        m_start = start;
        m_end = end;
        m_position = start;
    }

    @Override
    public void readFully(final byte[] b) throws IOException {
        int len = b.length;
        if (m_position + len > m_end) {
            throw new EOFException();
        }
        System.arraycopy(m_data, m_position, b, 0, len);
        m_position += len;
    }

    @Override
    public void readFully(final byte[] b, final int off, final int len) throws IOException {
        if (m_position + len > m_end) {
            throw new EOFException();
        }
        System.arraycopy(m_data, m_position, b, off, len);
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
        int available = m_end - m_position;
        int toRead = Math.min(len, available);
        System.arraycopy(m_data, m_position, b, off, toRead);
        m_position += toRead;
        return toRead;
    }

    @Override
    public byte[] readBytes() throws IOException {
        int remaining = m_end - m_position;
        if (remaining < 0) {
            throw new EOFException();
        }
        byte[] result = new byte[remaining];
        System.arraycopy(m_data, m_position, result, 0, remaining);
        m_position = m_end;
        return result;
    }

    @Override
    public int skipBytes(final int n) throws IOException {
        int k = Math.min(n, m_end - m_position);
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
        return m_data[m_position++];
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return readByte() & 0xFF;
    }

    @Override
    public short readShort() throws IOException {
        int ch1 = readUnsignedByte();
        int ch2 = readUnsignedByte();
        return (short)((ch1 << 8) + ch2);
    }

    @Override
    public int readUnsignedShort() throws IOException {
        int ch1 = readUnsignedByte();
        int ch2 = readUnsignedByte();
        return (ch1 << 8) + ch2;
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
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4);
    }

    @Override
    public long readLong() throws IOException {
        long ch1 = ((long)readUnsignedByte()) << 56;
        long ch2 = ((long)readUnsignedByte()) << 48;
        long ch3 = ((long)readUnsignedByte()) << 40;
        long ch4 = ((long)readUnsignedByte()) << 32;
        long ch5 = ((long)readUnsignedByte()) << 24;
        long ch6 = ((long)readUnsignedByte()) << 16;
        long ch7 = ((long)readUnsignedByte()) << 8;
        long ch8 = (readUnsignedByte());
        return ch1 + ch2 + ch3 + ch4 + ch5 + ch6 + ch7 + ch8;
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
        throw new UnsupportedOperationException("readLine not supported");
    }

    @Override
    public String readUTF() throws IOException {
        int utflen = readUnsignedShort();
        byte[] bytearr = new byte[utflen];
        readFully(bytearr, 0, utflen);
        return new String(bytearr, StandardCharsets.UTF_8);
    }
}