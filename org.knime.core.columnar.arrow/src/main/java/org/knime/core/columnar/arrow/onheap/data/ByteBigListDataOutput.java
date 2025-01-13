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

import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.bytes.ByteBigList;

/**
 * Custom DataOutput that writes directly into a {@link ByteBigList}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
class ByteBigListDataOutput implements DataOutput {
    protected ByteBigList m_data;

    protected long m_position;

    public ByteBigListDataOutput(final ByteBigList data, final long startPosition) {
        m_data = data;
        m_position = startPosition;
    }

    public long getPosition() {
        return m_position;
    }

    @Override
    public void write(final int b) throws IOException {
        m_data.add((byte)b);
        m_position++;
    }

    @Override
    public void write(final byte[] b) throws IOException {
        var bigB = BigArrays.wrap(b); // Note that this just wraps if b has less than SEGMENT_SIZE elements
        m_data.addElements(m_position, bigB);
        m_position += b.length;
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        var bigB = BigArrays.wrap(b);
        m_data.addElements(m_position, bigB, off, len);
        m_position += len;
    }

    @Override
    public void writeBoolean(final boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    @Override
    public void writeByte(final int v) throws IOException {
        write(v);
    }

    @Override
    public void writeShort(final int v) throws IOException {
        m_data.add((byte)(v & 0xFF));
        m_position++;
        m_data.add((byte)((v >>> 8) & 0xFF));
        m_position++;
    }

    @Override
    public void writeChar(final int v) throws IOException {
        writeShort(v);
    }

    @Override
    public void writeInt(final int v) throws IOException {
        m_data.add((byte)(v & 0xFF));
        m_position++;
        m_data.add((byte)((v >>> 8) & 0xFF));
        m_position++;
        m_data.add((byte)((v >>> 16) & 0xFF));
        m_position++;
        m_data.add((byte)((v >>> 24) & 0xFF));
        m_position++;
    }

    @Override
    public void writeLong(final long v) throws IOException {
        for (int i = 0; i < 8; i++) {
            m_data.add((byte)(v >>> (i * 8)));
            m_position++;
        }
    }

    @Override
    public void writeFloat(final float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeDouble(final double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeBytes(final String s) throws IOException {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            m_data.add((byte)s.charAt(i));
            m_position++;
        }
    }

    @Override
    public void writeChars(final String s) throws IOException {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            writeChar(s.charAt(i));
        }
    }

    /**
     * Writes four bytes of length information to the output stream, followed by the UTF-8 representation of every
     * character in the string <code>s</code>. If <code>s</code> is <code>null</code>, a
     * <code>NullPointerException</code> is thrown. The length of the information represents the number of bytes of the
     * encoded string and is written to the output stream in exactly the manner of the <code>writeInt</code> method.
     * <p>
     *
     * The implementation of this method differs from the contract of {@link DataOutput#writeUTF(String)} in two ways:
     * <ol>
     * <li>It encodes strings not in modified UTF-8 format, but in standard UTF-8 format.</li>
     * <li>It allows writing encoded strings with a length of up to {@link Integer#MAX_VALUE} bytes.</li>
     * </ol>
     *
     * The bytes written by this method may be read by the {@link ByteBigArrayDataInput#readUTF()}, which will then
     * return a <code>String</code> equal to <code>s</code>.
     *
     * @param s the string value to be written.
     */
    @Override
    public void writeUTF(final String s) throws IOException {
        // TODO(AP-23858) Is CharsetEncoder faster?
        // TODO(AP-23858) Can encode directly to the underlying ByteBigList without a temporary byte[]?
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeInt(bytes.length);
        write(bytes);
    }
}