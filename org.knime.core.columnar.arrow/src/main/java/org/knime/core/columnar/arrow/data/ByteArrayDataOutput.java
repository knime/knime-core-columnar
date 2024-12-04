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

import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Custom DataOutput that writes directly into a byte array without copying.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
abstract class ByteArrayDataOutput implements DataOutput {
    protected byte[] m_data;

    protected int m_position;

    public ByteArrayDataOutput(final byte[] data, final int startPosition) {
        m_data = data;
        m_position = startPosition;
    }

    protected abstract void ensureCapacity(int newLength);

    public int getPosition() {
        return m_position;
    }

    @Override
    public void write(final int b) throws IOException {
        ensureCapacity(m_position + 1);
        m_data[m_position++] = (byte)b;
    }

    @Override
    public void write(final byte[] b) throws IOException {
        ensureCapacity(m_position + b.length);
        System.arraycopy(b, 0, m_data, m_position, b.length);
        m_position += b.length;
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        ensureCapacity(m_position + len);
        System.arraycopy(b, off, m_data, m_position, len);
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
        ensureCapacity(m_position + 2);
        m_data[m_position++] = (byte)((v >>> 8) & 0xFF);
        m_data[m_position++] = (byte)(v & 0xFF);
    }

    @Override
    public void writeChar(final int v) throws IOException {
        writeShort(v);
    }

    @Override
    public void writeInt(final int v) throws IOException {
        ensureCapacity(m_position + 4);
        m_data[m_position++] = (byte)((v >>> 24) & 0xFF);
        m_data[m_position++] = (byte)((v >>> 16) & 0xFF);
        m_data[m_position++] = (byte)((v >>> 8) & 0xFF);
        m_data[m_position++] = (byte)(v & 0xFF);
    }

    @Override
    public void writeLong(final long v) throws IOException {
        ensureCapacity(m_position + 8);
        for (int i = 7; i >= 0; i--) {
            m_data[m_position++] = (byte)(v >>> (i * 8));
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
        ensureCapacity(m_position + len);
        for (int i = 0; i < len; i++) {
            m_data[m_position++] = (byte)s.charAt(i);
        }
    }

    @Override
    public void writeChars(final String s) throws IOException {
        int len = s.length();
        ensureCapacity(m_position + len * 2);
        for (int i = 0; i < len; i++) {
            writeChar(s.charAt(i));
        }
    }

    @Override
    public void writeUTF(final String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeShort(bytes.length);
        write(bytes);
    }
}