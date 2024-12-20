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
 *   Dec 12, 2024 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.knime.core.columnar.arrow.data.OffsetsBuffer.IntOffsetsBuffer;
import org.knime.core.table.util.StringEncoder;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;

/**
 * Holds the data of a string column in a combination of a string array and a byte array.
 *
 * Note that none of the two representations is guaranteed to be complete to the last set index.
 *
 * The byte[] is complete up to a certain index (the last one that was encoded) ({@link #lastWrittenBytesIndex()}) which
 * is tracked by the offsets buffer. The String[] is sparsely filled. However, it is guaranteed to contain all values
 * after the {@link #lastWrittenBytesIndex()}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public class LazyEncodedStringList {

    // TODO optimize string encoding/decoding

    private final StringEncoder m_encoder = new StringEncoder();

    private String[] m_data;

    private ByteArrayList m_dataBytes;

    private IntOffsetsBuffer m_offsets;

    public LazyEncodedStringList(final int capacity) {
        m_data = new String[capacity];
        m_dataBytes = new ByteArrayList();
        m_offsets = new IntOffsetsBuffer(capacity);
    }

    private LazyEncodedStringList(final String[] data, final ByteArrayList byteArrayList,
        final IntOffsetsBuffer offsets) {
        m_data = data;
        m_dataBytes = byteArrayList;
        m_offsets = offsets;
    }

    public int size() {
        return m_data.length;
    }

    /**
     * @return the index of the last element that was written to the <code>m_dataBytes</code> array. Note that
     *         <code>m_data</code> can contain values with larger indices.
     */
    private int lastWrittenBytesIndex() {
        return m_offsets.lastWrittenIndex();
    }

    /**
     * Encodes the strings in the range <code>[lastWrittenBytesIndex() + 1, endIdx)</code> into the byte array
     *
     * @param endIdx the next index after the last string to encode
     */
    private void encode(final int endIdx) {
        for (var i = m_offsets.lastWrittenIndex() + 1; i < endIdx; i++) {
            var str = m_data[i];
            if (str != null) {
                // TODO encode directly into the target array?
                var bytes = str.getBytes(StandardCharsets.UTF_8);
                var dataIndex = m_offsets.add(i, bytes.length);
                m_dataBytes.addElements(dataIndex.start(), bytes);
            } else {
                m_offsets.add(i, 0);
            }
        }
    }

    // Writing elements

    public void setString(final int index, final String value) {
        m_data[index] = value;
    }

    public void setNumElements(final int numElements) {
        var newData = new String[numElements];
        System.arraycopy(m_data, 0, newData, 0, Math.min(m_data.length, numElements));
        m_data = newData;
        m_offsets.setNumElements(numElements);
    }

    // TODO this also allocates the buffers which is kind of strange
    public void copyTo(final BaseVariableWidthVector vector) {
        encode(size());
        vector.allocateNew(m_dataBytes.size(), size());
        vector.getDataBuffer().setBytes(0, m_dataBytes.elements(), 0, m_dataBytes.size());
        m_offsets.copyTo(vector.getOffsetBuffer());
    }

    public void setStringBytes(final int index, final byte[] val) {
        // Note - we prefer to encode the previous values to decoding this one, because
        // - it is likely that we need the byte representation of the previous values anyway
        // - we have two representations of the data in the newer data object, so this will be more flexible and
        //   efficient to work with

        // Encode the values that not yet encoded
        encode(index);

        // Copy over the new value
        var dataIndex = m_offsets.add(index, val.length);
        System.arraycopy(val, 0, m_dataBytes, dataIndex.start(), val.length);
    }

    public void setFrom(final LazyEncodedStringList sourceList, final int sourceIndex, final int targetIndex) {
        // Copy the string
        // Note that this can be null if the data was just read and not yet decoded. In this case, the
        // bytes will be set and the string will be decoded on access
        m_data[targetIndex] = sourceList.m_data[sourceIndex];

        // Copy the bytes if they are present (must be present if the string is null)
        if (sourceList.m_offsets.lastWrittenIndex() >= sourceIndex) {
            var sourceDataIndex = sourceList.m_offsets.get(sourceIndex);
            var targetDataIndex = m_offsets.add(targetIndex, sourceDataIndex.length());
            System.arraycopy(sourceList.m_dataBytes, sourceDataIndex.start(), m_dataBytes, targetDataIndex.start(),
                sourceDataIndex.length());
        }
    }

    // Accessing elements

    public String getString(final int index) {
        if (m_data[index] == null) {
            // Decode the string
            var dataIndex = m_offsets.get(index);

            // TODO decode directly from the byte array?
            var bytes = new byte[dataIndex.length()];
            m_dataBytes.getElements(dataIndex.start(), bytes, 0, dataIndex.length());
            var val = m_encoder.decode(bytes);

            m_data[index] = val;
        }

        return m_data[index];
    }

    public byte[] getStringBytesNullable(final int index) {
        if (m_offsets.lastWrittenIndex() >= index) {
            var dataIndex = m_offsets.get(index);
            var bytes = new byte[dataIndex.length()];
            System.arraycopy(m_dataBytes, dataIndex.start(), bytes, 0, bytes.length);
            return bytes;
        } else {
            // TODO should we encode the data now?
            return null;
        }
    }

    public static LazyEncodedStringList createFrom(final ArrowBuf dataBuffer, final ArrowBuf offsetsBuffer,
        final int numElements) {
        var offsets = OffsetsBuffer.createIntBuffer(offsetsBuffer, numElements);
        var dataBytes = new byte[offsets.getNumData(numElements)];
        dataBuffer.getBytes(0, dataBytes);
        return new LazyEncodedStringList(new String[numElements], new ByteArrayList(dataBytes), offsets);
    }
}
