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
 *   Nov 9, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import org.apache.arrow.vector.LargeVarBinaryVector;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

/**
 * Helper class which holds a dictionary in a Map and can write it to a LargeVarBinaryVector
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
final class InMemoryDictEncoding<T> {

    private static final long INIT_BYTES_PER_ENTRY = 256;

    private int m_runningDictIndex = 0;

    private final BiMap<T, Integer> m_inMemDict;

    private final BiMap<Integer, T> m_invInMemDict;

    private final LargeVarBinaryVector m_vector;

    private final ObjectSerializer<T> m_serializer;

    InMemoryDictEncoding(final LargeVarBinaryVector vector, final int initialDictSize,
        final ObjectSerializer<T> serializer, final ObjectDeserializer<T> deserializer) {
        m_vector = vector;
        m_inMemDict = HashBiMap.create(initialDictSize);
        m_invInMemDict = m_inMemDict.inverse();
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isSet(i) != 0) {
                m_invInMemDict.put(i, ArrowBufIO.deserialize(i, vector, deserializer));
            }
        }
        m_serializer = serializer;
    }

    /**
     * @param id the index of the object
     * @return the object with this index
     */
    T get(final int id) {
        return m_invInMemDict.get(id);
    }

    /**
     * Set the value and return its id.
     *
     * @param value the object
     * @return the index of the object
     */
    Integer set(final T value) {
        return m_inMemDict.computeIfAbsent(value, k -> m_runningDictIndex++);
    }

    /**
     * Update the dictionary vector and return it
     */
    LargeVarBinaryVector getDictionaryVector() {
        final int numDistinctValues = m_inMemDict.size();
        final int dictValueCount = m_vector.getValueCount();
        if (dictValueCount == 0) {
            // Allocated new memory for all distinct values
            m_vector.allocateNew(INIT_BYTES_PER_ENTRY * numDistinctValues, numDistinctValues);
        }
        // Fill the vector with the encoded values
        for (int i = dictValueCount; i < numDistinctValues; i++) {
            ArrowBufIO.serialize(i, m_invInMemDict.get(i), m_vector, m_serializer);
        }
        m_vector.setValueCount(numDistinctValues);
        return m_vector;
    }

    long sizeOf() {
        return ArrowSizeUtils.sizeOfVariableWidth(m_vector);
    }

    void closeVector() {
        m_vector.close();
    }

    @Override
    public String toString() {
        return m_vector.toString();
    }
}
