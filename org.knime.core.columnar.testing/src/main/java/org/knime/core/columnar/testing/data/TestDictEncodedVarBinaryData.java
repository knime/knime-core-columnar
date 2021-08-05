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
 */
package org.knime.core.columnar.testing.data;

import java.util.Map;

import org.knime.core.columnar.data.dictencoding.DictEncodedVarBinaryData.DictEncodedVarBinaryReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedVarBinaryData.DictEncodedVarBinaryWriteData;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public final class TestDictEncodedVarBinaryData extends AbstractTestDictEncodedObjectData implements DictEncodedVarBinaryWriteData, DictEncodedVarBinaryReadData {

    public static final class TestDictEncodedVarBinaryDataFactory implements TestDataFactory {

        public static final TestDictEncodedVarBinaryDataFactory INSTANCE = new TestDictEncodedVarBinaryDataFactory();

        private TestDictEncodedVarBinaryDataFactory() {
        }

        @Override
        public TestDictEncodedVarBinaryData createWriteData(final int capacity) {
            return new TestDictEncodedVarBinaryData(capacity);
        }

        @SuppressWarnings("unchecked")
        @Override
        public TestDictEncodedVarBinaryData createReadData(final Object[] packedData) {
            return new TestDictEncodedVarBinaryData((Integer[])packedData[0], (Map<Integer, Object>)packedData[1]);
        }

        @Override
        public TestData createReadData(final Object[] packedData, final int length) {
            return createReadData(packedData);
        }

    }

    TestDictEncodedVarBinaryData(final int capacity) {
        super(capacity);
    }

    TestDictEncodedVarBinaryData(final Integer[] references, final Map<Integer, Object> dictionary) {
        super(references, dictionary);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getDictEntry(final int dictionaryIndex, final ObjectDeserializer<T> deserializer) {
        return (T)m_dict.get(dictionaryIndex);
    }

    @Override
    public <T> void setDictEntry(final int dictionaryIndex, final T obj, final ObjectSerializer<T> serializer) {
        m_dict.put(dictionaryIndex, obj);
    }

    @Override
    public DictEncodedVarBinaryReadData close(final int length) {
        closeInternal(length);
        return this;
    }
}
