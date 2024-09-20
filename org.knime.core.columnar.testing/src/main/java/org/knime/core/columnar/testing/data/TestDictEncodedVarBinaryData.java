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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedVarBinaryReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedVarBinaryWriteData;
import org.knime.core.columnar.testing.data.TestStructData.TestStructDataFactory;
import org.knime.core.columnar.testing.data.TestVarBinaryData.TestVarBinaryDataFactory;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.io.ReadableDataInputStream;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;

/**
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public final class TestDictEncodedVarBinaryData<K> extends AbstractTestDictEncodedData<Object, K>
    implements DictEncodedVarBinaryWriteData<K>, DictEncodedVarBinaryReadData<K> {

    public static final class TestDictEncodedVarBinaryDataFactory implements TestDataFactory {

        public static final TestDictEncodedVarBinaryDataFactory BYTE_INSTANCE =
                new TestDictEncodedVarBinaryDataFactory(KeyType.BYTE_KEY);

        public static final TestDictEncodedVarBinaryDataFactory INT_INSTANCE =
            new TestDictEncodedVarBinaryDataFactory(KeyType.INT_KEY);

        public static final TestDictEncodedVarBinaryDataFactory LONG_INSTANCE =
            new TestDictEncodedVarBinaryDataFactory(KeyType.LONG_KEY);

        public static TestDictEncodedVarBinaryDataFactory factoryForKeyType(final KeyType keyType) {
            if (keyType == KeyType.BYTE_KEY) {
                return BYTE_INSTANCE;
            } else if (keyType == KeyType.INT_KEY) {
                return INT_INSTANCE;
            } else if (keyType == KeyType.LONG_KEY) {
                return LONG_INSTANCE;
            } else {
                throw new IllegalArgumentException("Invalid key type " + keyType);
            }
        }

        private final TestStructDataFactory m_delegate;

        private final KeyType m_keyType;

        private TestDictEncodedVarBinaryDataFactory(final KeyType keyType) {
            m_keyType = keyType;
            m_delegate = new TestStructDataFactory(AbstractTestData.createKeyDataFactory(keyType),
                TestVarBinaryDataFactory.INSTANCE);
        }

        @Override
        public TestData createWriteData(final int capacity) {
            return new TestDictEncodedVarBinaryData<>(m_delegate.createWriteData(capacity),
                    AbstractTestData.createKeyInstance(m_keyType));
        }

        @Override
        public TestData createReadData(final Object[] data) {
            return new TestDictEncodedVarBinaryData<>(m_delegate.createReadData(data),
                    AbstractTestData.createKeyInstance(m_keyType));
        }

        @Override
        public TestData createReadData(final Object[] data, final int length) {
            return new TestDictEncodedVarBinaryData<>(m_delegate.createReadData(data, length),
                    AbstractTestData.createKeyInstance(m_keyType));
        }

    }

    TestDictEncodedVarBinaryData(final TestStructData delegate, final K keyInstance) {
        super(delegate, keyInstance);
    }

    @Override
    public TestDictEncodedVarBinaryData<K> close(final int length) {
        super.close(length);
        return this;
    }

    @Override
    public <T> void setObject(final int index, final T val, final ObjectSerializer<T> serializer) {
        final ByteArrayOutputStream bs = new ByteArrayOutputStream();
        final DataOutput output = new DataOutputStream(bs);
        try {
            serializer.serialize(output, val);
        } catch (IOException ex) {
            throw new IllegalStateException("Error during serialization", ex);
        }
        setBytes(index, bs.toByteArray());
    }

    @Override
    public <T> T getObject(final int index, final ObjectDeserializer<T> deserializer) {
        final DataInput input = new DataInputStream(new ByteArrayInputStream(getBytes(index)));
        try {
            return deserializer.deserialize(new ReadableDataInputStream(new ByteArrayInputStream(getBytes(index))));
        } catch (IOException ex) {
            throw new IllegalStateException("Error during deserialization", ex);
        }
    }

    @Override
    public Object[] get() {
        return m_delegate.get();
    }

    @Override
    public void setBytes(final int index, final byte[] val) {
        K dictKey = m_dictValToKey.computeIfAbsent(val, v -> {
            ((VarBinaryWriteData)m_delegate.getWriteDataAt(1)).setBytes(index, val);
            return generateKey(val);
        });

        setDictKey(index, dictKey);
    }

    @Override
    public byte[] getBytes(final int index) {
        K dictKey = getDictKey(index);

        return (byte[])m_dictKeyToVal.computeIfAbsent(dictKey,
            k -> ((VarBinaryReadData)m_delegate.getReadDataAt(1)).getBytes(index));
    }

    @Override
    public void setFrom(final VarBinaryReadData readData, final int fromIndex, final int toIndex) {
        if (readData.isMissing(fromIndex)) {
            setMissing(toIndex);
        } else {
            setBytes(toIndex, readData.getBytes(fromIndex));
        }
    }

    @Override
    public void writeToAccess(final WriteAccess access, final int index) {
        throw new UnsupportedOperationException("Writing to Access not implemented for Dict Encoded VarBinary in test");
    }
}
