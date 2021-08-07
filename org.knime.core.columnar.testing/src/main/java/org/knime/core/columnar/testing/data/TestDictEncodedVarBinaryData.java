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
import org.knime.core.columnar.testing.data.TestIntData.TestIntDataFactory;
import org.knime.core.columnar.testing.data.TestLongData.TestLongDataFactory;
import org.knime.core.columnar.testing.data.TestStructData.TestStructDataFactory;
import org.knime.core.columnar.testing.data.TestVarBinaryData.TestVarBinaryDataFactory;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public final class TestDictEncodedVarBinaryData extends AbstractTestDictEncodedData<Object>
    implements DictEncodedVarBinaryWriteData, DictEncodedVarBinaryReadData {

    public static final class TestDictEncodedVarBinaryDataFactory implements TestDataFactory {

        public static final TestDictEncodedVarBinaryDataFactory INSTANCE = new TestDictEncodedVarBinaryDataFactory();

        private final TestStructDataFactory m_delegate;

        private TestDictEncodedVarBinaryDataFactory() {
            m_delegate = new TestStructDataFactory(TestIntDataFactory.INSTANCE, TestLongDataFactory.INSTANCE,
                TestVarBinaryDataFactory.INSTANCE);
        }

        @Override
        public TestDictEncodedVarBinaryData createWriteData(final int capacity) {
            return new TestDictEncodedVarBinaryData(m_delegate.createWriteData(capacity));
        }

        @Override
        public TestDictEncodedVarBinaryData createReadData(final Object[] data) {
            return new TestDictEncodedVarBinaryData(m_delegate.createReadData(data));
        }

        @Override
        public TestDictEncodedVarBinaryData createReadData(final Object[] data, final int length) {
            return new TestDictEncodedVarBinaryData(m_delegate.createReadData(data, length));
        }

    }

    TestDictEncodedVarBinaryData(final TestStructData delegate) {
        super(delegate);
    }

    @Override
    public TestDictEncodedVarBinaryData close(final int length) {
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
            return deserializer.deserialize(input);
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
        int dictKey = m_dictValToKey.computeIfAbsent(val, v -> {
            ((VarBinaryWriteData)m_delegate.getWriteDataAt(2)).setBytes(index, val);
            return generateKey(val);
        });

        setDictKey(index, dictKey);
    }

    @Override
    public byte[] getBytes(final int index) {
        int dictKey = getDictKey(index);

        return (byte[])m_dictKeyToVal.computeIfAbsent(dictKey,
            k -> ((VarBinaryReadData)m_delegate.getReadDataAt(2)).getBytes(index));
    }
}
