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
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.io.ReadableDataInput;
import org.knime.core.table.io.ReadableDataInputStream;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public final class TestVarBinaryData extends AbstractTestData implements VarBinaryWriteData, VarBinaryReadData {

    public static final class TestVarBinaryDataFactory implements TestDataFactory {

        public static final TestVarBinaryDataFactory INSTANCE = new TestVarBinaryDataFactory();

        private TestVarBinaryDataFactory() {
        }

        @Override
        public TestVarBinaryData createWriteData(final int capacity) {
            return new TestVarBinaryData(capacity);
        }

        @Override
        public TestVarBinaryData createReadData(final Object[] data) {
            return createReadData(data, data.length);
        }

        @Override
        public TestVarBinaryData createReadData(final Object[] data, final int length) {
            return new TestVarBinaryData(data, length);
        }

    }

    TestVarBinaryData(final int capacity) {
        super(capacity);
    }

    TestVarBinaryData(final Object[] varBinaries, final int length) {
        super(varBinaries);
        close(length);
    }

    @Override
    public VarBinaryReadData close(final int length) {
        closeInternal(length);
        return this;
    }

    @Override
    public byte[] getBytes(final int index) {
        return (byte[])get()[index];
    }

    @Override
    public void setBytes(final int index, final byte[] val) {
        get()[index] = val;
    }

    @Override
    public void copyFrom(final VarBinaryReadData readData, final int fromIndex, final int toIndex) {
        setBytes(toIndex, readData.getBytes(fromIndex));
    }

    @Override
    public <T> T getObject(final int index, final ObjectDeserializer<T> deserializer) {
        final ReadableDataInput input = new ReadableDataInputStream(new ByteArrayInputStream(getBytes(index)));
        try {
            return deserializer.deserialize(input);
        } catch (IOException ex) {
            throw new IllegalStateException("Error during deserialization", ex);
        }
    }

    @Override
    public <T> void setObject(final int index, final T value, final ObjectSerializer<T> serializer) {
        final ByteArrayOutputStream bs = new ByteArrayOutputStream();
        final DataOutput output = new DataOutputStream(bs);
        try {
            serializer.serialize(output, value);
        } catch (IOException ex) {
            throw new IllegalStateException("Error during serialization", ex);
        }
        setBytes(index, bs.toByteArray());
    }

    @Override
    public void writeToAccess(final WriteAccess access, final int index) {
        // TODO: are getBytes and getObject equivalent here?
//        ((VarBinaryWriteAccess)access).setByteArray(getBytes(index));
        throw new UnsupportedOperationException("Var binaries are too complicated for now");
    }
}
