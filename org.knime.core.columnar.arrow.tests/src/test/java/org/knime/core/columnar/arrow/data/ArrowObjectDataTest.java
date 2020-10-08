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
 *   Sep 30, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.apache.arrow.vector.VarBinaryVector;
import org.knime.core.columnar.arrow.AbstractArrowDataTest;
import org.knime.core.columnar.arrow.data.ArrowObjectData.ArrowObjectDataFactory;
import org.knime.core.columnar.data.ObjectData.ObjectDataSerializer;

/**
 * Test {@link ArrowObjectData}
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public class ArrowObjectDataTest extends AbstractArrowDataTest<ArrowObjectData<byte[]>> {

    private static final int MAX_LENGTH = 100;

    /** Create the test for {@link ArrowObjectData} */
    public ArrowObjectDataTest() {
        super(new ArrowObjectDataFactory<>(DummySerializer.INSTANCE));
    }

    @Override
    protected ArrowObjectData<byte[]> cast(final Object o) {
        assertTrue(o instanceof ArrowObjectData);
        @SuppressWarnings("unchecked")
        final ArrowObjectData<byte[]> cast = (ArrowObjectData<byte[]>)o;
        return cast;
    }

    @Override
    protected void setValue(final ArrowObjectData<byte[]> data, final int index, final int seed) {
        data.setObject(index, valueFor(seed));
    }

    @Override
    protected void checkValue(final ArrowObjectData<byte[]> data, final int index, final int seed) {
        assertArrayEquals(valueFor(seed), data.getObject(index));
    }

    @Override
    @SuppressWarnings("resource") // Resources handled by vector
    protected boolean isReleased(final ArrowObjectData<byte[]> data) {
        final VarBinaryVector v = data.m_vector;
        return v.getDataBuffer().capacity() == 0 && v.getValidityBuffer().capacity() == 0;
    }

    @Override
    protected int getMinSize(final int valueCount, final int capacity) {
        int numBytes = 0;
        for (int i = 0; i < valueCount; i++) {
            numBytes += new Random(i).nextInt(MAX_LENGTH);
        }
        return numBytes + // data buffer
            4 * capacity + // offset buffer
            (int)Math.ceil(capacity / 8); // validity buffer
    }

    private static byte[] valueFor(final int seed) {
        final Random random = new Random(seed);
        final byte[] bytes = new byte[random.nextInt(MAX_LENGTH)];
        random.nextBytes(bytes);
        return bytes;
    }

    /**
     * Simple serializer implementation which adds one to the bytes on serialization and removes 1 when deserializing
     * again.
     **/
    private final static class DummySerializer implements ObjectDataSerializer<byte[]> {

        private final static DummySerializer INSTANCE = new DummySerializer();

        private DummySerializer() {
        }

        @Override
        public byte[] serialize(final byte[] obj) {
            final byte[] modifiedCopy = obj.clone();
            for (int i = 0; i < modifiedCopy.length; i++) {
                modifiedCopy[i]++;
            }
            return modifiedCopy;
        }

        @Override
        public byte[] deserialize(final byte[] modifiedCopy) {
            final byte[] obj = modifiedCopy.clone();
            for (int i = 0; i < obj.length; i++) {
                obj[i]--;
            }
            return obj;
        }

    }
}
