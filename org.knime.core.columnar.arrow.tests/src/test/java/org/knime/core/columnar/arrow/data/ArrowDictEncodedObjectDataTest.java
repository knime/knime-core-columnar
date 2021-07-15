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
package org.knime.core.columnar.arrow.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.util.Random;

import org.knime.core.columnar.arrow.AbstractArrowDataTest;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedVarBinaryData.ArrowDictEncodedVarBinaryDataFactory;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedVarBinaryData.ArrowDictEncodedVarBinaryReadData;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedVarBinaryData.ArrowDictEncodedVarBinaryWriteData;

/**
 * Test {@link ArrowDictEncodedVarBinaryData}
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class ArrowDictEncodedObjectDataTest extends AbstractArrowDataTest<ArrowDictEncodedVarBinaryWriteData, ArrowDictEncodedVarBinaryReadData> {

    /** Create the test for {@link ArrowDictEncodedVarBinaryData} */
    public ArrowDictEncodedObjectDataTest() {
        super(ArrowDictEncodedVarBinaryDataFactory.INSTANCE);
    }

    @Override
    protected ArrowDictEncodedVarBinaryWriteData castW(final Object o) {
        assertTrue(o instanceof ArrowDictEncodedVarBinaryWriteData);
        return (ArrowDictEncodedVarBinaryWriteData)o;
    }

    @Override
    protected ArrowDictEncodedVarBinaryReadData castR(final Object o) {
        assertTrue(o instanceof ArrowDictEncodedVarBinaryReadData);
        return (ArrowDictEncodedVarBinaryReadData)o;
    }

    @Override
    protected void setValue(final ArrowDictEncodedVarBinaryWriteData data, final int index, final int seed) {
        final var offset = data.m_offset;
        data.setDictEntry(index + offset, valueFor(seed), (out, v) -> out.writeUTF(v));
        data.setReference(index, index + offset);
    }

    @Override
    protected void checkValue(final ArrowDictEncodedVarBinaryReadData data, final int index, final int seed) {
        final var offset = data.m_offset;
        assertEquals(valueFor(seed), data.getDictEntry(index + offset, DataInput::readUTF));
        assertEquals(data.getReference(index), index + offset);
    }

    @Override
    protected boolean isReleasedW(final ArrowDictEncodedVarBinaryWriteData data) {
        return data.m_vector == null;
    }

    @Override
    @SuppressWarnings("resource") // Resources handled by vector
    protected boolean isReleasedR(final ArrowDictEncodedVarBinaryReadData data) {
        return data.m_vector.getDataBuffer().capacity() == 0
                && data.m_vector.getValidityBuffer().capacity() == 0
                && data.getDictionary().getDataBuffer().capacity() == 0
                && data.getDictionary().getValidityBuffer().capacity() == 0;
    }

    @Override
    protected long getMinSize(final int valueCount, final int capacity) {
        long numBytes = 4 * valueCount;
        long dictSize = 0;

        for (int i = 0; i < valueCount; i++) {
            // well, actually we should consider for the UTF8 length of the strings,
            // but the generated strings don't contain any chars that need encoding
            dictSize += valueFor(i).getBytes().length;
        }

        return numBytes // data buffer
            + dictSize // dictionary
            + 4 * capacity // offset buffer
            + (long)Math.ceil(capacity / 8.0); // validity buffer
    }

    private static String valueFor(final int seed) {
        final Random random = new Random(seed);
        return String.valueOf(random.nextLong());
    }
}
