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
 *   Oct 14, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.arrow.vector.complex.StructVector;
import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.arrow.AbstractArrowDataTest;
import org.knime.core.columnar.arrow.data.ArrowDoubleData.ArrowDoubleDataFactory;
import org.knime.core.columnar.arrow.data.ArrowIntData.ArrowIntDataFactory;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructDataFactory;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructReadData;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructWriteData;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;

/**
 * Test {@link ArrowStructData} with a struct consisting of a double and an integer.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class ArrowSimpleStructDataTest extends AbstractArrowDataTest<ArrowStructWriteData, ArrowStructReadData> {

    /** Create the test for {@link ArrowStructData} */
    public ArrowSimpleStructDataTest() {
        super(new ArrowStructDataFactory(ArrowDoubleDataFactory.INSTANCE, ArrowIntDataFactory.INSTANCE));
    }

    @Override
    protected ArrowStructWriteData castW(final Object o) {
        assertTrue(o instanceof ArrowStructWriteData);
        return (ArrowStructWriteData)o;
    }

    @Override
    protected ArrowStructReadData castR(final Object o) {
        assertTrue(o instanceof ArrowStructReadData);
        return (ArrowStructReadData)o;
    }

    @Override
    protected void setValue(final ArrowStructWriteData data, final int index, final int seed) {
        final DoubleWriteData doubleData = data.getWriteDataAt(0);
        assertTrue(doubleData instanceof ArrowDoubleData);
        final IntWriteData intData = data.getWriteDataAt(1);
        assertTrue(intData instanceof ArrowIntData);
        doubleData.setDouble(index, seed / 2.0);
        intData.setInt(index, seed * 2);
    }

    @Override
    protected void checkValue(final ArrowStructReadData data, final int index, final int seed) {
        final DoubleReadData doubleData = data.getReadDataAt(0);
        assertTrue(doubleData instanceof ArrowDoubleData);
        final IntReadData intData = data.getReadDataAt(1);
        assertTrue(intData instanceof ArrowIntData);
        assertEquals(seed / 2.0, doubleData.getDouble(index), 0);
        assertEquals(seed * 2, intData.getInt(index));
    }

    @Override
    @SuppressWarnings("resource")
    protected boolean isReleased(final ReferencedData data) {
        final StructVector vector;
        final ArrowDoubleData doubleData;
        final ArrowIntData intData;
        if (data instanceof ArrowStructWriteData) {
            final ArrowStructWriteData d = castW(data);
            doubleData = d.getWriteDataAt(0);
            intData = d.getWriteDataAt(1);
            vector = d.m_vector;
        } else {
            final ArrowStructReadData d = castR(data);
            doubleData = d.getReadDataAt(0);
            intData = d.getReadDataAt(1);
            vector = d.m_vector;
        }

        boolean doubleReleased = doubleData.m_vector.getDataBuffer().capacity() == 0 //
            && doubleData.m_vector.getValidityBuffer().capacity() == 0;
        boolean intReleased = intData.m_vector.getDataBuffer().capacity() == 0 //
            && intData.m_vector.getValidityBuffer().capacity() == 0;
        return vector.getValidityBuffer().capacity() == 0 && doubleReleased && intReleased;
    }

    @Override
    protected int getMinSize(final int valueCount, final int capacity) {
        return 3 * (int)Math.ceil(capacity / 8.0) // Validity buffers of Stuct, Double and Integer
            + capacity * 8 // Double: Data buffer
            + capacity * 4; // Integer: Data buffer
    }
}
