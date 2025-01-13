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
package org.knime.core.columnar.arrow.offheap.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.knime.core.columnar.arrow.offheap.AbstractOffHeapArrowDataTest;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowStringData.ArrowStringDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowStringData.ArrowStringReadData;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowStringData.ArrowStringWriteData;

import com.google.common.base.Utf8;

/**
 * Test {@link OffHeapArrowStringData}
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class OffHeapArrowStringDataTest extends AbstractOffHeapArrowDataTest<ArrowStringWriteData, ArrowStringReadData> {

    private static final int MAX_LENGTH = 100;

    private static final Map<Integer, String> VALUES = new HashMap<>();

    // Because generating random Strings takes to much time
    private static final String DEFAULT_STRING = "foobar";

    /** Create the test for {@link OffHeapArrowStringData} */
    public OffHeapArrowStringDataTest() {
        super(ArrowStringDataFactory.INSTANCE);
    }

    @Override
    protected ArrowStringWriteData castW(final Object o) {
        assertTrue(o instanceof ArrowStringWriteData);
        return (ArrowStringWriteData)o;
    }

    @Override
    protected ArrowStringReadData castR(final Object o) {
        assertTrue(o instanceof ArrowStringReadData);
        return (ArrowStringReadData)o;
    }

    @Override
    protected void setValue(final ArrowStringWriteData data, final int index, final int seed) {
        data.setString(index, valueFor(seed));
    }

    @Override
    protected void checkValue(final ArrowStringReadData data, final int index, final int seed) {
        assertEquals(valueFor(seed), data.getString(index));
    }

    @Override
    protected boolean isReleasedW(final ArrowStringWriteData data) {
        return data.m_vector == null;
    }

    @Override
    @SuppressWarnings("resource")
    protected boolean isReleasedR(final ArrowStringReadData data) {
        return data.m_vector.getDataBuffer().capacity() == 0 && data.m_vector.getValidityBuffer().capacity() == 0;
    }

    @Override
    protected long getMinSize(final int valueCount, final int capacity) {
        long numBytes = 0;
        for (int i = 0; i < valueCount; i++) {
            numBytes += Utf8.encodedLength(valueFor(i));
        }

        return numBytes // data
            + 4L * (capacity + 1) // 4 byte per value for offset buffer
            + (long)Math.ceil(capacity / 8.0); // 1 bit per value for validity buffer
    }

    private static String valueFor(final int seed) {
        if (seed > 50) {
            // Return a fixed value for all larger seeds
            // Generating random Strings takes a long time
            return DEFAULT_STRING;
        }
        return VALUES.computeIfAbsent(seed, s -> {
            final Random random = new Random(s);
            final int count = random.nextInt(MAX_LENGTH);
            return RandomStringUtils.random(count, 0, Integer.MAX_VALUE, true, true, null, random);
        });
    }
}
