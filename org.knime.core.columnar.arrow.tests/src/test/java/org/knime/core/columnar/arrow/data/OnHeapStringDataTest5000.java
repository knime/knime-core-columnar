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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.knime.core.columnar.arrow.AbstractArrowDataTest;
import org.knime.core.columnar.arrow.data.OnHeapStringData5000.OnHeapStringDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapStringData5000.OnHeapStringReadData;
import org.knime.core.columnar.arrow.data.OnHeapStringData5000.OnHeapStringWriteData;

import com.google.common.base.Utf8;

/**
 * Test {@link OnHeapStringData}
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public class OnHeapStringDataTest5000 extends AbstractArrowDataTest<OnHeapStringWriteData, OnHeapStringReadData> {

    private static final int MAX_LENGTH = 100;

    private static final Map<Integer, String> VALUES = new HashMap<>();

    // Because generating random Strings takes too much time
    private static final String DEFAULT_STRING = "foobar";

    /** Create the test for {@link OnHeapStringData} */
    public OnHeapStringDataTest5000() {
        super(OnHeapStringDataFactory.INSTANCE);
    }

    @Override
    protected OnHeapStringWriteData castW(final Object o) {
        assertTrue(o instanceof OnHeapStringWriteData);
        return (OnHeapStringWriteData)o;
    }

    @Override
    protected OnHeapStringReadData castR(final Object o) {
        assertTrue(o instanceof OnHeapStringReadData);
        return (OnHeapStringReadData)o;
    }

    @Override
    protected void setValue(final OnHeapStringWriteData data, final int index, final int seed) {
        data.setString(index, valueFor(seed));
    }

    @Override
    protected void checkValue(final OnHeapStringReadData data, final int index, final int seed) {
        assertEquals(valueFor(seed), data.getString(index));
    }

    @Override
    protected boolean isReleasedW(final OnHeapStringWriteData data) {
        // On-heap data does not have resources that need explicit release
        return false;
    }

    @Override
    protected boolean isReleasedR(final OnHeapStringReadData data) {
        // On-heap data does not have resources that need explicit release
        return false;
    }

    @Override
    protected long getMinSize(final int valueCount, final int capacity) {
        long numBytes = 0;
        for (int i = 0; i < valueCount; i++) {
            numBytes += Utf8.encodedLength(valueFor(i));
        }

        return numBytes // data
            + 4L * (capacity + 1) // 4 bytes per value for offset buffer
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
