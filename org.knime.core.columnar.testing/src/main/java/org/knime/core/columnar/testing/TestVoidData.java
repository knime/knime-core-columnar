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
 *   29 Oct 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.testing;

import static org.junit.Assert.assertEquals;

import org.knime.core.columnar.data.VoidData.VoidReadData;
import org.knime.core.columnar.data.VoidData.VoidWriteData;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
final class TestVoidData extends AbstractTestData implements VoidWriteData, VoidReadData {

    static final class TestVoidDataFactory implements TestDataFactory {

        static final TestVoidDataFactory INSTANCE = new TestVoidDataFactory();

        private TestVoidDataFactory() {
        }

        @Override
        public TestVoidData createWriteData(final int capacity) {
            return new TestVoidData(capacity);
        }

        @Override
        public TestVoidData createReadData(final Object data) {
            return new TestVoidData((Integer)data);
        }

    }

    private int m_capacity;

    private int m_numValues;

    TestVoidData(final int capacity) {
        m_capacity = capacity;
    }

    TestVoidData(final Integer numValues) {
        m_numValues = numValues;
    }

    @Override
    public int capacity() {
        return m_capacity;
    }

    @Override
    public void expand(final int minimumCapacity) {
        m_capacity = minimumCapacity;
    }

    @Override
    public long sizeOf() {
        return 0;
    }

    @Override
    public int length() {
        return m_numValues;
    }

    @Override
    public VoidReadData close(final int length) {
        m_numValues = length;
        assertEquals("Reference count on close not 1.", 1, getRefs());
        return this;
    }

    @Override
    public Object get() {
        return Integer.valueOf(m_numValues);
    }

}
