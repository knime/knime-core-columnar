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
package org.knime.core.columnar.testing;

import static org.junit.Assert.assertEquals;

import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class TestDoubleData extends AbstractTestData implements DoubleWriteData, DoubleReadData {

    static final class TestDoubleDataFactory implements TestDataFactory {

        static final TestDoubleDataFactory INSTANCE = new TestDoubleDataFactory();

        private TestDoubleDataFactory() {
        }

        @Override
        public TestDoubleData createWriteData(final int capacity) {
            return new TestDoubleData(capacity);
        }

        @Override
        public TestDoubleData createReadData(final Object data) {
            return new TestDoubleData((Double[])data);
        }

    }

    private int m_capacity;

    private Double[] m_values;

    private int m_numValues;

    TestDoubleData(final int capacity) {
        m_capacity = capacity;
        m_values = new Double[capacity];
    }

    TestDoubleData(final Double[] doubles) {
        m_capacity = doubles.length;
        m_values = doubles;
        m_numValues = doubles.length;
    }

    @Override
    public int capacity() {
        return m_capacity;
    }

    @Override
    public long sizeOf() {
        return m_numValues;
    }

    @Override
    public void expand(final int minimumCapacity) {
        final Double[] expanded = new Double[minimumCapacity];
        System.arraycopy(m_values, 0, expanded, 0, m_capacity);
        m_values = expanded;
        m_capacity = minimumCapacity;
    }

    @Override
    public DoubleReadData close(final int length) {
        m_numValues = length;
        assertEquals("Reference count on close not 1.", 1, getRefs());
        return this;
    }

    @Override
    public int length() {
        return m_numValues;
    }

    @Override
    public synchronized void setMissing(final int index) {
        m_values[index] = null;
    }

    @Override
    public synchronized boolean isMissing(final int index) {
        return m_values[index] == null;
    }

    @Override
    public synchronized double getDouble(final int index) {
        return m_values[index];
    }

    @Override
    public synchronized void setDouble(final int index, final double val) {
        m_values[index] = val;
    }

    @Override
    public Double[] get() {
        return m_values;
    }

}
