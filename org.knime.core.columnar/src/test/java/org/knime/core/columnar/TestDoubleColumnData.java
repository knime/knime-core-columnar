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
package org.knime.core.columnar;

import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.columnar.data.DoubleData;

class TestDoubleColumnData implements ColumnData, DoubleData {

    public Double[] m_values;

    public int m_numValues;

    private int m_chunkCapacity;

    private AtomicInteger m_ref = new AtomicInteger(1);

    TestDoubleColumnData(final int chunkCapacity) {
        m_chunkCapacity = chunkCapacity;
    }

    public TestDoubleColumnData(final Double[] doubles) {
        m_values = doubles;
        m_numValues = doubles.length;
    }

    @Override
    public void release() {
        if (m_ref.decrementAndGet() == 0) {
            m_values = null;
        }
    }

    @Override
    public void retain() {
        m_ref.incrementAndGet();
    }

    @Override
    public int sizeOf() {
        return m_values.length * 8;
    }

    @Override
    public void ensureCapacity(final int capacity) {
        m_values = new Double[m_chunkCapacity];
    }

    @Override
    public int getMaxCapacity() {
        return m_chunkCapacity;
    }

    @Override
    public void setNumValues(final int numValues) {
        m_numValues = numValues;
    }

    @Override
    public int getNumValues() {
        return m_numValues;
    }

    @Override
    public void setMissing(final int index) {
        m_values[index] = null;
    }

    @Override
    public boolean isMissing(final int index) {
        return m_values[index] == null;
    }

    @Override
    public double getDouble(final int index) {
        return m_values[index];
    }

    @Override
    public void setDouble(final int index, final double val) {
        m_values[index] = val;
    }

    public Double[] get() {
        return m_values;
    }

    public boolean isClosed() {
        return m_values == null;
    }

}
