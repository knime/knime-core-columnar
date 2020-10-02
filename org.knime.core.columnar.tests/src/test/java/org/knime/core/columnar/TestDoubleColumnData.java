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

import java.io.Closeable;

import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.phantom.CloseableDelegateFinalizer;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public final class TestDoubleColumnData implements DoubleWriteData, DoubleReadData {

    public static final class Delegate implements Closeable {

        private boolean m_closed = false;

        @Override
        public void close() {
            m_closed = true;
        }

        public boolean isClosed() {
            return m_closed;
        }

    }

    private final Delegate m_delegate = new Delegate();

    private int m_chunkCapacity;

    private int m_refs = 1;

    private Double[] m_values;

    private int m_numValues;

    private CloseableDelegateFinalizer m_closed;

    public static TestDoubleColumnData create(final int capacity) {
        final TestDoubleColumnData data = new TestDoubleColumnData(capacity);
        data.m_closed = CloseableDelegateFinalizer.create(data, data.m_delegate, "Test Column Data");
        return data;
    }

    private TestDoubleColumnData(final int capacity) {
        m_chunkCapacity = capacity;
        m_values = new Double[capacity];
    }

    public static TestDoubleColumnData create(final Double... doubles) {
        final TestDoubleColumnData data = new TestDoubleColumnData(doubles);
        data.m_closed = CloseableDelegateFinalizer.create(data, data.m_delegate, "Test Column Data");
        return data;
    }

    private TestDoubleColumnData(final Double[] doubles) {
        m_chunkCapacity = doubles.length;
        m_values = doubles;
        m_numValues = doubles.length;
    }

    @Override
    public synchronized void release() {
        if (--m_refs == 0) {
            close();
        }
    }

    @Override
    public synchronized void retain() {
        m_refs++;
    }

    @Override
    public int sizeOf() {
        return m_numValues;
    }

//    @Override
//    public void ensureCapacity(final int capacity) {
//        m_chunkCapacity = capacity;
//        m_values = new Double[m_chunkCapacity];
//    }
//
//    @Override
//    public int getMaxCapacity() {
//        return m_chunkCapacity;
//    }
//
//    @Override
//    public synchronized void setNumValues(final int numValues) {
//        m_numValues = numValues;
//    }
//
//    @Override
//    public synchronized int getNumValues() {
//        return m_numValues;
//    }

    @Override
    public int capacity() {
        return m_chunkCapacity;
    }

    @Override
    public DoubleReadData close(final int length) {
        m_numValues = length;
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

    Double[] get() {
        return m_values;
    }

    public synchronized int getRefs() {
        return m_refs;
    }

    public void close() {
        m_closed.close();
    }

    public Delegate getDelegate() {
        return m_delegate;
    }

}
