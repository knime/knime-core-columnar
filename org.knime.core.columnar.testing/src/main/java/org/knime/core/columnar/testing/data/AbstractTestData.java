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
 *   Jul 6, 2021 (Carsten Haubold, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.testing.data;

import java.util.Arrays;

import org.knime.core.columnar.testing.data.TestByteData.TestByteDataFactory;
import org.knime.core.columnar.testing.data.TestIntData.TestIntDataFactory;
import org.knime.core.columnar.testing.data.TestLongData.TestLongDataFactory;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;

/**
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractTestData implements TestData {
    private int m_refs = 1;

    private int m_size;

    private Object[] m_values;

    AbstractTestData(final int capacity) {
        this(new Object[capacity]);
    }

    AbstractTestData(final Object[] objects) {
        this(objects, objects.length);
    }

    AbstractTestData(final Object[] objects, final int size) {
        m_values = objects;
        m_size = size;
    }

    @Override
    public final synchronized void release() {
        m_refs--;
        if (m_refs < 0) {
            throw new IllegalStateException("Released too many times.");
        }
    }

    @Override
    public final synchronized void retain() {
        if (m_refs > 0) {
            m_refs++;
        } else {
            throw new IllegalStateException("Reference count of data at or below 0. Data is no longer available.");
        }
    }

    @Override
    public final synchronized int getRefs() {
        return m_refs;
    }

    @Override
    public long sizeOf() {
        return length();
    }

    @Override
    public long usedSizeFor(final int numElements) {
        return numElements;
    }

    @Override
    public final int capacity() {
        return m_size;
    }

    @Override
    public void expand(final int minimumCapacity) {
        final Object[] expanded = new Object[minimumCapacity];
        System.arraycopy(m_values, 0, expanded, 0, capacity());
        m_values = expanded;
        m_size = minimumCapacity;
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
    public final int length() {
        return m_size;
    }

    final void closeInternal(final int length) {
        if (getRefs() != 1) {
            throw new IllegalStateException("Closed with outstanding references.");
        }
        m_size = length;
    }

    @Override
    public final Object[] get() {
        return m_values;
    }


    @SuppressWarnings("unchecked")
    static <K> K createKeyInstance(final KeyType keyType) {
        if (keyType == KeyType.BYTE_KEY) {
            return (K)Byte.valueOf((byte)0);
        } else if (keyType == KeyType.INT_KEY) {
            return (K)Integer.valueOf(0);
        } else if (keyType == KeyType.LONG_KEY) {
            return (K)Long.valueOf(0);
        }

        throw new IllegalArgumentException("Unsupported key type " + keyType);
    }

    static TestDataFactory createKeyDataFactory(final KeyType keyType) {
        if (keyType == KeyType.BYTE_KEY) {
            return TestByteDataFactory.INSTANCE;
        } else if (keyType == KeyType.INT_KEY) {
            return TestIntDataFactory.INSTANCE;
        } else if (keyType == KeyType.LONG_KEY) {
            return TestLongDataFactory.INSTANCE;
        }

        throw new IllegalArgumentException("Unsupported key type " + keyType);
    }

    @Override
    public String toString() {
        return Arrays.deepToString(m_values);
    }
}
