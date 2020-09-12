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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.data.NullableWithCauseData;
import org.knime.core.columnar.phantom.CloseableCloser;

public class ArrowNullableWithCauseData<C extends ArrowData<?>>
    implements NullableWithCauseData<C>, ArrowData<StructVector> {

    private AtomicInteger m_ref = new AtomicInteger(1);

    private final ArrowVarCharData m_supplement;

    private C m_data;

    private final StructVector m_struct;

    CloseableCloser m_vectorCloser;

    private boolean m_hasSupplement = false;

    public ArrowNullableWithCauseData(final BufferAllocator allocator, final C data) {
        m_data = data;
        m_supplement = new ArrowVarCharData(allocator);
        final CustomStructVector vector = new CustomStructVector("String Supplement", allocator);

        vector.putChild("Data", m_data.get());

        m_struct = vector;
    }

    public ArrowNullableWithCauseData(final StructVector struct, final C data) {
        m_struct = struct;
        m_data = data;
        final ValueVector supplement = struct.getChildByOrdinal(1);
        if (supplement != null) {
            m_supplement = new ArrowVarCharData((VarCharVector)supplement);
        } else {
            m_supplement = null;
        }
    }

    @Override
    public void setMissing(final int index) {
        m_data.setMissing(index);
    }

    @Override
    public void ensureCapacity(final int chunkSize) {
        m_data.ensureCapacity(chunkSize);
    }

    @Override
    public C getColumnData() {
        return m_data;
    }

    @Override
    public boolean isMissing(final int index) {
        return m_data.isMissing(index);
    }

    @Override
    public int getMaxCapacity() {
        return m_data.getMaxCapacity();
    }

    @Override
    public int getNumValues() {
        return m_data.getNumValues();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMissingWithCause(final int i, final String value) {
        m_supplement.setString(i, value);
        m_hasSupplement = true;
    }

    @Override
    public String getMissingValueCause(final int index) {
        return (!m_hasSupplement || !m_data.isMissing(index)) ? null : m_supplement.getString(index);
    }

    @Override
    public void setNumValues(final int numValues) {
        // TODO only set if any value is set.
        if (m_hasSupplement) {
            ((CustomStructVector)m_struct).putChild("String Supplement Child", m_supplement.get());
            m_supplement.setNumValues(numValues);
        } else {
            // TODO release OK?
            m_supplement.release();
        }
        m_data.setNumValues(numValues);

        // TODO: needed?
        m_struct.setValueCount(numValues);
    }

    @Override
    public synchronized void release() {
        if (m_supplement != null) {
            m_supplement.release();
        }
        m_data.release();
        if (m_ref.decrementAndGet() == 0) {
            m_struct.close();
        }
    }

    @Override
    public synchronized void retain() {
        if (m_supplement != null) {
            m_supplement.retain();
        }
        m_data.retain();
        m_ref.incrementAndGet();
    }

    @Override
    public StructVector get() {
        return m_struct;
    }

    @Override
    public int sizeOf() {
        if (m_hasSupplement) {
            return (int)(m_supplement.sizeOf() + m_data.sizeOf() + m_struct.getValidityBuffer().capacity());
        } else {
            return (int)(m_data.sizeOf() + m_struct.getValidityBuffer().capacity());
        }
    }

    private static final class CustomStructVector extends StructVector {

        public CustomStructVector(final String name, final BufferAllocator allocator) {
            super(name, allocator, new FieldType(false, ArrowType.Struct.INSTANCE, null, null), null);
        }

        @Override
        public void putChild(final String name, final FieldVector vector) {
            super.putChild(name, vector);
        }

    }

}
