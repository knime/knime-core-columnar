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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.data.NullableWithCauseData;
import org.knime.core.columnar.phantom.CloseableCloser;

public class ArrowNullableWithCauseData<C extends ArrowData<?>>
    implements NullableWithCauseData<C>, ArrowData<FieldVector> {

    public final static String CFG_HAS_MISSING_WITH_CAUSE = "HAS_MISSING_WITH_CAUSE";

    private AtomicInteger m_ref = new AtomicInteger(1);

    private ArrowVarCharData m_supplement;

    private C m_data;

    CloseableCloser m_vectorCloser;

    private FieldVector m_vector;

    public ArrowNullableWithCauseData(final BufferAllocator allocator, final C data) {
        m_data = data;
    }

    public ArrowNullableWithCauseData(final C data, final StructVector vector) {
        m_vector = vector;
        m_supplement = new ArrowVarCharData((VarCharVector)vector.getChildByOrdinal(1));
    }

    public ArrowNullableWithCauseData(final C data) {
        m_data = data;
        m_vector = data.get();
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
    public void setMissingWithCause(final int index, final String value) {
        if (m_supplement == null) {
            m_supplement = new ArrowVarCharData(m_data.get().getAllocator());
            m_supplement.ensureCapacity(m_data.getMaxCapacity());
        }
        setMissing(index);
        m_supplement.setString(index, value);
    }

    @Override
    public String getMissingValueCause(final int index) {
        return (!hasSupplement() || !m_data.isMissing(index)) ? null : m_supplement.getString(index);
    }

    @Override
    public void setNumValues(final int numValues) {
        // TODO only set if any value is set.
        m_data.setNumValues(numValues);
        if (hasSupplement()) {
            CustomStructVector struct = new CustomStructVector("String Supplement", m_data.get().getAllocator());
            struct.putChild("Data", m_data.get());
            struct.putChild("String Supplement Child", m_supplement.get());
            struct.setValueCount(numValues);
            m_vector = struct;
        } else {
            m_vector = m_data.get();
        }
    }

    @Override
    public synchronized void release() {
        if (m_ref.decrementAndGet() == 0) {
            if (hasSupplement()) {
                m_supplement.release();

                if (m_vector != null) {
                    m_vector.clear();
                }
            }
            m_data.release();
        }
    }

    @Override
    public synchronized void retain() {
        m_ref.incrementAndGet();
    }

    @Override
    public FieldVector get() {
        return m_vector;
    }

    @Override
    public int sizeOf() {
        if (hasSupplement()) {
            return (int)(m_supplement.sizeOf() + m_data.sizeOf() + m_vector.getValidityBuffer().capacity());
        } else {
            return (m_data.sizeOf());
        }
    }

    private boolean hasSupplement() {
        return m_supplement != null;
    }

    private static final class CustomStructVector extends StructVector {

        private static final Map<String, String> METADATA = new HashMap<>();
        static {
            METADATA.put(CFG_HAS_MISSING_WITH_CAUSE, "true");
        }

        public CustomStructVector(final String name, final BufferAllocator allocator) {
            super(name, allocator, new FieldType(false, ArrowType.Struct.INSTANCE, null, METADATA), null);
        }

        @Override
        public void putChild(final String name, final FieldVector vector) {
            super.putChild(name, vector);
        }

    }

}
