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
 *   Dec 3, 2024 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import java.io.IOException;
import java.util.Collections;
import java.util.function.LongSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;

/**
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class OnHeapListData {

    private OnHeapListData() {
    }

    public static final class OnHeapListWriteData extends AbstractReferencedData
        implements ListWriteData, ArrowWriteData {

        private ArrowWriteData m_data;

        private int[] m_offsets;

        private ValidityBuffer m_validity;

        private OnHeapListWriteData(final int capacity, final ArrowWriteData data) {
            m_data = data;
            m_offsets = new int[capacity + 1];
            m_validity = new ValidityBuffer(capacity);
        }

        @Override
        public void setMissing(final int index) {
            m_validity.set(index, false);
        }

        @Override
        public void expand(final int minimumCapacity) {
            setNumElements(minimumCapacity);
        }

        @Override
        public int capacity() {
            return m_offsets.length - 1;
        }

        @Override
        public long usedSizeFor(final int numElements) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public long sizeOf() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public OnHeapListReadData close(final int length) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public <C extends NullableWriteData> C createWriteData(final int index, final int size) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        protected void closeResources() {
            // TODO Auto-generated method stub

        }

        /**
         * Expand or shrink the data to the given size.
         *
         * @param numElements the new size of the data
         */
        private void setNumElements(final int numElements) {
            m_validity.setNumElements(numElements);

            var newOffsets = new int[numElements + 1];
            System.arraycopy(m_offsets, 0, newOffsets, 0, Math.min(m_offsets.length, numElements));
            m_offsets = newOffsets;
        }
    }

    public static final class OnHeapListReadData extends AbstractReferencedData implements ListReadData, ArrowReadData {

        private final ArrowReadData m_data;

        @Override
        public boolean isMissing(final int index) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public int length() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public long sizeOf() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public <C extends NullableReadData> C createReadData(final int index) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        protected void closeResources() {
            // TODO Auto-generated method stub

        }
    }

    public static final class Factory extends AbstractArrowColumnDataFactory {

        private static final int CURRENT_VERSION = 0;

        private final ArrowColumnDataFactory m_inner;

        protected Factory(final ArrowColumnDataFactory inner) {
            super(ArrowColumnDataFactoryVersion.version(CURRENT_VERSION));
            m_inner = inner;
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            final Field data = m_inner.getField("listData", dictionaryIdSupplier);
            return new Field(name, new FieldType(true, MinorType.LIST.getType(), null),
                Collections.singletonList(data));
        }

        @Override
        public ArrowWriteData createWrite(final int capacity) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void copyToVector(final NullableReadData data, final FieldVector vector) {
            ArrowWriteData childData = null;
            long[] offsets = null;
            ValidityBuffer validity = null;

            var d = (OnHeapListReadData)data;
            var v = (ListVector)vector;

            // TODO is this comment still valid?
            // Note: we must do that before creating the inner data because "allocateNew" overwrites the allocation for
            // the child vector
            v.setInitialCapacity(d.length());
            v.allocateNew();


            // TODO copy offsets and validity

            m_inner.copyToVector(d.m_data, v.getDataVector());

        }

        @Override
        public int initialNumBytesPerElement() {
            // TODO Auto-generated method stub
            return 0;
        }
    }
}
