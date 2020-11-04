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
 *   Sep 24, 2020 (dietzc): created
 */
package org.knime.core.columnar.arrow.data;

import java.io.IOException;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.VoidData.VoidReadData;
import org.knime.core.columnar.data.VoidData.VoidWriteData;

/**
 * Arrow implementation of {@link VoidWriteData} and {@link VoidReadData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowVoidData implements VoidWriteData, ArrowWriteData, VoidReadData, ArrowReadData {

    private int m_capacity;

    private final NullVector m_vector;

    private final int m_length;

    private ArrowVoidData(final NullVector vector, final int capacity) {
        m_vector = vector;
        m_capacity = capacity;
        m_length = vector.getValueCount();
    }

    private ArrowVoidData(final NullVector vector, final int capacity, final int length) {
        m_vector = vector;
        m_capacity = capacity;
        m_length = length;
    }

    @Override
    public int capacity() {
        return m_capacity;
    }

    @Override
    public int sizeOf() {
        return 0;
    }

    @Override
    public void expand(final int minimumCapacity) {
        m_capacity = minimumCapacity;
    }

    @Override
    public ArrowVoidData close(final int length) {
        m_vector.setValueCount(length);
        return new ArrowVoidData(m_vector, m_capacity);
    }

    @Override
    public int length() {
        return m_length;
    }

    @Override
    public void release() {
        // Nothing to release
    }

    @Override
    public void retain() {
        // Nothing to retain
    }

    @Override
    public ArrowWriteData slice(final int start) {
        return new ArrowVoidData(m_vector, m_capacity);
    }

    @Override
    public ArrowVoidData slice(final int start, final int length) {
        return new ArrowVoidData(m_vector, m_capacity, length);
    }

    /** Implementation of {@link ArrowColumnDataFactory} for {@link ArrowVoidData} */
    public static final class ArrowVoidDataFactory implements ArrowColumnDataFactory {

        private static final ArrowColumnDataFactoryVersion CURRENT_VERSION = ArrowColumnDataFactoryVersion.version(0);

        /** Singleton instance of {@link ArrowVoidDataFactory} */
        public static final ArrowVoidDataFactory INSTANCE = new ArrowVoidDataFactory();

        private ArrowVoidDataFactory() {
            // Singleton
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return new Field(name, new FieldType(false, MinorType.NULL.getType(), null), null);
        }

        @Override
        public ArrowVoidData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            return new ArrowVoidData((NullVector)vector, capacity);
        }

        @Override
        public ArrowVoidData createRead(final FieldVector vector, final DictionaryProvider provider,
            final ArrowColumnDataFactoryVersion version) throws IOException {
            if (CURRENT_VERSION.equals(version)) {
                return new ArrowVoidData((NullVector)vector, vector.getValueCapacity());
            } else {
                throw new IOException("Cannot read ArrowVoidData with version " + version + ". Current version: "
                    + CURRENT_VERSION + ".");
            }
        }

        @Override
        public FieldVector getVector(final ColumnReadData data) {
            return ((ArrowVoidData)data).m_vector;
        }

        @Override
        public DictionaryProvider getDictionaries(final ColumnReadData data) {
            return null;
        }

        @Override
        public ArrowColumnDataFactoryVersion getVersion() {
            return CURRENT_VERSION;
        }
    }
}
