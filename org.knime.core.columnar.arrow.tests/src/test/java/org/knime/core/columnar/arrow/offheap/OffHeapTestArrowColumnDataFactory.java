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
 *   11 Dec 2025 (pietzsch): created
 */
package org.knime.core.columnar.arrow.offheap;

import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowWriteData;
import org.knime.core.columnar.data.NullableWriteData;

/**
 * Provides overrides that allow to inject {@code BufferAllocator} and {@code dictionaryIdSupplier}. This is needed for
 * creating {@link Dictionary} vectors (which we no longer write in new data, but still want to be able to create for
 * testing).
 */
interface OffHeapTestArrowColumnDataFactory extends OffHeapArrowColumnDataFactory {

    @Override
    default Field getField(final String name) {
        return getField(name, null);
    }

    @Override
    default OffHeapArrowWriteData createWrite(final FieldVector vector, final int capacity) {
        return createWrite(vector, null, null, capacity);
    }

    /**
     * Get the Arrow {@link Field} describing the vector of the data object.
     *
     * @param name the name of the field
     * @param dictionaryIdSupplier a supplier for dictionary ids. Make sure to use only dictionaries with ids coming
     *            from this supplier. Other ids might be used already in the parent data object.
     * @return the Arrow description for the vector type
     */
    Field getField(final String name, final LongSupplier dictionaryIdSupplier);

    /**
     * Create an empty column data for writing.
     *
     * @param vector the empty vector with the type according to {@link #getField(String, LongSupplier)}.
     * @param dictionaryIdSupplier a supplier for dictionary ids. Make sure to use only dictionaries with ids coming
     *            from this supplier. Other ids might be used already in the parent data object. Also take as many
     *            dictionary ids from the supplier as in {@link #getField(String, LongSupplier)}.
     * @param allocator the allocator used for memory allocation of dictionary vectors
     * @param capacity the initial capacity to allocate
     * @return the {@link NullableWriteData}
     */
    OffHeapArrowWriteData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
        final BufferAllocator allocator, final int capacity);
}
