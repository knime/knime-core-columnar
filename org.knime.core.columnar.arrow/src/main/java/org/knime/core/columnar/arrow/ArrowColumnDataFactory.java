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
package org.knime.core.columnar.arrow;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.data.ArrowReadData;
import org.knime.core.columnar.arrow.data.ArrowWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;

import com.google.common.base.Preconditions;

/**
 * An {@link ArrowColumnDataFactory} is used for input and output of a specific Arrow column data type. Can be used to
 * create a new empty instance ({@link #getField(String, LongSupplier)} and
 * {@link #createWrite(FieldVector, LongSupplier, BufferAllocator, int)}), wrap vectors and dictionaries that have been
 * read from a file
 * ({@link #createRead(FieldVector, ArrowVectorNullCount, DictionaryProvider, ArrowColumnDataFactoryVersion)}) and get
 * vectors and dictionaries that need to be written to a file ({@link #getVector(NullableReadData)} and
 * {@link #getDictionaries(NullableReadData)}).
 * </p>
 * The static method {@link #createWrite(ArrowColumnDataFactory, String, BufferAllocator, int)} can be used to create
 * new {@link NullableWriteData} in just one step.
 * </p>
 * A factor has a {@link ArrowColumnDataFactoryVersion}. Make sure to update the version if
 * {@link #getVector(NullableReadData)} or {@link #getDictionaries(NullableReadData)} change. Implement
 * {@link #createRead(FieldVector, ArrowVectorNullCount, DictionaryProvider, ArrowColumnDataFactoryVersion)} such that
 * vectors and dictionaries from all prior versions can be wrapped in an appropriate {@link NullableReadData} object.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public interface ArrowColumnDataFactory {

    // ===================== Creating new ColumnWriteData =====================

    /**
     * Get the Arrow {@link Field} describing the vector of the data object.
     *
     * @param name the name of the field
     * @param dictionaryIdSupplier a supplier for dictionary ids. Make sure to use only dictionaries with ids coming
     *            from this supplier. Other ids might be used already in the parent data object.
     * @return the Arrow description for the vector type
     */
    Field getField(String name, LongSupplier dictionaryIdSupplier);

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
    ArrowWriteData createWrite(FieldVector vector, LongSupplier dictionaryIdSupplier, BufferAllocator allocator,
        int capacity);

    // ===================== Reading ColumnReadData ===========================

    /**
     * Wrap the given vector and dictionaries into a column data for reading.
     *
     * @param vector the vector holding some data
     * @param nullCount the null count of this vector (and its children)
     * @param provider a dictionary provider holding dictionaries that can be used
     * @param version the version the vector and dictionaries were written with
     * @return the {@link NullableReadData}
     * @throws IOException if the data cannot be loaded with the given version
     */
    ArrowReadData createRead(FieldVector vector, ArrowVectorNullCount nullCount, DictionaryProvider provider,
        ArrowColumnDataFactoryVersion version) throws IOException;

    // ===================== Getting data for writing =========================

    /**
     * @param data a column data holding some values
     * @return a {@link FieldVector} which should be written to disk
     */
    FieldVector getVector(NullableReadData data);

    /**
     * @param data a column data holding some values
     * @return dictionaries which should be written to disk
     */
    DictionaryProvider getDictionaries(NullableReadData data);

    /**
     * @return the current version used for getting the vectors and dictionaries. Not allowed to contain ','.
     */
    ArrowColumnDataFactoryVersion getVersion();

    // ===================== Utility methods ==================================

    /**
     * @return a new {@link LongSupplier} counting upwards and starting with 0
     */
    public static LongSupplier newDictionaryIdSupplier() {
        final AtomicLong id = new AtomicLong(0);
        return id::getAndIncrement;
    }

    /**
     * Utility method to create a new instance of {@link NullableWriteData} with a newly allocated vector. First creates a
     * field using {@link #getField(String, LongSupplier)}. Then allocates a new vector for this field and creates the
     * {@link NullableWriteData} using {@link #createWrite(FieldVector, LongSupplier, BufferAllocator, int)}.
     *
     * @param factory the factory to create the column data
     * @param name the name of the vector
     * @param allocator the allocator to allocate memory
     * @param capacity the capacity of the data
     * @return a new {@link NullableWriteData} object
     */
    @SuppressWarnings("resource") // Vector resource handled by ColumnWriteData
    public static NullableWriteData createWrite(final ArrowColumnDataFactory factory, final String name,
        final BufferAllocator allocator, final int capacity) {
        final Field field = factory.getField(name, newDictionaryIdSupplier());
        final FieldVector vector = field.createVector(allocator);
        return factory.createWrite(vector, newDictionaryIdSupplier(), allocator, capacity);
    }

    /** A class holding the null count for a vector and its children. */
    public static final class ArrowVectorNullCount {

        private int m_nullCount;

        private ArrowVectorNullCount[] m_children;

        /**
         * Create a {@link ArrowVectorNullCount} with the given null count for the vector and its children.
         *
         * @param nullCount the null count of the vector
         * @param children the {@link ArrowVectorNullCount null counts} for the child vectors
         */
        ArrowVectorNullCount(final int nullCount, final ArrowVectorNullCount[] children) {
            Preconditions.checkNotNull(children);
            m_nullCount = nullCount;
            m_children = children;
        }

        /** @return the null count for the vector */
        public int getNullCount() {
            return m_nullCount;
        }

        /**
         * @param index the index of the child
         * @return the null count for the child vector
         */
        public ArrowVectorNullCount getChild(final int index) {
            return m_children[index];
        }
    }

    /**
     * @return How many bytes will be allocated per element. The prefix initial indicates that this size does not
     *         necessarily reflect the final size, as string and binary blobs can have variable lengths.
     */
    int initialNumBytesPerElement();
}
