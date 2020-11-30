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
 *   Oct 2, 2020 (dietzc): created
 */
package org.knime.core.columnar.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Class holding {@link ObjectReadData} and {@link ObjectWriteData} which describe data chunks containing objects of a
 * generic type T.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ObjectData {

    private ObjectData() {
    }

    /**
     * A {@link ColumnReadData} holding objects of type T at each index.
     *
     * @param <T> type of the elements
     */
    public interface ObjectReadData<T> extends ColumnReadData {

        /**
         * @param index the index in the chunk
         * @return the object at the index
         */
        T getObject(int index);
    }

    /**
     * A {@link ColumnWriteData} holding objects of type T at each index.
     *
     * @param <T> type of the elements
     */
    public interface ObjectWriteData<T> extends ColumnWriteData {

        /**
         * Set the element at the index to the given object.
         *
         * @param index the index in the chunk
         * @param obj the object
         */
        void setObject(int index, T obj);

        @Override
        ObjectReadData<T> close(int length);
    }

    /**
     * Spec for {@link ObjectReadData} and {@link ObjectWriteData} which can hold arbitrary objects. A
     * {@link ObjectDataSerializer} is used to serialize and deserialize the object using a {@link DataInput} and
     * {@link DataOutput}.
     *
     * @param <T> type of the objects
     */
    public static final class GenericObjectDataSpec<T> implements ColumnDataSpec {
        private final ObjectDataSerializer<T> m_serializer;

        private final boolean m_dictEncoded;

        /**
         * Create a spec for object data which can object of the type T.
         *
         * @param serializer a serializer which can read object of type T from a {@link DataInput} and write objects of
         *            type T to a {@link DataOutput}.
         * @param dictEncoded if the data is dict encoded.
         */
        public GenericObjectDataSpec(final ObjectDataSerializer<T> serializer, final boolean dictEncoded) {
            m_serializer = serializer;
            m_dictEncoded = dictEncoded;
        }

        @Override
        public <R> R accept(final Mapper<R> v) {
            return v.visit(this);
        }

        /**
         * @return the serializer which can read object of type T from a {@link DataInput} and write objects of type T
         *         to a {@link DataOutput}.
         */
        public ObjectDataSerializer<T> getSerializer() {
            return m_serializer;
        }

        /**
         * @return if the data is dictionary encoded
         */
        // TODO add more encoding options (AUTO, ENFORCE, NONE and config params: dict size etc).
        public boolean isDictEncoded() {
            return m_dictEncoded;
        }
    }

    /**
     * A serializer which can serialize data of type T to a {@link DataOutput} and deserialize it from a
     * {@link DataInput}.
     *
     * @param <T> type of the data
     */
    public interface ObjectDataSerializer<T> {

        /**
         * Serialize the object to the {@link DataOutput}.
         *
         * @param obj the object to serialize
         * @param output the {@link DataOutput} to serialize to
         * @throws IOException if the serialization fails
         */
        void serialize(T obj, DataOutput output) throws IOException;

        /**
         * Deserialize the object from the {@link DataInput}.
         *
         * @param input the {@link DataInput} to get the data from
         * @return the deserialized object
         * @throws IOException if the deserialization fails
         */
        T deserialize(DataInput input) throws IOException;
    }
}
