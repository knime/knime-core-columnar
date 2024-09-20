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
package org.knime.core.columnar.data;

import org.knime.core.columnar.ReadData;
import org.knime.core.columnar.WriteData;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 * Class holding {@link VarBinaryWriteData}, {@link VarBinaryReadData}, and {@link VarBinaryDataSpec} for data holding
 * byte arrays. The size of each byte array can vary between elements.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class VarBinaryData {

    private VarBinaryData() {
    }

    /**
     * {@link NullableWriteData} holding byte array elements.
     */
    public interface VarBinaryWriteData extends NullableWriteData {

        /**
         * Assigns a byte array value to the element at the given index (row). The contract is that values are only ever
         * set for ascending indices. It is the responsibility of the client calling this method to make sure that the
         * provided index is non-negative and smaller than the capacity of this {@link WriteData}.
         *
         * @param index the index (row) at which to set the byte array value
         * @param val the byte array value to set
         */
        void setBytes(int index, byte[] val);

        /**
         * Assigns an object to the element at the given index (row). The contract is that values are only ever set for
         * ascending indices. It is the responsibility of the client calling this method to make sure that the provied
         * index is non-negative and smaller than the capacity of this {@link WriteData}.
         *
         * @param <T> the type of object to set
         * @param index the index (row) at which to set the object
         * @param value the object to set
         * @param serializer capable of serializing the provided object
         */
        <T> void setObject(int index, T value, ObjectSerializer<T> serializer);

        @Override
        VarBinaryReadData close(int length);

        void setFrom(VarBinaryReadData data, int fromIndex, int toIndex);
    }

    /**
     * {@link NullableReadData} holding byte array elements.
     */
    public interface VarBinaryReadData extends NullableReadData {

        /**
         * Obtains the byte array value at the given index (row). It is the responsibility of the client calling this
         * method to make sure that the provided index is non-negative and smaller than the length of this
         * {@link ReadData}.
         *
         * @param index the index (row) at which to obtain the byte array element
         * @return the byte array element at the given index
         */
        byte[] getBytes(int index);

        /**
         * Obtains the object stored at the given index (row). It is the responsibility of the client calling this
         * method to make sure that the provided index is non-negative and smaller than the length of this
         * {@link ReadData}.
         *
         * @param <T> the type of read object
         * @param index the index (row) at which to obtain the object element
         * @param deserializer {@link ObjectDeserializer} capable of deserializing the object
         * @return the object at the given index (row)
         */
        <T> T getObject(int index, ObjectDeserializer<T> deserializer);
    }
}
