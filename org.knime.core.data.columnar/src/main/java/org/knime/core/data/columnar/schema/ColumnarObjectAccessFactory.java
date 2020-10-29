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
package org.knime.core.data.columnar.schema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.knime.core.columnar.ColumnDataIndex;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.data.ObjectData.ObjectDataSerializer;
import org.knime.core.columnar.data.ObjectData.ObjectDataSpec;
import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.data.ObjectData.ObjectWriteData;
import org.knime.core.data.v2.access.ObjectAccess.ObjectAccessSpec;
import org.knime.core.data.v2.access.ObjectAccess.ObjectReadAccess;
import org.knime.core.data.v2.access.ObjectAccess.ObjectSerializer;
import org.knime.core.data.v2.access.ObjectAccess.ObjectWriteAccess;
import org.knime.core.data.v2.access.ReadAccess;
import org.knime.core.data.v2.access.WriteAccess;

/**
 * A ColumnarValueFactory implementation wrapping {@link ColumnReadData} / {@link ColumnWriteData} as {@link ReadAccess}
 * / {@link WriteAccess}
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
final class ColumnarObjectAccessFactory<T> implements ColumnarAccessFactory<ObjectReadData<T>, //
        ObjectReadAccess<T>, ObjectWriteData<T>, ObjectWriteAccess<T>> {

    private final ObjectDataSpec<T> m_spec;

    public ColumnarObjectAccessFactory(final ObjectAccessSpec<T> spec) {
        m_spec = wrap(spec);
    }

    private ObjectDataSpec<T> wrap(final ObjectAccessSpec<T> spec) {
        return new ObjectDataSpec<>(new DefaultObjectDataSerializer<>(spec.getSerializer()), spec.isDictEncoded());
    }

    @Override
    public final ObjectDataSpec<T> getColumnDataSpec() {
        return m_spec;
    }

    @Override
    public final ObjectWriteAccess<T> createWriteAccess(final ObjectWriteData<T> data, final ColumnDataIndex index) {
        return new ColumnarObjectWriteAccess<T>(data, index);
    }

    @Override
    public final ObjectReadAccess<T> createReadAccess(final ObjectReadData<T> data, final ColumnDataIndex index) {
        return new ColumnarObjectReadAccess<T>(data, index);
    }

    private static final class DefaultObjectDataSerializer<T> implements ObjectDataSerializer<T> {

        private final ObjectSerializer<T> m_serializer;

        public DefaultObjectDataSerializer(final ObjectSerializer<T> serializer) {
            m_serializer = serializer;
        }

        @Override
        public void serialize(final T obj, final DataOutput output) throws IOException {
            m_serializer.serialize(obj, output);
        }

        @Override
        public T deserialize(final DataInput input) throws IOException {
            return m_serializer.deserialize(input);
        }

    }

    private static final class ColumnarObjectReadAccess<T> implements ObjectReadAccess<T> {
        private final ColumnDataIndex m_index;

        private final ObjectReadData<T> m_data;

        public ColumnarObjectReadAccess(final ObjectReadData<T> data, final ColumnDataIndex index) {
            m_data = data;
            m_index = index;
        }

        @Override
        public T getObject() {
            return m_data.getObject(m_index.getIndex());
        }

        @Override
        public boolean isMissing() {
            return m_data.isMissing(m_index.getIndex());
        }
    }

    private static final class ColumnarObjectWriteAccess<T> implements ObjectWriteAccess<T> {
        private final ColumnDataIndex m_index;

        private final ObjectWriteData<T> m_data;

        public ColumnarObjectWriteAccess(final ObjectWriteData<T> data, final ColumnDataIndex index) {
            m_data = data;
            m_index = index;
        }

        @Override
        public void setObject(final T object) {
            m_data.setObject(m_index.getIndex(), object);
        }

        @Override
        public void setMissing() {
            m_data.setMissing(m_index.getIndex());
        }
    }
}
