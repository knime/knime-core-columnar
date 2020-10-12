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

@SuppressWarnings("javadoc")
public final class ObjectData {

    private ObjectData() {
    }

    public static final class ObjectDataSpec<T> implements ColumnDataSpec {
        private final ObjectDataSerializer<T> m_serializer;

        private final boolean m_dictEncoded;

        public ObjectDataSpec(final ObjectDataSerializer<T> serializer, final boolean dictEncoded) {
            m_serializer = serializer;
            m_dictEncoded = dictEncoded;
        }

        @Override
        public <R> R accept(final Mapper<R> v) {
            return v.visit(this);
        }

        public ObjectDataSerializer<T> getSerializer() {
            return m_serializer;
        }

        // TODO add more encoding options (AUTO, ENFORCE, NONE and config params: dict size etc).
        public boolean isDictEncoded() {
            return m_dictEncoded;
        }
    }

    public interface ObjectDataSerializer<T> {
        byte[] serialize(T obj);

        T deserialize(byte[] bytes);
    }

    public interface ObjectReadData<T> extends ColumnReadData {
        T getObject(int index);
    }

    public interface ObjectWriteData<T> extends ColumnWriteData {
        void setObject(int index, T obj);

        @Override
        ObjectReadData<T> close(int length);
    }
}
