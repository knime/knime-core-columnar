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
 *   Oct 25, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.data.dictencoding;

import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.knime.core.columnar.data.ListData;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;

/**
 * {@link DecoratedListWriteData} and {@link DecoratedListReadData} allow to decorate the individual data objects inside
 * a {@link ListData}. This can e.g. be used to inject a global key generator for dictionary encoding.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
final class DecoratedListData {

    private DecoratedListData() {

    }

    static final class DecoratedListWriteData implements ListWriteData {

        private final ListWriteData m_delegate;

        private final Function<NullableWriteData, NullableWriteData> m_decorator;

        DecoratedListWriteData(final ListWriteData delegate, final UnaryOperator<NullableWriteData> decorator) {
            m_delegate = delegate;
            m_decorator = decorator;
        }

        @Override
        public void expand(final int minimumCapacity) {
            m_delegate.expand(minimumCapacity);
        }

        @Override
        public void setMissing(final int index) {
            m_delegate.setMissing(index);
        }

        @Override
        public int capacity() {
            return m_delegate.capacity();
        }

        @Override
        public void retain() {
            m_delegate.retain();
        }

        @SuppressWarnings("unchecked")
        @Override
        public <C extends NullableWriteData> C createWriteData(final int index, final int size) {
            return (C)m_decorator.apply(m_delegate.createWriteData(index, size));
        }

        @Override
        public void release() {
            m_delegate.release();
        }

        @Override
        public long sizeOf() {
            return m_delegate.sizeOf();
        }

        @Override
        public ListReadData close(final int length) {
            return m_delegate.close(length);
        }

    }

    static final class DecoratedListReadData implements ListReadData {

        private final ListReadData m_delegate;

        private final UnaryOperator<NullableReadData> m_decorator;

        DecoratedListReadData(final ListReadData delegate, final UnaryOperator<NullableReadData> decorator) {
            m_delegate = delegate;
            m_decorator = decorator;
        }

        @Override
        public int length() {
            return m_delegate.length();
        }

        @Override
        public boolean isMissing(final int index) {
            return m_delegate.isMissing(index);
        }

        @Override
        public void retain() {
            m_delegate.retain();
        }

        @Override
        public void release() {
            m_delegate.release();
        }

        @Override
        public long sizeOf() {
            return m_delegate.sizeOf();
        }

        @SuppressWarnings("unchecked")
        @Override
        public <C extends NullableReadData> C createReadData(final int index) {
            return (C)m_decorator.apply(m_delegate.createReadData(index));
        }

    }

}
