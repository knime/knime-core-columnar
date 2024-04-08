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
 *   Dec 20, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.data;

/**
 * Provides interfaces, base classes and utility methods for data objects that decorate other data objects.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class DecoratingData {

    /**
     * Recursively unpacks the provided data instance if it is a {@link DecoratingNullableReadData} object. The
     * recursion stops once {@link DecoratingNullableReadData#getReadDelegate()} does not return a
     * {@link DecoratingNullableReadData} object.
     *
     * @param data to unpack
     * @return the inner most delegate (which can be data itself if data is not a DecoratingNullableReadData)
     */
    public static NullableReadData unpack(NullableReadData data) {
        while (data instanceof DecoratingNullableReadData decoratingData) {
            data = decoratingData.getReadDelegate();
        }
        return data;
    }

    /**
     * Recursively unpacks the provided data instance if it is a {@link DecoratingNullableWriteData} object. The
     * recursion stops once {@link DecoratingNullableWriteData#getWriteDelegate()} does not return a
     * {@link DecoratingNullableWriteData} object.
     *
     * @param data to unpack
     * @return the inner most delegate (which can be data itself if data is not a DecoratingNullableWriteData)
     */
    public static NullableWriteData unpack(NullableWriteData data) {
        while (data instanceof DecoratingNullableWriteData decoratingData) {
            data = decoratingData.getWriteDelegate();
        }
        return data;
    }

    /**
     * {@link NullableReadData} that decorates another {@link NullableReadData}.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public interface DecoratingNullableReadData extends NullableReadData {

        /**
         * @return the decorated data object
         */
        NullableReadData getReadDelegate();
    }

    /**
     * A {@link NullableWriteData} that decorates another {@link NullableWriteData}.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public interface DecoratingNullableWriteData extends NullableWriteData {

        /**
         * @return the decorated data object
         */
        NullableWriteData getWriteDelegate();
    }

    /**
     * Base implementations for a {@link DecoratingNullableReadData} that delegates all methods calls to the delegate.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     * @param <D> the type of the decorated data object
     */
    public abstract static class AbstractDecoratingNullableReadData<D extends NullableReadData>
        implements DecoratingNullableReadData {

        /**
         * The decorated data object.
         */
        protected final D m_delegate;

        /**
         * Creates an instance that decorates the provided delegate.
         *
         * @param delegate to decorate
         */
        protected AbstractDecoratingNullableReadData(final D delegate) {
            m_delegate = delegate;
        }

        @Override
        public D getReadDelegate() {
            return m_delegate;
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
    }

    /**
     * Base implementation for {@link DecoratingNullableWriteData} implementations that delegates all method calls to
     * the delegate.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     * @param <W> the type of the decorated data object
     */
    public abstract static class AbstractDecoratingNullableWriteData<W extends NullableWriteData>
        implements DecoratingNullableWriteData {

        /**
         * The decorated data object.
         */
        protected final W m_delegate;

        /**
         * Constructs an instance with the provided delegate.
         *
         * @param delegate the object to decorate
         */
        protected AbstractDecoratingNullableWriteData(final W delegate) {
            m_delegate = delegate;
        }

        @Override
        public W getWriteDelegate() {
            return m_delegate;
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

        @Override
        public NullableReadData close(final int length) {
            return m_delegate.close(length);
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
        public long usedSizeFor(final int numElements) {
            return m_delegate.usedSizeFor(numElements);
        }
    }

}
