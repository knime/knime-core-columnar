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
 *   Feb 3, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import java.io.IOException;
import java.util.function.Consumer;

import org.knime.core.data.columnar.table.ResourceLeakDetector.Finalizer;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowRead;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.cursor.LookaheadCursor;

/**
 * Utility class providing decorators for various interfaces whose instance might be resource sensitive and need to be
 * tracked by the ResourceLeakDetector.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class CursorsWithFinalizer {

    static <A> LookaheadCursor<A> lookaheadCursor(final LookaheadCursor<A> cursor,
        final Consumer<Finalizer> finalizerRegistry) {
        var cursorWithFinalizer = new LookaheadCursorWithFinalizer<>(cursor);
        finalizerRegistry.accept(cursorWithFinalizer.m_finalizer);
        return cursorWithFinalizer;
    }

    static <A> Cursor<A> cursor(final Cursor<A> cursor, final Consumer<Finalizer> finalizerRegistry) {
        if (cursor instanceof LookaheadCursor) {
            return lookaheadCursor((LookaheadCursor<A>)cursor, finalizerRegistry);
        } else {
            var cursorWithFinalizer = new CursorWithFinalizer<>(cursor);
            finalizerRegistry.accept(cursorWithFinalizer.m_finalizer);
            return cursorWithFinalizer;
        }
    }

    static RowCursor rowCursor(final RowCursor rowCursor, final Consumer<Finalizer> finalizerRegistry) {
        var cursorWithFinalizer = new RowCursorWithFinalizer(rowCursor);
        finalizerRegistry.accept(cursorWithFinalizer.m_finalizer);
        return cursorWithFinalizer;
    }

    private static final class CursorWithFinalizer<A> implements Cursor<A> {

        private final Cursor<A> m_delegate;

        private final Finalizer m_finalizer;

        CursorWithFinalizer(final Cursor<A> delegate) {
            m_delegate = delegate;
            m_finalizer = ResourceLeakDetector.getInstance().createFinalizer(this, delegate);
        }

        @Override
        public A access() {
            return m_delegate.access();
        }

        @Override
        public boolean forward() {
            return m_delegate.forward();
        }

        @Override
        public void close() throws IOException {
            m_delegate.close();
            m_finalizer.close();
        }

    }

    private static final class LookaheadCursorWithFinalizer<A> implements LookaheadCursor<A> {

        private final LookaheadCursor<A> m_delegate;

        private final Finalizer m_finalizer;

        LookaheadCursorWithFinalizer(final LookaheadCursor<A> delegate) {
            m_delegate = delegate;
            m_finalizer = ResourceLeakDetector.getInstance().createFinalizer(this, delegate);
        }

        @Override
        public A access() {
            return m_delegate.access();
        }

        @Override
        public A pinAccess() {
            return m_delegate.pinAccess();
        }

        @Override
        public boolean forward() {
            return m_delegate.forward();
        }

        @Override
        public boolean canForward() {
            return m_delegate.canForward();
        }

        @Override
        public void close() throws IOException {
            m_delegate.close();
            m_finalizer.close();
        }

    }

    private static final class RowCursorWithFinalizer implements RowCursor {

        private final RowCursor m_delegate;

        private final Finalizer m_finalizer;

        RowCursorWithFinalizer(final RowCursor delegate) {
            m_delegate = delegate;
            m_finalizer = ResourceLeakDetector.getInstance().createFinalizer(this, delegate);
        }

        @Override
        public RowRead forward() {
            return m_delegate.forward();
        }

        @Override
        public boolean canForward() {
            return m_delegate.canForward();
        }

        @Override
        public int getNumColumns() {
            return m_delegate.getNumColumns();
        }

        @Override
        public void close() {
            m_delegate.close();
            m_finalizer.close();
        }

    }

    private CursorsWithFinalizer() {

    }
}
