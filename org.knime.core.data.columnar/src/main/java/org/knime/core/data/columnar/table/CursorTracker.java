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
 *   Feb 4, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import org.knime.core.data.columnar.table.ResourceLeakDetector.Finalizer;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.cursor.LookaheadCursor;

import com.google.common.cache.CacheBuilder;

/**
 * Tracks {@link Closeable} cursors emitted by tables and makes sure that their resources are properly released if
 * <ul>
 * <li> They are no longer referenced but haven't been closed, yet.
 * <li> The emitting table is closed before the cursors released their resources.
 * </ul>
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class CursorTracker<C extends Closeable> implements Closeable {

    private final UnaryOperator<C> m_finalizerFactory;

    private final Set<Finalizer> m_openFinalizers;

    private CursorTracker(final UnaryOperator<C> finalizerFactory, final Set<Finalizer> trackedCursors) {
        m_finalizerFactory = finalizerFactory;
        m_openFinalizers = trackedCursors;
    }

    @SuppressWarnings("unchecked")
    private static Set<Finalizer> finalizerSet() {
        @SuppressWarnings("rawtypes")
        Map map = CacheBuilder.newBuilder().weakKeys().build().asMap();
        return Collections.newSetFromMap(map);
    }

    static <A> CursorTracker<Cursor<A>> createCursorTracker() {
        Set<Finalizer> openFinalizers = finalizerSet();
        return new CursorTracker<>(c -> CursorsWithFinalizer.cursor(c, openFinalizers::add), openFinalizers);
    }

    static <A> CursorTracker<LookaheadCursor<A>> createLookaheadCursorTracker() {
        Set<Finalizer> openFinalizers = finalizerSet();
        return new CursorTracker<>(c -> CursorsWithFinalizer.lookaheadCursor(c, openFinalizers::add), openFinalizers);
    }

    static CursorTracker<RowCursor> createRowCursorTracker() {
        Set<Finalizer> openFinalizers = finalizerSet();
        return new CursorTracker<>(c -> CursorsWithFinalizer.rowCursor(c, openFinalizers::add), openFinalizers);
    }

    @SuppressWarnings("resource")
    C createTrackedCursor(final Supplier<C> cursorSupplier) {
        return m_finalizerFactory.apply(cursorSupplier.get());
    }

    @Override
    public void close() throws IOException {

        var exceptions = new ArrayList<Exception>();
        for (var finalizer : m_openFinalizers) {
            finalizer.releaseResourcesAndLogOutput();
        }
        m_openFinalizers.clear();
        if (!exceptions.isEmpty()) {
            throw new IOException("Exception while closing tracked cursor.", exceptions.get(0));
        }
    }
}
