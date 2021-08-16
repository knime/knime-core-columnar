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
 *   Aug 3, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * A {@link Supplier} that keeps track of the objects it created and closes them all if its own {@link #close()} method
 * is invoked.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @param <T> the type of supplied objects
 */
public final class CloseableTrackingSupplier<T extends Closeable> implements Closeable, Supplier<T> {

    private final Supplier<T> m_supplier;

    private final List<T> m_trackedCloseables = new ArrayList<>();

    private boolean m_isClosed;

    /**
     * Constructor.
     *
     * @param supplier that provides the Closeables
     */
    public CloseableTrackingSupplier(final Supplier<T> supplier) {
        m_supplier = supplier;
    }

    @Override
    public synchronized void close() throws IOException {
        m_isClosed = true;
        final List<IOException> exceptions = new ArrayList<>();
        for (T t : m_trackedCloseables) {
            try {
                t.close();
            } catch (IOException ex) {
                exceptions.add(ex);
            }
        }
        if (!exceptions.isEmpty()) {
            // TODO use IOExceptionList once org.apache.commons.io >= 2.7.0 is available in the nightlies
            throw exceptions.get(0);
        }
    }

    @Override
    public synchronized T get() {
        if (m_isClosed) {
            throw new IllegalStateException("Already closed.");
        }
        final var t = m_supplier.get();
        m_trackedCloseables.add(t);
        return t;
    }

}