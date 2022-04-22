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
 *   Apr 11, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import java.util.NoSuchElementException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.knime.core.data.DataRow;
import org.knime.core.data.container.CloseableRowIterator;

/**
 * RowIterator that uses a thread pool to asynchronously prefetch rows.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class PrefetchingRowIterator extends CloseableRowIterator {

    private static final ThreadPoolExecutor EXECUTOR =
        new ThreadPoolExecutor(4, 8, 10L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new ThreadFactory() {
            private final AtomicLong m_threadCount = new AtomicLong();

            @Override
            public Thread newThread(final Runnable r) {
                return new Thread(r, "KNIME-Columnar-Iterator-Prefetching-Thread-" + m_threadCount.getAndIncrement());
            }
        });

    private static final int BATCH_SIZE = 1024;

    private final CloseableRowIterator m_source;

    private final BlockingDeque<DataRow[]> m_rowQueue = new LinkedBlockingDeque<>();

    private DataRow m_currentRow;

    private final AtomicBoolean m_closed = new AtomicBoolean(false);

    private int m_currentIndex = -1;

    private DataRow[] m_currentRows = null;

    public PrefetchingRowIterator(final CloseableRowIterator source) {
        m_source = source;
        startPrefetching();
    }

    private void startPrefetching() {
        EXECUTOR.execute(this::prefetchNextBatch);
    }

    private void prefetchNextBatch() {
        final DataRow[] rows = new DataRow[BATCH_SIZE];
        for (int i = 0; i < BATCH_SIZE && m_source.hasNext(); i++) {//NOSONAR
            if (m_closed.get()) {
                // row iterator is closed -> stop prefetching
                return;
            }
            rows[i] = m_source.next();
        }
        m_rowQueue.add(rows);
        if (m_source.hasNext()) {
            startPrefetching();
        } else {
            m_rowQueue.add(new DataRow[0]);
        }
    }

    @Override
    public boolean hasNext() {
        if (m_currentRow == null) {
            m_currentRow = getNextRow();
        }
        return m_currentRow != null;
    }

    private DataRow getNextRow() {
        try {
            if (m_currentRows == null || ++m_currentIndex == m_currentRows.length) {
                m_currentRows = m_rowQueue.takeFirst();

                if (m_currentRows.length == 0) {
                    return null;
                } else {
                    m_currentIndex = 0;
                }
            }
            return m_currentRows[m_currentIndex];
        } catch (InterruptedException ex) {
            ex.printStackTrace();
            return null;
        }
    }

    @Override
    public DataRow next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        } else {
            var row = m_currentRow;
            m_currentRow = null;
            return row;
        }
    }

    @Override
    public void close() {
        m_closed.set(true);
        m_rowQueue.clear();
        m_source.close();
    }
}
