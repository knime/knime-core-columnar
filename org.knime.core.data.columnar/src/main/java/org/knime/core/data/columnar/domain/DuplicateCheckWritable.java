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
package org.knime.core.data.columnar.domain;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.knime.core.columnar.ColumnarSchema;
import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.DuplicateChecker;
import org.knime.core.util.DuplicateKeyException;

/**
 * A {@link BatchWritable} that checks for duplicates among row keys. Only required during writing.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class DuplicateCheckWritable implements BatchWritable {

    private final class DuplicateCheckBatchWriter implements BatchWriter {

        private final BatchWriter m_writerDelegate;

        private CompletableFuture<Void> m_future = CompletableFuture.completedFuture(null);

        DuplicateCheckBatchWriter(final BatchWriter delegate) {
            m_writerDelegate = delegate;
        }

        @Override
        public WriteBatch create(final int capacity) {
            return m_writerDelegate.create(capacity);
        }

        @SuppressWarnings("unchecked")
        @Override
        public synchronized void write(final ReadBatch batch) throws IOException {

            m_writerDelegate.write(batch);
            try {
                waitForPrevBatch();
            } catch (InterruptedException e) {
                // Restore interrupted state...
                Thread.currentThread().interrupt();
                LOGGER.info(ERROR_ON_INTERRUPT, e);
                return;
            }

            final ObjectReadData<String> rowKeyData = (ObjectReadData<String>)batch.get(0);
            rowKeyData.retain();
            m_future = m_future.thenRunAsync(() -> { // NOSONAR
                try {
                    for (int i = 0; i < rowKeyData.length(); i++) {
                        m_duplicateChecker.addKey(rowKeyData.getObject(i));
                    }
                } catch (IOException e) {
                    throw new IllegalStateException("Failure while checking for duplicate row IDs", e);
                } finally {
                    rowKeyData.release();
                }
            }, m_executor);
        }

        @Override
        public synchronized void close() throws IOException {
            if (m_duplicateChecker != null) {
                try {
                    waitForPrevBatch();
                    m_duplicateChecker.checkForDuplicates();
                } catch (InterruptedException e) {
                    // Restore interrupted state...
                    Thread.currentThread().interrupt();
                    LOGGER.info(ERROR_ON_INTERRUPT, e);
                } finally {
                    m_duplicateChecker.clear();
                    m_duplicateChecker = null;
                    m_writerDelegate.close();
                }
            }
        }

        private void waitForPrevBatch() throws InterruptedException, IOException {
            try {
                m_future.get();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof DuplicateKeyException) {
                    final String key = ((DuplicateKeyException)e.getCause()).getKey();
                    final DuplicateKeyException newDKE =
                        new DuplicateKeyException(String.format("Encountered duplicate row ID \"%s\"", key), key);
                    newDKE.initCause(e);
                    throw newDKE;
                } else {
                    throw new IOException("Failed to asynchronously check for duplicate row IDs.", e);
                }
            }
        }

    }

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DuplicateCheckWritable.class);

    private static final String ERROR_ON_INTERRUPT = "Interrupted while waiting for row key duplicate checker thread.";

    private final ColumnarSchema m_schema;

    private final DuplicateCheckBatchWriter m_writer;

    private final ExecutorService m_executor;

    private DuplicateChecker m_duplicateChecker;

    /**
     * @param delegate to read/write data from/to
     * @param duplicateChecker the duplicate checker to use
     * @param executor the executor to which to submit asynchronous duplicate checks
     */
    @SuppressWarnings("resource")
    public DuplicateCheckWritable(final BatchWritable delegate, final DuplicateChecker duplicateChecker,
        final ExecutorService executor) {
        m_schema = delegate.getSchema();
        m_writer = new DuplicateCheckBatchWriter(delegate.getWriter());
        m_duplicateChecker = duplicateChecker;
        m_executor = executor;
    }

    @Override
    public BatchWriter getWriter() {
        return m_writer;
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_schema;
    }

}
