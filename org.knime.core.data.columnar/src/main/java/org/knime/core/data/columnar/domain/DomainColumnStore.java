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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.store.BatchWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.DelegatingColumnStore;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.meta.DataColumnMetaData;
import org.knime.core.node.NodeLogger;

/**
 * A {@link ColumnStore} for calculating domains for individual columns. Only required during writing.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class DomainColumnStore extends DelegatingColumnStore {

    private final class DomainBatchWriter extends DelegatingBatchWriter {

        private CompletableFuture<Void> m_future = CompletableFuture.completedFuture(null);

        DomainBatchWriter() {
            super(DomainColumnStore.this);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void writeInternal(final ReadBatch batch) throws IOException {

            super.writeInternal(batch);
            try {
                waitForPrevBatch();
            } catch (InterruptedException e) {
                // Restore interrupted state...
                Thread.currentThread().interrupt();
                LOGGER.info(ERROR_ON_INTERRUPT, e);
                return;
            }

            // lazily init domain and metadata calculators; needs to happen here because of setMaxPossibleValues
            if (m_domainCalculators == null) {
                m_domainCalculators = m_config.createDomainCalculators();
                m_metadataCalculators = m_config.createMetadataCalculators();
            }

            m_future = CompletableFuture.allOf(Stream
                .concat(m_domainCalculators.entrySet().stream(), m_metadataCalculators.entrySet().stream()).map(e -> {
                    final NullableReadData data = batch.get(e.getKey());
                    data.retain();
                    return CompletableFuture.runAsync(() -> {
                        ((ColumnarDomainCalculator<NullableReadData, ?>)e.getValue()).update(data);
                        data.release();
                    }, m_executor);
                }).toArray(CompletableFuture[]::new));
        }

        @Override
        protected void closeOnce() throws IOException {
            try {
                waitForPrevBatch();
            } catch (InterruptedException e) {
                // Restore interrupted state...
                Thread.currentThread().interrupt();
                LOGGER.info(ERROR_ON_INTERRUPT, e);
            }
            super.closeOnce();
        }

        private void waitForPrevBatch() throws InterruptedException, IOException {
            try {
                m_future.get();
            } catch (ExecutionException e) {
                throw new IOException("Failed to asynchronously calculate domains.", e);
            }
        }

    }

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DomainColumnStore.class);

    private static final String ERROR_ON_INTERRUPT = "Interrupted while waiting for domain calculation thread.";

    private final ExecutorService m_executor;

    /* Not final as initialized lazily */
    private Map<Integer, ColumnarDomainCalculator<? extends NullableReadData, DataColumnDomain>> m_domainCalculators;

    private Map<Integer, ColumnarDomainCalculator<? extends NullableReadData, DataColumnMetaData[]>> m_metadataCalculators;

    private DomainStoreConfig m_config;

    /**
     * @param delegate to read/write data from/to
     * @param config of the store
     * @param executor the executor to which to submit asynchronous domain calculations checks
     */
    public DomainColumnStore(final ColumnStore delegate, final DomainStoreConfig config,
        final ExecutorService executor) {
        super(delegate);
        m_config = config;
        m_executor = executor;
    }

    @Override
    protected BatchWriter createWriterInternal() {
        return new DomainBatchWriter();
    }

    /**
     * Get the resulting {@link DataColumnDomain}.
     *
     * @param colIndex the columnIndex
     * @return the resulting domain, if present, otherwise null
     */
    public final DataColumnDomain getDomain(final int colIndex) {
        if (m_domainCalculators != null) {
            final ColumnarDomainCalculator<?, DataColumnDomain> calculator = m_domainCalculators.get(colIndex);
            if (calculator != null) {
                return calculator.createDomain();
            }
        }
        return null;
    }

    /**
     * Get the resulting {@link DataColumnMetaData}.
     *
     * @param colIndex the columnIndex
     * @return the resulting metadata
     */
    public final DataColumnMetaData[] getMetadata(final int colIndex) {
        if (m_metadataCalculators != null) {
            final ColumnarDomainCalculator<?, DataColumnMetaData[]> calculator = m_metadataCalculators.get(colIndex);
            if (calculator != null) {
                return calculator.createDomain();
            }
        }
        return new DataColumnMetaData[0];
    }

    /**
     * Only to be used by {@code ColumnarRowWriteCursor.setMaxPossibleValues(int)} for backward compatibility reasons.
     * <br>
     * May only be called before the first call to {@link BatchWriter#write(ReadBatch) write()}.
     *
     * @param maxPossibleValues maximum number of possible values for nominal domains
     *
     * @throws IllegalStateException when called after {@link #getWriter()}
     */
    public void setMaxPossibleValues(final int maxPossibleValues) {
        if (m_domainCalculators != null) {
            throw new IllegalStateException(
                "The maximum number of possible values for a nominal domain may only be set "
                    + "before any values were written.");
        }
        m_config = m_config.withMaxPossibleNominalDomainValues(maxPossibleValues);
    }

    @Override
    protected void closeOnce() throws IOException {
        m_domainCalculators = null;
        m_metadataCalculators = null;
        super.closeOnce();
    }

}
