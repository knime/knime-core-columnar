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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.store.ColumnDataWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.DelegatingColumnStore;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.meta.DataColumnMetaData;
import org.knime.core.util.DuplicateChecker;
import org.knime.core.util.DuplicateKeyException;

// TODO: make sure everything is closed in case of exceptions, etc.
// TODO: Split duplicate checking from domain calculation - reusing async calc code.
// TODO: tickets bug / domain metadata.

/**
 * A {@link ColumnStore} calculating the domains for individual columns and checking for duplicates. Only required
 * during writing.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class DomainColumnStore extends DelegatingColumnStore {

    private final ExecutorService m_executor;

    /* Not final as initialized lazily */
    private volatile Map<Integer, ColumnarDomainCalculator<?, DataColumnDomain>> m_domainCalculators;

    private volatile Map<Integer, ColumnarDomainCalculator<?, DataColumnMetaData[]>> m_metadataCalculators;

    private DomainStoreConfig m_config;

    /**
     * Create a new DomainColumnStore.
     *
     * @param delegate to read/write data from/to
     * @param config of the store
     * @param executor the executor to which to submit asynchronous domain calculations and duplicate checks
     */
    public DomainColumnStore(final ColumnStore delegate, final DomainStoreConfig config,
        final ExecutorService executor) {
        super(delegate);
        m_config = config;
        m_executor = executor;
    }

    @Override
    protected ColumnDataWriter createWriterInternal() {
        return new DomainColumnDataWriter();
    }

    /**
     * Get the resulting {@link DataColumnDomain}s.
     *
     * @param colIndex the columnIndex
     * @return the resulting domain
     */
    public final DataColumnDomain getDomains(final int colIndex) {
        if (m_domainCalculators != null) {
            final ColumnarDomainCalculator<?, DataColumnDomain> calculator = m_domainCalculators.get(colIndex);
            if (calculator != null) {
                return calculator.getDomain();
            }
        }
        return null;
    }

    /**
     * Get the resulting DataColumnMetadata
     *
     * @param colIndex the columnIndex
     * @return the resulting domain
     */
    public final DataColumnMetaData[] getDomainMetadata(final int colIndex) {
        if (m_metadataCalculators != null) {
            final ColumnarDomainCalculator<?, DataColumnMetaData[]> calculator = m_metadataCalculators.get(colIndex);
            if (calculator != null) {
                return calculator.getDomain();
            }
        }
        return null;
    }

    /**
     * Only to be used by {@code ColumnarRowWriteCursor.setMaxPossibleValues(int)} for backward compatibility reasons.
     * <br>
     * May only be called before the first to {@link #getWriter()}.
     *
     * @param maxPossibleValues maximum number of possible values for nominal domains
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
        m_config = null;
        super.closeOnce();
    }

    /**
     * {@link ColumnDataWriter} taking care of domain calculation
     *
     * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
     */
    // TODO can we make this static?
    public final class DomainColumnDataWriter extends DelegatingColumnDataWriter {

        private DuplicateChecker m_duplicateChecker;

        private List<Future<Void>> m_duplicateChecks = new ArrayList<>();

        private Map<Integer, Future<Void>> m_futures = new HashMap<>();

        /**
         * @param delegate the delegate {@link ColumnDataWriter}.
         * @param config config of store
         */
        DomainColumnDataWriter() {
            super(DomainColumnStore.this);
            m_duplicateChecker = m_config.createDuplicateChecker();
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void writeInternal(final ReadBatch record) throws IOException {
            if (m_domainCalculators == null) {
                // needs to happen here because of setMaxPossibleValues
                m_domainCalculators = m_config.createDomainCalculators();
                m_metadataCalculators = m_config.createMetadataCalculators();

                for (final Entry<Integer, ColumnarDomainCalculator<?, DataColumnDomain>> entry : m_domainCalculators
                    .entrySet()) {
                    m_futures.put(entry.getKey(), CompletableFuture.completedFuture(null));
                }

                for (final Entry<Integer, ColumnarDomainCalculator<?, DataColumnMetaData[]>> entry : m_metadataCalculators
                    .entrySet()) {
                    if (m_futures.get(entry.getKey()) == null) {
                        m_futures.put(entry.getKey(), CompletableFuture.completedFuture(null));
                    }
                }
            }

            if (m_duplicateChecker != null) {
                final Future<Void> duplicateCheck;
                final ColumnReadData keyChunk = record.get(0);
                // Retain for async. duplicate checking. Submitted task will release.
                keyChunk.retain();
                try {
                    duplicateCheck = m_executor.submit(new DuplicateCheckTask(keyChunk, m_duplicateChecker));
                } catch (final Exception ex) {
                    keyChunk.release();
                    throw ex;
                }
                // TODO: list grows indefinitely right now. Clean up every now and then?
                m_duplicateChecks.add(duplicateCheck);
            }

            // Append all domain calculators
            for (final Entry<Integer, ColumnarDomainCalculator<?, DataColumnDomain>> entry : m_domainCalculators
                .entrySet()) {
                final ColumnarDomainCalculator<ColumnReadData, ?> calculator =
                    (ColumnarDomainCalculator<ColumnReadData, ?>)entry.getValue();
                m_futures.put(entry.getKey(),
                    append(record.get(entry.getKey()), m_futures.get(entry.getKey()), calculator));
            }

            // Append all metadata mappers
            for (final Entry<Integer, ColumnarDomainCalculator<?, DataColumnMetaData[]>> entry : m_metadataCalculators
                .entrySet()) {
                final ColumnarDomainCalculator<ColumnReadData, ?> calculator =
                    (ColumnarDomainCalculator<ColumnReadData, ?>)entry.getValue();
                m_futures.put(entry.getKey(),
                    append(record.get(entry.getKey()), m_futures.get(entry.getKey()), calculator));
            }

            super.writeInternal(record);
        }

        private Future<Void> append(final ColumnReadData chunk, final Future<Void> previous,
            final ColumnarDomainCalculator<ColumnReadData, ?> calculator) {
            final Future<Void> current;
            // Retain for async. domain computation. Submitted task will release.
            chunk.retain();
            try {
                current = m_executor.submit(new DomainCalculationTask(previous, chunk, calculator));
            } catch (final Exception ex) {
                chunk.release();
                throw ex;
            }
            return current;
        }

        @Override
        protected void closeOnce() throws IOException {
            try {
                // Wait for duplicate checks and domain calculations to finish before
                // closing.
                for (final Future<Void> duplicateChecks : m_duplicateChecks) {
                    duplicateChecks.get();
                }
                if (m_duplicateChecker != null) {
                    m_duplicateChecker.checkForDuplicates();
                }
                for (final Future<Void> domainCalculations : m_futures.values()) {
                    domainCalculations.get();
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            } catch (final ExecutionException e) {
                if (e.getCause() instanceof DuplicateKeyException) {
                    throw (DuplicateKeyException)e.getCause();
                } else {
                    throw new IOException(e.getCause());
                }
            } finally {
                if (m_duplicateChecker != null) {
                    m_duplicateChecker.clear();
                    m_duplicateChecker = null;
                }
                m_duplicateChecks = null;
                m_futures = null;
            }
            super.closeOnce();
        }

        // TODO Marc can we make this static?
        private final class DuplicateCheckTask implements Callable<Void> {

            private final ColumnReadData m_keyChunk;

            @SuppressWarnings("hiding")
            private final DuplicateChecker m_duplicateChecker;

            public DuplicateCheckTask(final ColumnReadData keyChunk, final DuplicateChecker duplicateChecker) {
                m_keyChunk = keyChunk;
                m_duplicateChecker = duplicateChecker;
            }

            @Override
            public Void call() throws IOException {
                try {
                    if (!DomainColumnStore.this.isClosed()) {
                        @SuppressWarnings("unchecked")
                        final ObjectReadData<String> rowKeyData = (ObjectReadData<String>)m_keyChunk;
                        for (int i = 0; i < rowKeyData.length(); i++) {
                            // TODO: check if we can implement that on a lower level, e.g. on ColumnData directly.
                            m_duplicateChecker.addKey(rowKeyData.getObject(i));
                        }
                    }
                } finally {
                    m_keyChunk.release();
                }
                return null;
            }
        }

        private final class DomainCalculationTask implements Callable<Void> {

            private final Future<Void> m_previous;

            private final ColumnReadData m_chunk;

            private final ColumnarDomainCalculator<ColumnReadData, ?> m_calculator;

            public DomainCalculationTask(final Future<Void> previous, final ColumnReadData chunk,
                final ColumnarDomainCalculator<ColumnReadData, ?> calculator) {
                m_previous = previous;
                m_chunk = chunk;
                m_calculator = calculator;
            }

            @Override
            public Void call() throws InterruptedException, ExecutionException {
                // wait for prev
                m_previous.get();
                if (DomainColumnStore.this.isClosed()) {
                    return null;
                }
                try {
                    m_calculator.update(m_chunk);
                } finally {
                    m_chunk.release();
                }
                return null;
            }
        }
    }
}
