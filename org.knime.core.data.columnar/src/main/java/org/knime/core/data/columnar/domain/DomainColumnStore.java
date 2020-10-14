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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.domain.Domain;
import org.knime.core.columnar.domain.DomainCalculator;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.ColumnDataFactory;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnDataWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.ColumnStoreSchema;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.util.DuplicateChecker;
import org.knime.core.util.DuplicateKeyException;
import org.knime.core.util.ThreadPool;

// TODO: make sure everything is closed in case of exceptions, etc.
// TODO: refactor to core.columnar?
// TODO: re-integrate changes of marc bux made in bug fix (should be done - please check CD.)
// TODO: Split duplicate checking from domain calculation - reusing async calc code.
// TODO: tickets bug / domain metadata.

/**
 * A {@link ColumnStore} calculating the domains for individual columns and checking for duplicates. Only required
 * during writing.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class DomainColumnStore implements ColumnStore {

    // TODO: should I use ThreadPool or my own executor service?
    // TODO: how many executors may I block?
    private static final ThreadPool EXECUTOR = KNIMEConstants.GLOBAL_THREAD_POOL;

    private final ColumnStore m_delegate;

    private final DomainColumnDataWriter m_writer;

    private final Map<Integer, DomainFactory<? extends ColumnReadData, ? extends Domain>> m_mappers;

    private volatile boolean m_storeClosed;

    /**
     * Create a new DomainColumnStore.
     *
     * @param delegate to read/write data from/to
     * @param config of the store
     */
    @SuppressWarnings("resource")
    public DomainColumnStore(final ColumnStore delegate, final DomainStoreConfig config) {
        m_delegate = delegate;
        m_writer = new DomainColumnDataWriter(delegate.getWriter(), config);
        m_mappers = config.createMappers();
    }

    @Override
    public ColumnDataFactory getFactory() {
        return m_delegate.getFactory();
    }

    @Override
    public DomainColumnDataWriter getWriter() {
        return m_writer;
    }

    /**
     * Get the resulting {@link DataColumnDomain}s.
     *
     * @param colIndex the columnIndex
     * @return the resulting domain
     */
    public final DataColumnDomain getResultDomain(final int colIndex) {
        try {
            @SuppressWarnings("unchecked")
            final DomainFactory<? extends ColumnReadData, Domain> cast =
                (DomainFactory<? extends ColumnReadData, Domain>)m_mappers.get(colIndex);
            // TODO API
            final Future<Domain> future = m_writer.m_domainCalculations.get(colIndex);
            if (future != null) {
                return cast.convert(future.get());
            } else {
                return null;
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Domain calculation has been interrupted.", ex);
        } catch (final ExecutionException ex) {
            throw new IllegalStateException(ex.getCause());
        }
    }

    @Override
    public void save(final File f) throws IOException {
        m_delegate.save(f);
    }

    @Override
    public ColumnStoreSchema getSchema() {
        return m_delegate.getSchema();
    }

    @Override
    public ColumnDataReader createReader(final ColumnSelection config) {
        return m_delegate.createReader(config);
    }

    @Override
    public void close() throws IOException {
        m_storeClosed = true;

        m_writer.close();
        m_delegate.close();
    }

    /**
     * {@link ColumnDataWriter} taking care of domain calculation
     *
     * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
     */
    // TODO make static?
    public final class DomainColumnDataWriter implements ColumnDataWriter {

        private final ColumnDataWriter m_delegateWriter;

        private final DuplicateChecker m_duplicateChecker;

        private final List<Future<Void>> m_duplicateChecks = new ArrayList<>();

        private final Map<Integer, Future<Domain>> m_domainCalculations = new HashMap<>();

        /* Set in {@link #setMaxPossibleValues(int)}. */
        private DomainStoreConfig m_config;

        /* Depends on m_config */
        @SuppressWarnings("hiding")
        private Map<Integer, DomainFactory<? extends ColumnReadData, ? extends Domain>> m_mappers;

        /* Not final as initialized lazily */
        private Map<Integer, DomainCalculator<?, ?>> m_calculators;

        /**
         * @param delegate the delegate {@link ColumnDataWriter}.
         * @param config config of store
         */
        DomainColumnDataWriter(final ColumnDataWriter delegate, final DomainStoreConfig config) {
            m_delegateWriter = delegate;
            m_config = config;
            m_duplicateChecker = config.createDuplicateChecker();
        }

        /**
         * Only to be used by {@code ColumnarRowWriteCursor.setMaxPossibleValues(int)} for backward compatibility
         * reasons. <br>
         * May only be called before the first call to {@link #write(ReadBatch)}.
         *
         * @param maxPossibleValues maximum number of possible values for nominal domainsl
         */
        public void setMaxPossibleValues(final int maxPossibleValues) {
            if (m_mappers != null) {
                throw new IllegalStateException(
                    "The maximum number of possible values for a nominal domain may only be set "
                        + "before any values were written.");
            }
            m_config = m_config.withMaxPossibleNominalDomainValues(maxPossibleValues);
        }

        @Override
        public void write(final ReadBatch record) throws IOException {
            if (m_calculators == null) {
                // needs to happen here because of setMaxPossibleValues
                m_mappers = m_config.createMappers();
                m_calculators = new HashMap<>();
                final Map<Integer, DataColumnDomain> initialDomains = m_config.getInitialDomains();
                for (final Entry<Integer, DomainFactory<?, ?>> entry : m_mappers.entrySet()) {
                    @SuppressWarnings("unchecked")
                    final DomainCalculator<?, Domain> cast = (DomainCalculator<?, Domain>)entry.getValue()
                        .createCalculator(initialDomains.get(entry.getKey()));
                    m_calculators.put(entry.getKey(), cast);
                    m_domainCalculations.put(entry.getKey(),
                        CompletableFuture.completedFuture(cast.createInitialDomain()));
                }
            }

            if (m_duplicateChecker != null) {
                final Future<Void> duplicateCheck;
                final ColumnReadData keyChunk = record.get(0);
                // Retain for async. duplicate checking. Submitted task will release.
                keyChunk.retain();
                try {
                    duplicateCheck = EXECUTOR.enqueue(new DuplicateCheckTask(keyChunk, m_duplicateChecker));
                } catch (final Exception ex) {
                    keyChunk.release();
                    throw ex;
                }
                // TODO: list grows indefinitely right now. Clean up every now and then?
                m_duplicateChecks.add(duplicateCheck);
            }

            for (final Entry<Integer, DomainCalculator<?, ?>> entry : m_calculators.entrySet()) {
                @SuppressWarnings("unchecked")
                final DomainCalculator<ColumnReadData, Domain> calculator =
                    (DomainCalculator<ColumnReadData, Domain>)entry.getValue();
                if (calculator != null) {
                    final int index = entry.getKey();
                    final Future<Domain> merged;
                    final ColumnReadData chunk = record.get(index);
                    // Retain for async. domain computation. Submitted task will release.
                    chunk.retain();
                    try {
                        final Future<Domain> previous = m_domainCalculations.get(index);
                        merged = EXECUTOR.enqueue(new DomainCalculationTask(previous, chunk, calculator));
                    } catch (final Exception ex) {
                        chunk.release();
                        throw ex;
                    }
                    m_domainCalculations.put(index, merged);
                }
            }

            m_delegateWriter.write(record);
        }

        @Override
        public void close() throws IOException {
            try {
                // Wait for duplicate checks and domain calculations to finish before
                // closing.
                for (final Future<Void> duplicateChecks : m_duplicateChecks) {
                    duplicateChecks.get();
                }
                if (m_duplicateChecker != null) {
                    m_duplicateChecker.checkForDuplicates();
                }
                for (final Future<Domain> domainCalculations : m_domainCalculations.values()) {
                    domainCalculations.get();
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            } catch (final ExecutionException e) {
                throw new IOException(e.getCause());
            } catch (final DuplicateKeyException e) {
                throw new IOException(e);
            } finally {
                if (m_duplicateChecker != null) {
                    m_duplicateChecker.clear();
                }
                m_delegateWriter.close();
            }
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
                    if (!m_storeClosed) {
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

        private final class DomainCalculationTask implements Callable<Domain> {

            private final Future<Domain> m_previous;

            private final ColumnReadData m_chunk;

            private final DomainCalculator<ColumnReadData, Domain> m_calculator;

            public DomainCalculationTask(final Future<Domain> previous, final ColumnReadData chunk,
                final DomainCalculator<ColumnReadData, Domain> calculator) {
                m_previous = previous;
                m_chunk = chunk;
                m_calculator = calculator;
            }

            @Override
            public Domain call() throws InterruptedException, ExecutionException {
                final Domain previous = m_previous.get();
                if (m_storeClosed) {
                    return previous;
                }
                final Domain merged;
                if (previous.isValid()) {
                    final Domain current;
                    try {
                        current = m_calculator.calculateDomain(m_chunk);
                    } finally {
                        m_chunk.release();
                    }
                    merged = m_calculator.mergeDomains(previous, current);
                } else {
                    // No need to put any more effort into an already invalid domain.
                    m_chunk.release();
                    merged = previous;
                }
                return merged;
            }
        }
    }
}
