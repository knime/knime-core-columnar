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
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.ColumnDataFactory;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnDataWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.ColumnStoreSchema;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.util.DuplicateChecker;
import org.knime.core.util.DuplicateKeyException;
import org.knime.core.util.ThreadPool;

// TODO: support DataColumnMetaData
// TODO: make sure everything is closed in case of exceptions, etc.
public final class DomainColumnStore implements ColumnStore {

	private final ColumnStore m_delegate;

	private final DomainColumnDataWriter m_writer;

	public DomainColumnStore(final ColumnStore delegate, final DomainStoreConfig config) {
		m_delegate = delegate;
		m_writer = new DomainColumnDataWriter(delegate.getWriter(), config);
	}

	@Override
	public ColumnDataFactory getFactory() {
		return m_delegate.getFactory();
	}

	@Override
	public DomainColumnDataWriter getWriter() {
		return m_writer;
	}

	public <D extends ColumnarDomain> D getResultDomain(final int index) {
		try {
			@SuppressWarnings("unchecked")
			final D casted = (D) m_writer.m_domainCalculations.get(index).get();
			return casted;
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
		m_writer.close();
		m_delegate.close();
	}

	public static final class DomainColumnDataWriter implements ColumnDataWriter {

		// TODO: should I use ThreadPool or my own executor service?
		// TODO: how many executors may I block?
		private static final ThreadPool executor = KNIMEConstants.GLOBAL_THREAD_POOL;

		private final ColumnDataWriter m_delegate;

		private final DuplicateChecker m_duplicateChecker;

		private final List<Future<Void>> m_duplicateChecks = new ArrayList<>();

		private final Map<Integer, Future<ColumnarDomain>> m_domainCalculations = new HashMap<>();

		/**
		 * Set in {@link #setMaxPossibleValues(int)}.
		 */
		private DomainStoreConfig m_config;

		/**
		 * Initialized during the first call to {@link #write(ColumnReadData[])}.
		 */
		private DomainCalculator<?, ?>[] m_calculators;

		public DomainColumnDataWriter(final ColumnDataWriter delegate, final DomainStoreConfig config) {
			m_delegate = delegate;
			m_config = config;
			m_duplicateChecker = config.createDuplicateChecker();
		}

		/**
		 * Only to be used by {@code ColumnarRowWriteCursor.setMaxPossibleValues(int)}
		 * for backward compatibility reasons. <br>
		 * May only be called before the first call to {@link #write(ColumnReadData[])}.
		 */
		public void setMaxPossibleValues(final int maxPossibleValues) {
			if (m_calculators != null) {
				throw new IllegalStateException(
						"The maximum number of possible values for a nominal domain may only be set "
								+ "before any values were written.");
			}
			m_config = m_config.withMaxPossibleNominalDomainValues(maxPossibleValues);
		}

		@Override
		public void write(final ReadBatch record) throws IOException {
			if (m_calculators == null) {
				m_calculators = m_config.createCalculators();
				for (int i = 0; i < m_calculators.length; i++) {
					@SuppressWarnings("unchecked")
					final DomainCalculator<?, ColumnarDomain> calculator = (DomainCalculator<?, ColumnarDomain>) m_calculators[i];
					if (calculator != null) {
						m_domainCalculations.put(i,
								CompletableFuture.completedFuture(calculator.createInitialDomain()));
					}
				}
			}

			if (m_duplicateChecker != null) {
				final Future<Void> duplicateCheck;
				final ColumnReadData keyChunk = record.get(0);
				// Retain for async. duplicate checking. Submitted task will release.
				keyChunk.retain();
				try {
					duplicateCheck = executor.enqueue(new DuplicateCheckTask(keyChunk, m_duplicateChecker));
				} catch (final Exception ex) {
					keyChunk.release();
					throw ex;
				}
				// TODO: list grows indefinitely right now. Clean up every now and then?
				m_duplicateChecks.add(duplicateCheck);
			}

			for (int i = 0; i < m_calculators.length; i++) {
				@SuppressWarnings("unchecked")
				final DomainCalculator<ColumnReadData, ColumnarDomain> calculator = (DomainCalculator<ColumnReadData, ColumnarDomain>) m_calculators[i];
				if (calculator != null) {
					final Future<ColumnarDomain> merged;
					final ColumnReadData chunk = record.get(i + 1);
					// Retain for async. domain computation. Submitted task will release.
					chunk.retain();
					try {
						final Future<ColumnarDomain> previous = m_domainCalculations.get(i);
						merged = executor.enqueue(new DomainCalculationTask(previous, chunk, calculator));
					} catch (final Exception ex) {
						chunk.release();
						throw ex;
					}
					m_domainCalculations.put(i, merged);
				}
			}

			m_delegate.write(record);
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
				for (final Future<ColumnarDomain> domainCalculations : m_domainCalculations.values()) {
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
				m_delegate.close();
			}
		}

		private static final class DuplicateCheckTask implements Callable<Void> {

			private final ColumnReadData m_keyChunk;

			private final DuplicateChecker m_duplicateChecker;

			public DuplicateCheckTask(final ColumnReadData keyChunk, final DuplicateChecker duplicateChecker) {
				m_keyChunk = keyChunk;
				m_duplicateChecker = duplicateChecker;
			}

			@Override
			public Void call() throws IOException {
				try {
					final StringReadData rowKeyData = (StringReadData) m_keyChunk;
					for (int i = 0; i < rowKeyData.length(); i++) {
						// TODO: duplicate checking can be done on the byte level in the
						// future.
						m_duplicateChecker.addKey(rowKeyData.getString(i));
					}
				} finally {
					m_keyChunk.release();
				}
				return null;
			}
		}

		private static final class DomainCalculationTask implements Callable<ColumnarDomain> {

			private final Future<ColumnarDomain> m_previous;

			private final ColumnReadData m_chunk;

			private final DomainCalculator<ColumnReadData, ColumnarDomain> m_calculator;

			public DomainCalculationTask(final Future<ColumnarDomain> previous, final ColumnReadData chunk,
					final DomainCalculator<ColumnReadData, ColumnarDomain> calculator) {
				m_previous = previous;
				m_chunk = chunk;
				m_calculator = calculator;
			}

			@Override
			public ColumnarDomain call() throws InterruptedException, ExecutionException {
				final ColumnarDomain previous = m_previous.get();
				final ColumnarDomain merged;
				if (previous.isValid()) {
					final ColumnarDomain current;
					try {
						current = m_calculator.apply(m_chunk);
					} finally {
						m_chunk.release();
					}
					merged = m_calculator.merge(previous, current);
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
