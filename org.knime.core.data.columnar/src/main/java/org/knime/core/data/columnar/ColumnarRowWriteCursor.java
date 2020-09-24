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

package org.knime.core.data.columnar;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.store.ColumnDataFactory;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.columnar.store.ColumnStoreUtils;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DataTypeConfig;
import org.knime.core.data.RowWriteCursor;
import org.knime.core.data.columnar.domain.ColumnarDomain;
import org.knime.core.data.columnar.domain.DefaultDomainStoreConfig;
import org.knime.core.data.columnar.domain.DomainColumnStore;
import org.knime.core.data.columnar.domain.DomainColumnStore.DomainColumnDataWriter;
import org.knime.core.data.columnar.domain.DomainStoreConfig;
import org.knime.core.data.columnar.mapping.DataTypeMapper;
import org.knime.core.data.columnar.mapping.DataTypeMapperRegistry;
import org.knime.core.data.columnar.mapping.DomainDataTypeMapper;
import org.knime.core.data.columnar.mapping.DomainMapper;
import org.knime.core.data.columnar.table.UnsavedColumnarContainerTable;
import org.knime.core.data.container.DataContainer;
import org.knime.core.data.values.WriteValue;

public final class ColumnarRowWriteCursor implements RowWriteCursor<UnsavedColumnarContainerTable>, IndexSupplier {

	private final ColumnStoreFactory m_storeFactory;
	private final ColumnarDataTableSpec m_spec;
	private final DomainColumnStore m_store;
	private final ColumnDataFactory m_columnDataFactory;
	private final DomainColumnDataWriter m_writer;
	private final int m_tableId;

	private WriteValue<?>[] m_values;

	private UnsavedColumnarContainerTable m_table;

	private WriteBatch m_currentData;

	private long m_currentDataMaxIndex;

	private int m_index = 0;

	private long m_size = 0;

	public ColumnarRowWriteCursor(final int tableId, final DataTableSpec spec,
			final ColumnarRowWriteCursorConfig config, final Map<Integer, DataTypeConfig> configs) throws IOException {
		m_tableId = tableId;
		m_spec = DataTypeMapperRegistry.convert(spec, config.getRowKeyConfig(), configs);
		m_storeFactory = new ArrowColumnStoreFactory();
		final Map<Integer, ColumnarDomain> initialDomains = DataTypeMapperRegistry.extractInitialDomains(spec);
		final DomainStoreConfig domainStoreConfig = new DefaultDomainStoreConfig(m_spec, initialDomains, config);
		m_store = new DomainColumnStore(ColumnStoreUtils
				/* Where to get chunk-size from? */
				.cache(m_storeFactory.createWriteStore(m_spec, DataContainer.createTempFile(".knable"), 28000)),
				domainStoreConfig);

		m_columnDataFactory = m_store.getFactory();
		m_writer = m_store.getWriter();

		switchToNextData();
	}

	/**
	 * Only to be used by
	 * {@link ColumnarDataContainerDelegate#setMaxPossibleValues(int)} for backward
	 * compatibility reasons.
	 */
	public void setMaxPossibleValues(final int maxPossibleValues) {
		m_writer.setMaxPossibleValues(maxPossibleValues);
	}

	@Override
	public final void push() {
		if (++m_index > m_currentDataMaxIndex) {
			switchToNextData();
		}
	}

	@Override
	public <W extends WriteValue<?>> W getWriteValue(final int index) {
		@SuppressWarnings("unchecked")
		// TODO avoid the `+` we can do the index shifting upfront in the
		// constructor
		final W value = (W) m_values[index + 1];
		return value;
	}

	@Override
	public void setMissing(final int index) {
		m_currentData.get(index + 1).setMissing(m_index);
	}

	@Override
	public UnsavedColumnarContainerTable finish() throws IOException {
		if (m_table == null) {
			releaseCurrentData(m_index);
			m_writer.close();
			final Map<Integer, DataColumnDomain> columnDomains = mapBackDomains(m_spec.getSourceSpec());
			final ColumnarDataTableSpec specWithUpdatedDomain = m_spec.withUpdatedSourceDomains(columnDomains);
			m_table = new UnsavedColumnarContainerTable(m_tableId, m_storeFactory, specWithUpdatedDomain, m_store,
					m_size);
		}
		return m_table;
	}

	private Map<Integer, DataColumnDomain> mapBackDomains(final DataTableSpec sourceSpec) {
		final Map<Integer, DataColumnDomain> columnDomains = new HashMap<>();
		for (int i = 0; i < sourceSpec.getNumColumns(); i++) {
			final ColumnarDomain columnDomain = m_store.getResultDomain(i);
			if (columnDomain != null) {
				final DataColumnSpec columnSpec = sourceSpec.getColumnSpec(i);
				final DataType columnType = columnSpec.getType();
				final DataTypeMapper columnMapper = DataTypeMapperRegistry.getColumnTypeMapper(columnType);
				if (columnMapper instanceof DomainDataTypeMapper) {
					final DomainDataTypeMapper<?> domainColumnMapper = (DomainDataTypeMapper<?>) columnMapper;
					final Class<?> domainClass = domainColumnMapper.getDomainClass();
					if (!domainClass.isInstance(columnDomain)) {
						throw new IllegalArgumentException("Column " + columnSpec.getName() + " at index " + i
								+ " has a domain of type " + columnDomain.getClass()
								+ " but the expected domain type was " + domainClass + ".");
					}
					@SuppressWarnings("unchecked") // Type safety was ensured above.
					final DomainMapper<ColumnarDomain> domainMapper = (DomainMapper<ColumnarDomain>) domainColumnMapper
							.getDomainMapper();
					columnDomains.put(i, domainMapper.mapFromColumnarDomain(columnDomain));
				} else {
					throw new IllegalStateException("Column mapper " + columnMapper.getClass() + " for data type "
							+ columnType.getName() + " does not support mapping domains.");
				}
			}
		}
		return columnDomains;
	}

	@Override
	public void close() {
		// in case m_table was not created, we have to destroy the store. otherwise
		// the
		// m_table has a handle on the store and therefore the store shouldn't be
		// destroyed.
		try {
			if (m_table == null) {
				// TODO: check if anything gets written in this case -- should not be
				// the case
				releaseCurrentData(0);
				m_writer.close();
				m_store.close();
			}
		} catch (final Exception e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	@Override
	public int getNumColumns() {
		return m_spec.getNumColumns() - 1;
	}

	@Override
	public final int getIndex() {
		return m_index;
	}

	private void switchToNextData() {
		releaseCurrentData(m_index);

		// TODO can we preload data?
		m_currentData = m_columnDataFactory.create();
		m_values = create(m_currentData);

		m_currentDataMaxIndex = m_currentData.capacity() - 1l;
		m_index = 0;
	}

	private WriteValue<?>[] create(WriteBatch batch) {
		final WriteValue<?>[] values = new WriteValue<?>[m_spec.getNumColumns()];
		for (int i = 0; i < values.length; i++) {
			@SuppressWarnings("unchecked")
			final ColumnType<ColumnWriteData, ?> type = (ColumnType<ColumnWriteData, ?>) m_spec.getColumnType(i);
			values[i] = type.createWriteValue(batch.get(i), this);
		}
		return values;
	}

	private void releaseCurrentData(final int numValues) {
		if (m_currentData != null) {
			final ReadBatch readBatch = m_currentData.close(numValues);
			try {
				m_writer.write(readBatch);
			} catch (final IOException e) {
				throw new RuntimeException("Problem occured when writing column data.", e);
			}
			readBatch.release();
			m_currentData = null;
			m_size += numValues;
		}
	}

}
