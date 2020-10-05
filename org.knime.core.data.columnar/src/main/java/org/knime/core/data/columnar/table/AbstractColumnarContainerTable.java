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

package org.knime.core.data.columnar.table;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.knime.core.columnar.phantom.CloseableCloser;
import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowCursor;
import org.knime.core.data.columnar.ColumnarDataTableSpec;
import org.knime.core.data.columnar.mapping.MappedColumnarDataTableSpec;
import org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.ContainerTable;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTable.KnowsRowCountTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.ExtensionTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.WorkflowDataRepository;

/**
 * TODO
 *
 * @author Christian Dietz
 */
abstract class AbstractColumnarContainerTable extends ExtensionTable implements ContainerTable {

	private static final String CFG_FACTORY_TYPE = "columnstore_factory_type";
	private static final String CFG_TABLE_SIZE = "tabe_size";

	private final ColumnStoreFactory m_factory;
	private final ColumnarDataTableSpec m_spec;
	private final long m_tableId;
	private final long m_size;

	private ColumnReadStore m_store;

	private final Set<CloseableCloser> m_openCursorCloseables = Collections.newSetFromMap(new ConcurrentHashMap<>());

	AbstractColumnarContainerTable(final LoadContext context) throws InvalidSettingsException {
		final NodeSettingsRO settings = context.getSettings();
		m_tableId = -1;
		m_size = settings.getLong(CFG_TABLE_SIZE);
		m_factory = createInstance(settings.getString(CFG_FACTORY_TYPE));
		m_spec = MappedColumnarDataTableSpec.Serializer.load(context.getTableSpec(), settings);
		m_store = ColumnarPreferenceUtils.wrap(m_factory.createReadStore(m_spec, context.getDataFileRef().getFile()));
	}

	@Override
	protected void saveToFileOverwrite(final File f, final NodeSettingsWO settings, final ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		settings.addLong(CFG_TABLE_SIZE, m_size);
		settings.addString(CFG_FACTORY_TYPE, m_factory.getClass().getName());
		// FIXME: remove need to cast spec
		MappedColumnarDataTableSpec.Serializer.save((MappedColumnarDataTableSpec) m_spec, settings);
	}

	public AbstractColumnarContainerTable(final int tableId, final ColumnStoreFactory factory,
			final ColumnarDataTableSpec spec, final ColumnReadStore store, final long size) {
		m_tableId = tableId;
		m_factory = factory;
		m_spec = spec;
		m_store = store;
		m_size = size;
	}

	@Override
	public DataTableSpec getDataTableSpec() {
		return m_spec.getSourceSpec();
	}

	@Override
	public int getTableId() {
		return (int) m_tableId;
	}

	@Override
	public int getRowCount() {
		return KnowsRowCountTable.checkRowCount(m_size);
	}

	@Override
	public void putIntoTableRepository(final WorkflowDataRepository dataRepository) {
		// TODO only relevant in case of newly created tables?
		dataRepository.addTable((int) m_tableId, this);
	}

	@Override
	public boolean removeFromTableRepository(final WorkflowDataRepository dataRepository) {
		// TODO only relevant in case of newly created tables?
		dataRepository.removeTable((int) m_tableId);
		return true;
	}

	@Override
	public long size() {
		return m_size;
	}

	@Override
	public void clear() {
		try {
			for (final CloseableCloser closer : m_openCursorCloseables) {
				closer.closeCloseableAndLogOutput();
			}
			m_openCursorCloseables.clear();
			if (m_store != null) {
				m_store.close();
				m_store = null;
			}
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public BufferedDataTable getBufferedDataTable(final ExecutionContext context) {
		return create(context);
	}

	@Override
	public BufferedDataTable[] getReferenceTables() {
		return new BufferedDataTable[0];
	}

	@Override
	public final void ensureOpen() {
		// Noop. We directly read from workspace and don't copy data over first.
	}

	@Override
	public RowCursor cursor() {
		return ColumnarRowCursor.create(m_store, m_spec, 0, m_size - 1, m_openCursorCloseables);
	}

	@Override
	public final CloseableRowIterator iterator() {
		return new RowCursorBasedRowIterator(
				ColumnarRowCursor.create(m_store, m_spec, 0, m_size - 1, m_openCursorCloseables));
	}

	@Override
	public RowCursor cursor(final TableFilter filter) {
		final long fromRowIndex = filter.getFromRowIndex().orElse(0l);
		final long toRowIndex = filter.getToRowIndex().orElse(m_size - 1);

		final Optional<Set<Integer>> colIndicesOpt = filter.getMaterializeColumnIndices();
		if (colIndicesOpt.isPresent()) {
			return ColumnarRowCursor.create(m_store, m_spec, fromRowIndex, toRowIndex, m_openCursorCloseables,
					toSortedIntArray(colIndicesOpt.get()));
		} else {
			return ColumnarRowCursor.create(m_store, m_spec, fromRowIndex, toRowIndex, m_openCursorCloseables);
		}
	}

	@Override
	public final CloseableRowIterator iteratorWithFilter(final TableFilter filter, final ExecutionMonitor exec) {
		final long fromRowIndex = filter.getFromRowIndex().orElse(0l);
		final long toRowIndex = filter.getToRowIndex().orElse(m_size - 1);

		final Optional<Set<Integer>> colIndicesOpt = filter.getMaterializeColumnIndices();
		if (colIndicesOpt.isPresent()) {
			int[] selection = toSortedIntArray(colIndicesOpt.get());
			return new FilteredColumnarRowIterator(ColumnarRowCursor.create(m_store, m_spec, fromRowIndex, toRowIndex,
					m_openCursorCloseables, selection), selection);
		} else {

			return new RowCursorBasedRowIterator(
					ColumnarRowCursor.create(m_store, m_spec, fromRowIndex, toRowIndex, m_openCursorCloseables));
		}
	}

	private static <O> O createInstance(final String type) throws InvalidSettingsException {
		try {
			@SuppressWarnings("unchecked")
			final O o = (O) Class.forName(type).getDeclaredConstructor().newInstance();
			return o;
		} catch (Exception e) {
			throw new InvalidSettingsException("Unable to instantiate object of type: " + type, e);
		}
	}

	private static int[] toSortedIntArray(final Set<Integer> selection) {
		return selection.stream().sorted().mapToInt((i) -> (i)).toArray();
	}
}
