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

import java.util.Set;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.phantom.CloseableCloser;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.data.DataValue;
import org.knime.core.data.RowCursor;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.columnar.ColumnType;
import org.knime.core.data.columnar.ColumnarDataTableSpec;
import org.knime.core.data.columnar.IndexSupplier;
import org.knime.core.data.values.ReadValue;
import org.knime.core.data.values.RowKeyReadValue;

class ColumnarRowCursor implements RowCursor, IndexSupplier {

	// effectively final (set in #create)
	private CloseableCloser m_closer;

	private final Set<CloseableCloser> m_openCursorCloseables;

	private final ColumnDataReader m_reader;

	private final ColumnarDataTableSpec m_spec;

	private final int m_maxBatchIndex;

	private final int m_lastBatchMaxIndex;

	private final int[] m_selection;

	private ReadBatch m_currentBatch;

	private ReadValue[] m_currentValues;

	private int m_currentBatchIndex;

	private int m_currentIndex;

	private int m_currentMaxIndex;

	static ColumnarRowCursor create(ColumnReadStore store, ColumnarDataTableSpec spec, long fromRowIndex,
			long toRowIndex, Set<CloseableCloser> openCursorCloseables) {
		return create(store, spec, fromRowIndex, toRowIndex, openCursorCloseables, null);
	}

	static ColumnarRowCursor create(ColumnReadStore store, ColumnarDataTableSpec spec, long fromRowIndex,
			long toRowIndex, Set<CloseableCloser> openCursorCloseables, int[] selection) {
		if (selection != null) {
			selection = addRowKeyIndexToSelection(selection);
		}
		final ColumnarRowCursor cursor = new ColumnarRowCursor(store, spec, fromRowIndex, toRowIndex,
				openCursorCloseables, selection);
		cursor.m_closer = new CloseableCloser(cursor);
		openCursorCloseables.add(cursor.m_closer);
		return cursor;
	}

	private ColumnarRowCursor(ColumnReadStore store, ColumnarDataTableSpec spec, long fromRowIndex, long toRowIndex,
			Set<CloseableCloser> openCursorCloseables, int[] selection) {
		// TODO check that from fromRowIndex > 0 and <= toRowIndex
		// check that toRowIndex < table size (which currently cannot be determined)

		m_selection = selection;
		m_reader = store.createReader();
		m_spec = spec;
		m_openCursorCloseables = openCursorCloseables;

		// number of chunks
		final int maxLength;
		try {
			maxLength = m_reader.getMaxLength();
		} catch (final Exception e) {
			// TODO
			throw new RuntimeException(e);
		}
		if (maxLength < 1) {
			m_maxBatchIndex = m_lastBatchMaxIndex = m_currentBatchIndex = m_currentIndex = m_currentMaxIndex = -1;
		} else {
			m_maxBatchIndex = (int) (toRowIndex / maxLength);

			// in the last chunk we only iterate until toRowIndex
			m_lastBatchMaxIndex = (int) (toRowIndex % maxLength);

			m_currentBatchIndex = (int) (fromRowIndex / maxLength) - 1;

			// start index
			m_currentIndex = (int) (fromRowIndex % maxLength) - 1;

			// read next batch
			readNextBatch();
		}
	}

	@Override
	public boolean poll() {
		// TODO throw appropriate exception in case canPoll = false but poll() called
		if (++m_currentIndex > m_currentMaxIndex) {
			m_currentBatch.release();
			readNextBatch();
			m_currentIndex = 0;
		}
		return canPoll();
	}

	@Override
	public boolean canPoll() {
		return m_currentIndex < m_currentMaxIndex || m_currentBatchIndex < m_maxBatchIndex;
	}

	@Override
	public RowKeyValue getRowKeyValue() {
		return (RowKeyReadValue) m_currentValues[0];
	}

	@Override
	public int getNumColumns() {
		return m_spec.getNumColumns() - 1;
	}

	@Override
	public <V extends DataValue> V getValue(int index) {
		@SuppressWarnings("unchecked")
		final V cast = (V) m_currentValues[index + 1];
		return cast;
	}

	@Override
	public boolean isMissing(int index) {
		return m_currentBatch.get(index + 1).isMissing(m_currentIndex);
	}

	@Override
	public void close() {
		try {
			if (m_currentBatch != null) {
				m_currentBatch.release();
				m_currentBatch = null;
			}
			m_closer.close();
			m_openCursorCloseables.remove(m_closer);
			m_reader.close();
		} catch (Exception e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	@Override
	public final int getIndex() {
		return m_currentIndex;
	}

	private void readNextBatch() {
		try {
			m_currentBatch = m_reader.readRetained(++m_currentBatchIndex);
		} catch (final Exception e) {
			// TODO
			throw new RuntimeException(e);
		}
		if (m_selection == null) {
			m_currentValues = create(m_currentBatch);
		} else {
			m_currentValues = create(m_currentBatch, m_selection);
		}
		// as soon as we're in the last chunk, we might want to iterate fewer
		// values.
		m_currentMaxIndex = m_currentBatchIndex != m_maxBatchIndex ? m_currentBatch.length() - 1 : m_lastBatchMaxIndex;
	}

	// Expensive for many columns, but only called once per chunk
	private ReadValue[] create(ReadBatch batch) {
		final ReadValue[] values = new ReadValue[m_spec.getNumColumns()];
		for (int i = 0; i < values.length; i++) {
			@SuppressWarnings("unchecked")
			final ColumnType<?, ColumnReadData> type = (ColumnType<?, ColumnReadData>) m_spec.getColumnType(i);
			values[i] = type.createReadValue(batch.get(i), this);
		}
		return values;
	}

	private final ReadValue[] create(ReadBatch batch, int[] selection) {
		// TODO Marc Bux: here the assumption is that ColumnDataAccess always has size
		// of incoming spec with some nulls.
		final ReadValue[] values = new ReadValue[m_spec.getNumColumns()];
		for (int i = 0; i < selection.length; i++) {
			@SuppressWarnings("unchecked")
			ColumnType<?, ColumnReadData> type = (ColumnType<?, ColumnReadData>) m_spec.getColumnType(selection[i]);
			values[selection[i]] = type.createReadValue(batch.get(selection[i]), this);
		}
		return values;
	}

	private static int[] addRowKeyIndexToSelection(int[] selection) {
		final int[] colIndicesAsInt = new int[selection.length + 1];
		// add row key as selected column
		colIndicesAsInt[0] = 0;
		int i = 1;
		for (int index : selection) {
			colIndicesAsInt[i++] = index + 1;
		}

		return colIndicesAsInt;
	}

}
