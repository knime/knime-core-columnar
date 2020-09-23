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

import org.knime.core.columnar.batch.Batch;
import org.knime.core.columnar.data.ColumnData;
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

	private final ColumnDataReader m_reader;

	private int m_numChunks;

	private int m_chunkIndex = 0;

	private int m_currentDataMaxIndex;

	private Batch m_currentBatch;

	private int m_lastChunkMaxIndex;

	private final Set<CloseableCloser> m_openCursorCloseables;

	private CloseableCloser m_closer;

	private int m_index = -1;

	private ReadValue[] m_values;

	private ColumnarDataTableSpec m_spec;

	static ColumnarRowCursor create(ColumnReadStore reader, ColumnarDataTableSpec spec, long fromRowIndex,
			long toRowIndex, Set<CloseableCloser> openCursorCloseables) {
		final ColumnarRowCursor cursor = new ColumnarRowCursor(reader, spec, fromRowIndex, toRowIndex,
				openCursorCloseables);
		cursor.m_closer = new CloseableCloser(cursor);
		openCursorCloseables.add(cursor.m_closer);
		return cursor;
	}

	private ColumnarRowCursor(ColumnReadStore reader, ColumnarDataTableSpec spec, long fromRowIndex, long toRowIndex,
			Set<CloseableCloser> openCursorCloseables) {
		m_reader = reader.createReader();
		m_spec = spec;

		m_numChunks = m_reader.getNumBatches();

		// start chunk
		m_chunkIndex = (int) (fromRowIndex / m_reader.getMaxLength());

		// start index
		m_index = (int) (fromRowIndex % m_reader.getMaxLength()) - 1;

		// number of chunks
		m_numChunks = (int) Math.min(m_reader.getNumBatches(), (toRowIndex / m_reader.getMaxLength()) + 1);

		// in the last chunk we only iterate until toRowIndex
		m_lastChunkMaxIndex = (int) (toRowIndex % m_reader.getMaxLength());

		// switch to next chunk
		switchToNextData();

		m_openCursorCloseables = openCursorCloseables;
	}

	@Override
	public boolean canPoll() {
		return m_index < m_currentDataMaxIndex || m_chunkIndex < m_numChunks;
	}

	@Override
	public boolean poll() {
		if (++m_index > m_currentDataMaxIndex) {
			switchToNextData();
			m_index = 0;
		}
		return canPoll();
	}

	@Override
	public RowKeyValue getRowKeyValue() {
		return (RowKeyReadValue) m_values[0];
	}

	@Override
	public int getNumColumns() {
		return m_values.length - 1;
	}

	@Override
	public <V extends DataValue> V getValue(int index) {
		@SuppressWarnings("unchecked")
		final V cast = (V) m_values[index + 1];
		return cast;
	}

	@Override
	public boolean isMissing(int index) {
		return m_currentBatch.get(index + 1).isMissing(m_index);
	}

//	@Override
//	public Optional<String> getMissingValueError(int index) {
//		return Optional.ofNullable(m_currentBatch.getData(index).getMissingCause(m_index));
//	}

	@Override
	public void close() {
		try {
			releaseCurrentData();
			m_closer.close();
			m_openCursorCloseables.remove(m_closer);
			m_reader.close();
		} catch (Exception e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private void switchToNextData() {
		try {
			releaseCurrentData();
			m_currentBatch = m_reader.readRetained(m_chunkIndex++);
			m_values = create(m_currentBatch);
			m_index = -1;

			// as soon as we're in the last chunk, we might want to iterate fewer
			// values.
			if (m_chunkIndex == m_numChunks) {
				m_currentDataMaxIndex = m_lastChunkMaxIndex;
			} else {
				m_currentDataMaxIndex = m_currentBatch.length() - 1;
			}
		} catch (final Exception e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	// Expensive for many columns, but only called once per chunk
	private ReadValue[] create(Batch batch) {
		final ReadValue[] values = new ReadValue[m_spec.getNumColumns()];
		for (int i = 0; i < values.length; i++) {
			@SuppressWarnings("unchecked")
			final ColumnType<?, ColumnData> type = (ColumnType<?, ColumnData>) m_spec.getColumnType(i);
			values[i] = type.createReadValue(batch.get(i), this);
		}
		return values;
	}

	private void releaseCurrentData() {
		if (m_currentBatch != null) {
			m_currentBatch.release();
		}
	}

	@Override
	public final int getIndex() {
		return m_index;
	}

}
