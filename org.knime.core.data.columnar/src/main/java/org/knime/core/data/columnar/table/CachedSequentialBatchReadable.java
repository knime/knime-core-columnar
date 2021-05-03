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
 *   17 Nov 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar.table;

import java.io.IOException;

import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.SequentialBatchReadable;
import org.knime.core.columnar.batch.SequentialBatchReader;
import org.knime.core.columnar.cache.data.ReadDataCache;
import org.knime.core.columnar.cache.data.ReadDataReadCache;
import org.knime.core.columnar.cache.object.ObjectCache;
import org.knime.core.columnar.cache.object.ObjectReadCache;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * A {@link SequentialBatchReadable} that delegates operations through an {@link ObjectCache} and a
 * {@link ReadDataCache}.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
final class CachedSequentialBatchReadable implements SequentialBatchReadable {

    private static final class SequentialDelegateRandomAccessBatchReader implements RandomAccessBatchReader {

        private final SequentialBatchReadable m_delegateReadable;

        private final ColumnSelection m_selection;

        private int m_index;

        private ReadBatch m_loadedBatch;

        private SequentialBatchReader m_delegateReader;

        SequentialDelegateRandomAccessBatchReader(final SequentialBatchReadable delegateReadable,
            final ColumnSelection selection) {
            m_delegateReadable = delegateReadable;
            m_selection = selection;
        }

        @Override
        public ReadBatch readRetained(final int index) throws IOException {
            // if batch already loaded - return it
            if (m_loadedBatch != null && m_index == index) {
                return m_loadedBatch;
            }

            // otherwise, release and reload
            releaseBatch();
            if (m_index > index) {
                m_delegateReader.close();
                m_index = 0;
                m_delegateReader = m_delegateReadable.createSequentialReader(m_selection);
            }
            for (; m_index < index; m_index++) {
                m_delegateReader.forward().release();
            }
            m_index++;
            m_loadedBatch = m_delegateReader.forward();
            return m_loadedBatch;
        }

        @Override
        public void close() throws IOException {
            releaseBatch();
            m_delegateReader.close();
        }

        private void releaseBatch() {
            if (m_loadedBatch != null) {
                m_loadedBatch.release();
            }
            m_loadedBatch = null;
        }

    }

    private static final class SequentialDelegateRandomAccessBatchReadable implements RandomAccessBatchReadable {

        private final SequentialBatchReadable m_delegateReadable;

        SequentialDelegateRandomAccessBatchReadable(final SequentialBatchReadable delegateReadable) {
            m_delegateReadable = delegateReadable;
        }

        @Override
        public ColumnarSchema getSchema() {
            return m_delegateReadable.getSchema();
        }

        @Override
        public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
            return new SequentialDelegateRandomAccessBatchReader(m_delegateReadable, selection);
        }

        @Override
        public void close() throws IOException {
            m_delegateReadable.close();
        }

    }

    private static final class RandomAccessDelegateSequentialBatchReader implements SequentialBatchReader {

        private final RandomAccessBatchReader m_delegateReader;

        private int m_index;

        RandomAccessDelegateSequentialBatchReader(final RandomAccessBatchReadable delegateReadable,
            final ColumnSelection selection) {
            m_delegateReader = delegateReadable.createRandomAccessReader(selection);
        }

        @Override
        public ReadBatch forward() throws IOException {
            try {
                final ReadBatch batch = m_delegateReader.readRetained(m_index);
                m_index++;
                return batch;
            } catch (IndexOutOfBoundsException e) {
                // TODO: determine in another way when to return null
                return null;
            }
        }

        @Override
        public void close() throws IOException {
            m_delegateReader.close();
        }

    }

    private final RandomAccessBatchReadable m_delegateReadable;

    @SuppressWarnings("resource")
    CachedSequentialBatchReadable(final SequentialBatchReadable readable) {
        RandomAccessBatchReadable wrapped = new SequentialDelegateRandomAccessBatchReadable(readable);
        if (ColumnarPreferenceUtils.getColumnDataCacheSize() > 0) {
            wrapped = new ReadDataReadCache(wrapped, ColumnarPreferenceUtils.getColumnDataCache());
        }
        m_delegateReadable = new ObjectReadCache(wrapped, ColumnarPreferenceUtils.getHeapCache());
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_delegateReadable.getSchema();
    }

    @Override
    public SequentialBatchReader createSequentialReader(final ColumnSelection selection) {
        return new RandomAccessDelegateSequentialBatchReader(m_delegateReadable, selection);
    }

    @Override
    public void close() throws IOException {
        m_delegateReadable.close();
    }

}
