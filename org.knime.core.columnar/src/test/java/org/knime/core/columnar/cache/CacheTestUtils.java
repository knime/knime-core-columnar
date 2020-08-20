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
package org.knime.core.columnar.cache;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnSelectionUtil;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
class CacheTestUtils {

    static class TestColumnData implements ColumnData {

        private final int m_sizeOf;

        private AtomicInteger m_refs = new AtomicInteger();

        TestColumnData(final int sizeOf) {
            m_sizeOf = sizeOf;
        }

        @Override
        public void ensureCapacity(final int capacity) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getMaxCapacity() {
            return 0;
        }

        @Override
        public void setNumValues(final int numValues) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getNumValues() {
            return 0;
        }

        @Override
        public void release() {
            m_refs.decrementAndGet();
        }

        @Override
        public void retain() {
            m_refs.incrementAndGet();
        }

        @Override
        public int sizeOf() {
            return m_sizeOf;
        }

        int getRefs() {
            return m_refs.get();
        }

    }

    static ColumnStoreSchema createSchema(final int numColumns) {
        return new ColumnStoreSchema() {
            @Override
            public int getNumColumns() {
                return numColumns;
            }

            @Override
            public ColumnDataSpec<?> getColumnDataSpec(final int idx) {
                return null;
            }
        };
    }

    static int checkRefs(final List<TestColumnData[]> table) {
        if (table.size() == 0) {
            return 0;
        }
        final int refs = checkRefs(table.get(0));
        for (final TestColumnData[] batch : table) {
            assertEquals(refs, checkRefs(batch));
        }
        return refs;
    }

    static int checkRefs(final TestColumnData[] batch) {
        if (batch.length == 0) {
            return 0;
        }
        final int refs = batch[0].getRefs();
        for (final TestColumnData data : batch) {
            assertEquals(refs, data.getRefs());
        }
        return refs;
    }

    static void readAndCompareTable(final ColumnStore store, final List<TestColumnData[]> table) throws Exception {
        readSelectionAndCompareTable(store, table, null);
    }

    static void readSelectionAndCompareTable(final ColumnStore store, final List<TestColumnData[]> table, final int... indices)
        throws Exception {
        try (final ColumnDataReader reader = store.createReader(ColumnSelectionUtil.create(indices))) {
            assertEquals(table.size(), reader.getNumChunks());
            for (int i = 0; i < reader.getNumChunks(); i++) {
                final TestColumnData[] written;
                if (indices == null) {
                    written = table.get(i);
                } else {
                    final TestColumnData[] all = table.get(i);
                    written = Arrays.stream(indices).mapToObj(index -> all[index]).toArray(TestColumnData[]::new);
                }
                final ColumnData[] batch = reader.read(i);
                assertArrayEquals(written, batch);
                for (final ColumnData data : batch) {
                    data.release();
                }
            }
        }
    }

    static void readTwiceAndCompareTable(final ColumnStore store) throws Exception {
        try (final ColumnDataReader reader1 = store.createReader();
                final ColumnDataReader reader2 = store.createReader()) {
            assertEquals(reader1.getNumChunks(), reader2.getNumChunks());
            for (int i = 0; i < reader1.getNumChunks(); i++) {
                final ColumnData[] batch1 = reader1.read(i);
                final ColumnData[] batch2 = reader2.read(i);
                assertArrayEquals(batch1, batch2);
                for (final ColumnData data : batch1) {
                    data.release();
                    data.release();
                }
            }
        }
    }

    static boolean tableInStore(final ColumnStore store, final List<TestColumnData[]> table) throws Exception {
        try (final ColumnDataReader reader = store.createReader()) {
        } catch (IllegalStateException e) {
            return false;
        }
        readAndCompareTable(store, table);
        return true;
    }

    static void writeTable(final ColumnStore store, final List<TestColumnData[]> table) throws Exception {
        try (final ColumnDataWriter writer = store.getWriter()) {
            for (final ColumnData[] batch : table) {
                writer.write(batch);
            }
        }
    }

    static List<TestColumnData[]> createTable(final int height, final int width, final int size) {
        return IntStream.range(0, height).mapToObj(i -> createBatch(width, size)).collect(Collectors.toList());
    }

    static TestColumnData[] createBatch(final int width, final int size) {
        return IntStream.range(0, width).mapToObj(i -> new TestColumnData(size)).toArray(TestColumnData[]::new);
    }

}
