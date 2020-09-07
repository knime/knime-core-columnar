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
 *   20 Aug 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnSelectionUtil;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public final class TestColumnStoreUtils {

    private static double RUNNING_DOUBLE = 0d;

    public static final class TestTable implements Closeable {
        private final List<TestDoubleColumnData[]> m_batches;

        public TestTable(final List<TestDoubleColumnData[]> batches) {
            m_batches = batches;
        }

        public TestDoubleColumnData[] getBatch(final int index) {
            return m_batches.get(index);
        }

        @Override
        public void close() throws IOException {
            for (TestDoubleColumnData[] batch : m_batches) {
                for (TestDoubleColumnData data : batch) {
                    if (data != null) {
                        data.release();
                    }
                }
            }
        }

        private int size() {
            return m_batches.size();
        }
    }

    private static final int DEF_NUM_COLUMNS = 2;

    private static final int DEF_NUM_BATCHES = 2;

    public static final int DEF_SIZE_OF_DATA = 2;

    public static final int DEF_SIZE_OF_TABLE = DEF_NUM_BATCHES * DEF_NUM_COLUMNS * DEF_SIZE_OF_DATA;

    private TestColumnStoreUtils() {
        // Utility class
    }

    public static ColumnStoreSchema createSchema(final int numColumns) {
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

    public static ColumnStoreSchema generateDefaultSchema() {
        return createSchema(DEF_NUM_COLUMNS);
    }

    public static TestColumnStore generateDefaultTestColumnStore() {
        return new TestColumnStore(generateDefaultSchema(), DEF_SIZE_OF_DATA);
    }

    private static TestDoubleColumnData[] createBatch(final ColumnStore store) {
        final TestDoubleColumnData[] batch = (TestDoubleColumnData[])store.getFactory().create();

        for (final TestDoubleColumnData data : batch) {
            for (int i = 0; i < data.getMaxCapacity(); i++) {
                data.setDouble(i, RUNNING_DOUBLE++);
            }
            data.setNumValues(data.getMaxCapacity());
        }

        return batch;
    }

    public static TestTable createTable(final ColumnStore store, final int numBatches) {
        final TestTable table = new TestTable(
            IntStream.range(0, numBatches).mapToObj(i -> createBatch(store)).collect(Collectors.toList()));
        assertEquals(1, checkRefs(table));
        return table;
    }

    public static TestTable generateDefaultTable(final ColumnStore store) {
        return createTable(store, DEF_NUM_BATCHES);
    }

    public static TestTable generateDoubleSizedDefaultTable(final ColumnStore store) {
        return createTable(store, 2 * DEF_NUM_BATCHES);
    }

    private static int checkRefs(final TestDoubleColumnData[] batch) {
        if (batch.length == 0) {
            return 0;
        }
        final int refs = batch[0].getRefs();
        for (final TestDoubleColumnData data : batch) {
            assertEquals(refs, data.getRefs());
        }
        return refs;
    }

    public static int checkRefs(final TestTable table) {
        if (table.size() == 0) {
            return 0;
        }
        final int refs = checkRefs(table.getBatch(0));
        for (int i = 0; i < table.size(); i++) {
            assertEquals(refs, checkRefs(table.getBatch(i)));
        }
        return refs;
    }

    public static void writeTable(final ColumnStore store, final TestTable table) throws IOException {
        try (final ColumnDataWriter writer = store.getWriter()) {
            for (int i = 0; i < table.size(); i++) {
                writer.write(table.getBatch(i));
            }
        }
    }

    public static boolean tableInStore(final ColumnStore store, final TestTable table) throws IOException {
        try (final ColumnDataReader reader = store.createReader()) {
        } catch (IllegalStateException e) {
            return false;
        }
        try (final TestTable reassembledTable = readAndCompareTable(store, table)) {
        }
        return true;
    }

    public static TestTable readAndCompareTable(final ColumnStore store, final TestTable table) throws IOException {
        return readSelectionAndCompareTable(store, table, null);
    }

    public static TestTable readSelectionAndCompareTable(final ColumnStore store, final TestTable table,
        final int... indices) throws IOException {

        try (final ColumnDataReader reader = store.createReader(ColumnSelectionUtil.create(indices))) {
            assertEquals(table.size(), reader.getNumChunks());

            final List<TestDoubleColumnData[]> result = new ArrayList<>();
            for (int i = 0; i < reader.getNumChunks(); i++) {
                final TestDoubleColumnData[] written = table.getBatch(i);
                final TestDoubleColumnData[] batch = Arrays.stream(reader.read(i))
                    .map(data -> (TestDoubleColumnData)data).toArray(TestDoubleColumnData[]::new);

                assertEquals(written.length, batch.length);

                if (indices == null) {
                    for (int j = 0; j < written.length; j++) {
                        assertArrayEquals(written[j].get(), (batch[j]).get());
                    }
                } else {
                    for (int j = 0; j < indices.length; j++) {
                        assertArrayEquals(written[indices[j]].get(), (batch[indices[j]]).get());
                    }
                }

                result.add(batch);
            }
            return new TestTable(result);
        }
    }

    public static void readTwiceAndCompareTable(final ColumnStore store) throws IOException {
        try (final ColumnDataReader reader1 = store.createReader();
                final ColumnDataReader reader2 = store.createReader()) {
            assertEquals(reader1.getNumChunks(), reader2.getNumChunks());
            for (int i = 0; i < reader1.getNumChunks(); i++) {
                final TestDoubleColumnData[] batch1 = Arrays.stream(reader1.read(i))
                    .map(data -> (TestDoubleColumnData)data).toArray(TestDoubleColumnData[]::new);
                final TestDoubleColumnData[] batch2 = Arrays.stream(reader2.read(i))
                    .map(data -> (TestDoubleColumnData)data).toArray(TestDoubleColumnData[]::new);

                assertEquals(batch1.length, batch2.length);
                for (int j = 0; j < batch1.length; j++) {
                    assertArrayEquals(batch1[j].get(), (batch2[j]).get());
                }

                for (final ColumnData data : batch1) {
                    data.release();
                }
                for (final ColumnData data : batch2) {
                    data.release();
                }
            }
        }
    }

}