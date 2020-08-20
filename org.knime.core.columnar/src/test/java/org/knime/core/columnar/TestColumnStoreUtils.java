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
public class TestColumnStoreUtils {

    private static final int DEF_TABLE_HEIGHT = 2;

    private static final int DEF_TABLE_WIDTH = 2;

    private static final int DEF_SIZE_OF_COLUMN_DATA = 1;

    public static final int DEF_SIZE_OF_TABLE = DEF_TABLE_HEIGHT * DEF_TABLE_WIDTH * DEF_SIZE_OF_COLUMN_DATA;

    private TestColumnStoreUtils() {
        // Utility class
    }

    public static List<TestDoubleColumnData[]> generateDefaultTable() {
        return createTable(DEF_TABLE_HEIGHT, DEF_TABLE_WIDTH, DEF_SIZE_OF_COLUMN_DATA);
    }

    public static List<TestDoubleColumnData[]> generateDoubleSizedDefaultTable() {
        return createTable(DEF_TABLE_HEIGHT * 2, DEF_TABLE_WIDTH, DEF_SIZE_OF_COLUMN_DATA);
    }

    public static ColumnStoreSchema generateDefaultSchema() {
        return createSchema(DEF_TABLE_WIDTH);
    }

    public static ColumnStore generateDefaultTestColumnStore() {
        return new TestColumnStore(generateDefaultSchema(), DEF_SIZE_OF_COLUMN_DATA);
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

    public static int checkRefs(final List<TestDoubleColumnData[]> table) {
        if (table.size() == 0) {
            return 0;
        }
        final int refs = checkRefs(table.get(0));
        for (final TestDoubleColumnData[] batch : table) {
            assertEquals(refs, checkRefs(batch));
        }
        return refs;
    }

    public static int checkRefs(final TestDoubleColumnData[] batch) {
        if (batch.length == 0) {
            return 0;
        }
        final int refs = batch[0].getRefs();
        for (final TestDoubleColumnData data : batch) {
            assertEquals(refs, data.getRefs());
        }
        return refs;
    }

    public static List<TestDoubleColumnData[]> readAndCompareTable(final ColumnStore store,
        final List<TestDoubleColumnData[]> table) throws Exception {
        return readSelectionAndCompareTable(store, table, null);
    }

    public static List<TestDoubleColumnData[]> readSelectionAndCompareTable(final ColumnStore store,
        final List<TestDoubleColumnData[]> table, final int... indices) throws Exception {

        try (final ColumnDataReader reader = store.createReader(ColumnSelectionUtil.create(indices))) {
            assertEquals(table.size(), reader.getNumChunks());

            final List<TestDoubleColumnData[]> result = new ArrayList<>();
            for (int i = 0; i < reader.getNumChunks(); i++) {
                final TestDoubleColumnData[] written;
                if (indices == null) {
                    written = table.get(i);
                } else {
                    final TestDoubleColumnData[] all = table.get(i);
                    written = Arrays.stream(indices).mapToObj(index -> all[index]).toArray(TestDoubleColumnData[]::new);
                }
                final TestDoubleColumnData[] batch = Arrays.stream(reader.read(i))
                    .map(data -> (TestDoubleColumnData)data).toArray(TestDoubleColumnData[]::new);

                assertEquals(written.length, batch.length);
                for (int j = 0; j < written.length; j++) {
                    assertArrayEquals(written[j].get(), (batch[j]).get());
                }

                for (final ColumnData data : batch) {
                    data.release();
                }

                result.add(batch);
            }
            return result;
        }
    }

    public static void readTwiceAndCompareTable(final ColumnStore store) throws Exception {
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

    public static boolean tableInStore(final ColumnStore store, final List<TestDoubleColumnData[]> table)
        throws Exception {
        try (final ColumnDataReader reader = store.createReader()) {
        } catch (IllegalStateException e) {
            return false;
        }
        readAndCompareTable(store, table);
        return true;
    }

    public static void writeTable(final ColumnStore store, final List<TestDoubleColumnData[]> table) throws Exception {
        try (final ColumnDataWriter writer = store.getWriter()) {
            for (final ColumnData[] batch : table) {
                writer.write(batch);
            }
        }
    }

    public static List<TestDoubleColumnData[]> createTable(final int height, final int width, final int size) {
        return IntStream.range(0, height).mapToObj(i -> createBatch(width, size)).collect(Collectors.toList());
    }

    public static TestDoubleColumnData[] createBatch(final int width, final int size) {
        final TestDoubleColumnData[] batch =
            IntStream.range(0, width).mapToObj(i -> new TestDoubleColumnData()).toArray(TestDoubleColumnData[]::new);

        for (final TestDoubleColumnData data : batch) {
            data.ensureCapacity(size);
            for (int i = 0; i < size; i++) {
                data.setDouble(0, i);
            }
            data.setNumValues(size);
        }

        return batch;
    }

}
