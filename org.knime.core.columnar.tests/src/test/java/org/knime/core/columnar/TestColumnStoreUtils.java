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

import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.ColumnDataSpec;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.data.DoubleData.DoubleDataSpec;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.data.ObjectData.ObjectWriteData;
import org.knime.core.columnar.data.StringData.StringDataSpec;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnDataWriter;
import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.ColumnStoreSchema;
import org.knime.core.columnar.testing.TestColumnStore;
import org.knime.core.columnar.testing.data.TestData;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public final class TestColumnStoreUtils {

    private static int RUNNING_INT = 0;

    public static final class TestDataTable implements Closeable {

        private final List<TestData[]> m_batches;

        private TestDataTable(final List<TestData[]> batches) {
            m_batches = batches;
        }

        public TestData[] getBatch(final int index) {
            return m_batches.get(index);
        }

        @Override
        public void close() {
            for (TestData[] batch : m_batches) {
                for (TestData data : batch) {
                    if (data != null) {
                        data.release();
                    }
                }
            }
        }

        int size() {
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

    private static ColumnStoreSchema createSchema(final int numColumns) {
        return new ColumnStoreSchema() {
            @Override
            public int getNumColumns() {
                return numColumns;
            }

            @Override
            public ColumnDataSpec getColumnDataSpec(final int index) {
                return index % 2 == 0 ? DoubleDataSpec.INSTANCE : StringDataSpec.INSTANCE;
            }
        };
    }

    private static ColumnStoreSchema createDefaultSchema() {
        return createSchema(DEF_NUM_COLUMNS);
    }

    public static TestColumnStore createDefaultTestColumnStore() {
        return TestColumnStore.create(createDefaultSchema());
    }

    private static ColumnWriteData[] createBatch(final ColumnStore store) {
        final WriteBatch batch = store.getFactory().create(DEF_SIZE_OF_DATA);
        final ColumnWriteData[] data = new ColumnWriteData[store.getSchema().getNumColumns()];

        for (int i = 0; i < store.getSchema().getNumColumns(); i++) {
            for (int j = 0; j < batch.capacity(); j++) {
                final ColumnDataSpec colSpec = store.getSchema().getColumnDataSpec(i);
                if (colSpec == DoubleDataSpec.INSTANCE) {
                    final DoubleWriteData doubleData = ((DoubleWriteData)batch.get(i));
                    doubleData.setDouble(j, RUNNING_INT++);
                    data[i] = doubleData;
                } else if (colSpec == StringDataSpec.INSTANCE) {
                    @SuppressWarnings("unchecked")
                    final ObjectWriteData<String> stringData = ((ObjectWriteData<String>)batch.get(i));
                    stringData.setObject(j, Integer.toString(RUNNING_INT++));
                    data[i] = stringData;
                } else {
                    throw new UnsupportedOperationException("Not yet implemented.");
                }
            }
        }
        batch.close(batch.capacity());

        return data;
    }

    private static List<ColumnWriteData[]> createWriteTable(final ColumnStore store, final int numBatches) {
        return IntStream.range(0, numBatches).mapToObj(i -> createBatch(store)).collect(Collectors.toList());
    }

    public static TestDataTable createEmptyTestTable(final TestColumnStore store) {
        return new TestDataTable(createTestTable(store, 0));
    }

    public static TestDataTable createDefaultTestTable(final TestColumnStore store) {
        return new TestDataTable(createTestTable(store, DEF_NUM_BATCHES));
    }

    public static TestDataTable createDoubleSizedDefaultTestTable(final TestColumnStore store) {
        return new TestDataTable(createTestTable(store, 2 * DEF_NUM_BATCHES));
    }

    private static List<TestData[]> createTestTable(final TestColumnStore store, final int numBatches) {
        return IntStream.range(0, numBatches)
            .mapToObj(i -> Arrays.stream(createBatch(store)).map(d -> (TestData)d).toArray(TestData[]::new))
            .collect(Collectors.toList());
    }

    private static int checkRefs(final TestData[] batch) {
        if (batch.length == 0) {
            return 0;
        }
        final int refs = batch[0].getRefs();
        for (final TestData data : batch) {
            assertEquals(refs, data.getRefs());
        }
        return refs;
    }

    public static int checkRefs(final TestDataTable table) {
        if (table.size() == 0) {
            return 0;
        }
        final int refs = checkRefs(table.getBatch(0));
        for (int i = 0; i < table.size(); i++) {
            assertEquals(refs, checkRefs(table.getBatch(i)));
        }
        return refs;
    }

    public static List<ColumnReadData[]> writeDefaultTable(final ColumnStore store) throws IOException {
        return writeTable(store, createWriteTable(store, DEF_NUM_BATCHES));
    }

    private static List<ColumnReadData[]> writeTable(final ColumnStore store, final List<ColumnWriteData[]> table)
        throws IOException {
        List<ColumnReadData[]> result = new ArrayList<>();
        try (final ColumnDataWriter writer = store.getWriter()) {
            for (WriteData[] writeBatch : table) {
                final ColumnReadData[] readBatch =
                    Arrays.stream(writeBatch).map(d -> d.close(d.capacity())).toArray(ColumnReadData[]::new);
                result.add(readBatch);
                writer.write(new DefaultReadBatch(readBatch, readBatch[0].length()));
            }
        }
        return result;
    }

    public static void writeTable(final ColumnStore store, final TestDataTable table) throws IOException {
        try (final ColumnDataWriter writer = store.getWriter()) {
            for (int i = 0; i < table.size(); i++) {
                final TestData[] batch = table.getBatch(i);
                final int length = batch[0].length();
                writer.write(new DefaultReadBatch(batch, length));
            }
        }
    }

    public static boolean tableInStore(final ColumnStore store, final TestDataTable table) throws IOException {
        try (final ColumnDataReader reader = store.createReader()) {
        } catch (IllegalStateException e) {
            return false;
        }
        try (final TestDataTable reassembledTable = readAndCompareTable(store, table)) {
        }
        return true;
    }

    public static TestDataTable readAndCompareTable(final ColumnReadStore store, final TestDataTable table)
        throws IOException {
        return readSelectionAndCompareTable(store, table, null);
    }

    public static TestDataTable readSelectionAndCompareTable(final ColumnReadStore store, final TestDataTable table,
        final int... indices) throws IOException {

        try (final ColumnDataReader reader = indices == null ? store.createReader()
            : store.createReader(new FilteredColumnSelection(store.getSchema().getNumColumns(), indices))) {
            assertEquals(table.size(), reader.getNumBatches());

            final List<TestData[]> result = new ArrayList<>();
            for (int i = 0; i < reader.getNumBatches(); i++) {
                final TestData[] written = table.getBatch(i);
                final ReadBatch batch = reader.readRetained(i);
                final TestData[] data = new TestData[store.getSchema().getNumColumns()];

                assertEquals(written.length, data.length);

                if (indices == null) {
                    for (int j = 0; j < written.length; j++) {
                        data[j] = (TestData)batch.get(j);
                        assertArrayEquals(written[j].get(), data[j].get());
                    }
                } else {
                    for (int j : indices) {
                        data[j] = (TestData)batch.get(j);
                        assertArrayEquals(written[j].get(), data[j].get());
                    }
                }

                result.add(data);
            }
            return new TestDataTable(result);
        }
    }

    public static void readAndCompareTable(final ColumnReadStore store, final List<ColumnReadData[]> table)
        throws IOException {
        readSelectionAndCompareTable(store, table, null);
    }

    public static void readSelectionAndCompareTable(final ColumnReadStore store, final List<ColumnReadData[]> table,
        final int... indices) throws IOException {

        try (final ColumnDataReader reader = indices == null ? store.createReader()
            : store.createReader(new FilteredColumnSelection(store.getSchema().getNumColumns(), indices))) {
            assertEquals(table.size(), reader.getNumBatches());

            for (int i = 0; i < reader.getNumBatches(); i++) {
                final ColumnReadData[] refBatch = table.get(i);
                final ReadBatch readBatch = reader.readRetained(i);

                assertEquals(refBatch.length, readBatch.length());
                final int[] indicesNonNull =
                    indices == null ? IntStream.range(0, store.getSchema().getNumColumns()).toArray() : indices;
                for (int j : indicesNonNull) {
                    final ColumnDataSpec colSpec = store.getSchema().getColumnDataSpec(j);
                    final ColumnReadData refData = refBatch[j];
                    final ColumnReadData readData = readBatch.get(j);
                    final int length = refData.length();
                    assertEquals(length, readData.length());

                    for (int k = 0; k < length; k++) {
                        if (colSpec == DoubleDataSpec.INSTANCE) {
                            assertEquals(((DoubleReadData)refData).getDouble(k),
                                ((DoubleReadData)readData).getDouble(k), 0d);
                        } else if (colSpec == StringDataSpec.INSTANCE) {
                            @SuppressWarnings("unchecked")
                            final ObjectReadData<String> refObjectData = (ObjectReadData<String>)refData;
                            @SuppressWarnings("unchecked")
                            final ObjectReadData<String> readObjectData = (ObjectReadData<String>)readData;
                            assertEquals(refObjectData.getObject(k), readObjectData.getObject(k));
                        } else {
                            throw new UnsupportedOperationException("Not yet implemented.");
                        }
                    }
                }

                readBatch.release();
            }
        }
    }

    public static void releaseTable(final List<ColumnReadData[]> table) {
        for (ColumnReadData[] batch : table) {
            for (ColumnReadData data : batch) {
                data.release();
            }
        }
    }

    public static void readTwiceAndCompareTable(final ColumnReadStore store) throws IOException {
        try (final ColumnDataReader reader1 = store.createReader();
                final ColumnDataReader reader2 = store.createReader()) {
            assertEquals(reader1.getNumBatches(), reader2.getNumBatches());
            for (int i = 0; i < reader1.getNumBatches(); i++) {
                final ReadBatch batch1 = reader1.readRetained(i);
                final ReadBatch batch2 = reader2.readRetained(i);

                assertEquals(batch1.length(), batch2.length());
                for (int j = 0; j < batch1.length(); j++) {
                    final ColumnDataSpec colSpec = store.getSchema().getColumnDataSpec(j);
                    final ColumnReadData data1 = batch1.get(j);
                    final ColumnReadData data2 = batch2.get(j);
                    final int length = data1.length();
                    assertEquals(length, data2.length());

                    for (int k = 0; k < length; k++) {
                        if (colSpec == DoubleDataSpec.INSTANCE) {
                            assertEquals(((DoubleReadData)data1).getDouble(k), ((DoubleReadData)data2).getDouble(k),
                                0d);
                        } else if (colSpec == StringDataSpec.INSTANCE) {
                            @SuppressWarnings("unchecked")
                            final ObjectReadData<String> writtenData = (ObjectReadData<String>)data1;
                            @SuppressWarnings("unchecked")
                            final ObjectReadData<String> readData = (ObjectReadData<String>)data2;
                            assertEquals(writtenData.getObject(k), readData.getObject(k));
                        } else {
                            throw new UnsupportedOperationException("Not yet implemented.");
                        }
                    }
                }

                batch1.release();
                batch2.release();
            }
        }
    }

}
