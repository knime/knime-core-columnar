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
 *   19 Feb 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.stream.IntStream;

import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.data.ObjectData.ObjectWriteData;
import org.knime.core.columnar.filter.DefaultColumnSelection;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public final class TestDataBatchStoreUtils {

    static final int DEF_LENGTH = 4;

    public static final int DEF_CAPACITY = 2;

    static final String[] DEF_ROW_KEYS =
        IntStream.range(0, DEF_LENGTH).mapToObj(Integer::toString).toArray(String[]::new);

    static final int[] DEF_INTEGERS = IntStream.range(0, DEF_LENGTH).toArray();

    static final String[] DEF_STRINGS =
        IntStream.range(0, DEF_LENGTH).mapToObj(Integer::toString).toArray(String[]::new);

    public static DataTableSpec createSpec() {
        return new DataTableSpec(new DataColumnSpecCreator("int", IntCell.TYPE).createSpec(),
            new DataColumnSpecCreator("string", StringCell.TYPE).createSpec());
    }

    public static WriteBatch[] createBatches(final BatchWritable store, final int length, final int capacity) {
        return IntStream.range(0, length / capacity).mapToObj(i -> store.getWriter().create(capacity))
            .toArray(WriteBatch[]::new);
    }

    @SuppressWarnings("unchecked")
    public static void writeRowKeys(final WriteBatch[] batches, final String... rowKeys) {
        int i = 0;
        int j = 0;
        ObjectWriteData<String> data = (ObjectWriteData<String>)batches[i].get(0);
        i++;
        for (String rowKey : rowKeys) {
            if (j >= data.capacity()) {
                data = (ObjectWriteData<String>)batches[i].get(0);
                i++;
                j = 0;
            }
            data.setObject(j, rowKey);
            j++;
        }
    }

    public static void writeIntegers(final WriteBatch[] batches, final int... integers) {
        int i = 0;
        int j = 0;
        IntWriteData data = (IntWriteData)batches[i].get(1);
        i++;
        for (int integer : integers) {
            if (j >= data.capacity()) {
                data = (IntWriteData)batches[i].get(1);
                i++;
                j = 0;
            }
            data.setInt(j, integer);
            j++;
        }
    }

    @SuppressWarnings("unchecked")
    public static void writeStrings(final WriteBatch[] batches, final String... strings) {
        int i = 0;
        int j = 0;
        ObjectWriteData<String> data = (ObjectWriteData<String>)batches[i].get(2);
        i++;
        for (String string : strings) {
            if (j >= data.capacity()) {
                data = (ObjectWriteData<String>)batches[i].get(2);
                i++;
                j = 0;
            }
            data.setObject(j, string);
            j++;
        }
    }

    public static void closeWriteReleaseBatches(final BatchWritable store, final WriteBatch[] batches)
        throws IOException {
        try (final BatchWriter writer = store.getWriter()) {
            for (WriteBatch writeBatch : batches) {
                final ReadBatch readBatch = writeBatch.close(writeBatch.capacity());
                writer.write(readBatch);
                readBatch.release();
            }
        }
    }

    @SuppressWarnings("unchecked")
    static void readCompareReleaseRowKeys(final RandomAccessBatchReadable store, final String... rowKeys)
        throws IOException {
        try (final RandomAccessBatchReader reader =
            store.createReader(new DefaultColumnSelection(store.getSchema().numColumns()))) {
            int i = 0;
            int j = 0;
            ReadBatch batch = reader.readRetained(i);
            i++;
            for (String rowKey : rowKeys) {
                if (j >= batch.length()) {
                    batch.release();
                    batch = reader.readRetained(i);
                    i++;
                    j = 0;
                }
                assertEquals(rowKey, ((ObjectReadData<String>)batch.get(0)).getObject(j));
                j++;
            }
            batch.release();
        }
    }

    static void readCompareReleaseIntegers(final RandomAccessBatchReadable store, final int... integers)
        throws IOException {
        try (final RandomAccessBatchReader reader =
            store.createReader(new FilteredColumnSelection(store.getSchema().numColumns(), 1))) {
            int i = 0;
            int j = 0;
            IntReadData data = (IntReadData)reader.readRetained(i).get(1);
            i++;
            for (int integer : integers) {
                if (j >= data.length()) {
                    data.release();
                    data = (IntReadData)reader.readRetained(i).get(1);
                    i++;
                    j = 0;
                }
                assertEquals(integer, data.getInt(j));
                j++;
            }
            data.release();
        }
    }

    @SuppressWarnings("unchecked")
    static void readCompareReleaseStrings(final RandomAccessBatchReadable store, final String... strings)
        throws IOException {
        try (final RandomAccessBatchReader reader =
            store.createReader(new FilteredColumnSelection(store.getSchema().numColumns(), 2))) {
            int i = 0;
            int j = 0;
            ObjectReadData<String> data = (ObjectReadData<String>)reader.readRetained(i).get(2);
            i++;
            for (String string : strings) {
                if (j >= data.length()) {
                    data.release();
                    data = (ObjectReadData<String>)reader.readRetained(i).get(2);
                    i++;
                    j = 0;
                }
                assertEquals(string, data.getObject(j));
                j++;
            }
            data.release();
        }
    }

    public static void writeDefaultTable(final BatchWritable store) throws IOException {
        final WriteBatch[] batches = createBatches(store, DEF_LENGTH, DEF_CAPACITY);
        writeRowKeys(batches, DEF_ROW_KEYS);
        writeIntegers(batches, DEF_INTEGERS);
        writeStrings(batches, DEF_STRINGS);
        closeWriteReleaseBatches(store, batches);
    }

    public static void readDefaultTable(final RandomAccessBatchReadable store) throws IOException {
        readCompareReleaseRowKeys(store, DEF_ROW_KEYS);
        readCompareReleaseIntegers(store, DEF_INTEGERS);
        readCompareReleaseStrings(store, DEF_STRINGS);
    }

    private TestDataBatchStoreUtils() {
    }

}
