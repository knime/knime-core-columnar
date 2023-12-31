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
package org.knime.core.columnar.arrow;

import static org.knime.core.table.schema.DataSpecs.DOUBLE;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.arrow.memory.RootAllocator;
import org.junit.Ignore;
import org.junit.Test;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpecs.DataSpecWithTraits;

/**
 * A long running test allocating, writing and reading many chunks.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public class ArrowMemoryTest {

    /**
     * A long running test allocating, writing and reading many chunks.
     *
     * @throws Exception
     */
    @Ignore // Ignored because long running
    @Test
    public void testCopyManyChunksParallel() throws Exception { // NOSONAR
        final int numChunks = 196;
        final int chunkSize = 16_000;
        final int numColumns = 33;
        final int nThreads = 32;
        final int numLoops = 128;

        final ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        try (var allocator = new RootAllocator()) {
            final ColumnStoreFactory factory =
                new ArrowColumnStoreFactory(allocator, ArrowCompressionUtil.ARROW_LZ4_FRAME_COMPRESSION);

            for (int l = 0; l < numLoops; l++) {
                pool.submit(() -> {
                    final ColumnarSchema schema = createWideSchema(DOUBLE, numColumns);

                    try (final BatchStore store =
                        factory.createStore(schema, ArrowTestUtils.createTmpKNIMEArrowFileSupplier())) {

                        storeData(numChunks, chunkSize, numColumns, store);

                        try (final BatchStore copyStore =
                            factory.createStore(schema, ArrowTestUtils.createTmpKNIMEArrowFileSupplier())) {

                            copyData(numChunks, store, copyStore);
                        }
                    }
                    return null;
                }).get();
            }
        }
    }

    /** Store some data into the column store */
    @SuppressWarnings("resource")
    private static void storeData(final int numChunks, final int chunkSize, final int numColumns,
        final BatchStore store) throws IOException {
        // let's store some data
        try (final BatchWriter writer = store.getWriter()) {
            for (int c = 0; c < numChunks; c++) {
                final WriteBatch batch = store.getWriter().create(chunkSize);
                for (int i = 0; i < numColumns; i++) {
                    final NullableWriteData data = batch.get(i);
                    for (int j = 0; j < chunkSize; j++) {
                        ((DoubleWriteData)data).setDouble(j, j);
                    }
                }
                var readBatch = batch.close(chunkSize);
                writer.write(readBatch);
                readBatch.release();
            }
        }
    }

    /** Read some data from one column store and copy it to the other */
    private static void copyData(final int numChunks, final BatchStore store, final BatchStore copyStore)
        throws IOException {
        // let's read some data back
        try (final RandomAccessBatchReader reader = store.createRandomAccessReader();
                final BatchWriter copyWriter = copyStore.getWriter()) {
            for (int c = 0; c < numChunks; c++) {
                final ReadBatch batch = reader.readRetained(c);
                copyWriter.write(batch);
                batch.release();
            }
        }
    }

    /** Create a schema with the given type multiple times. */
    private static ColumnarSchema createWideSchema(final DataSpecWithTraits type, final int width) {
        final DataSpecWithTraits[] types = new DataSpecWithTraits[width];
        for (int i = 0; i < width; i++) {
            types[i] = type;
        }
        return ColumnarSchema.of(types);
    }
}
