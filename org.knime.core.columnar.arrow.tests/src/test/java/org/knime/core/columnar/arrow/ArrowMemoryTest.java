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

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Ignore;
import org.junit.Test;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.DataSpec;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.store.BatchReader;
import org.knime.core.columnar.store.BatchWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.columnar.store.ColumnStoreSchema;

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

        for (int l = 0; l < numLoops; l++) {
            pool.submit(() -> {
                final ColumnStoreFactory factory = new ArrowColumnStoreFactory();
                final ColumnStoreSchema schema =
                    ArrowTestUtils.createWideSchema(DataSpec.doubleSpec(), numColumns);

                try (final ColumnStore store =
                    factory.createStore(schema, ArrowTestUtils.createTmpKNIMEArrowFile())) {

                    storeData(numChunks, chunkSize, numColumns, store);

                    try (final ColumnStore copyStore =
                        factory.createStore(schema, ArrowTestUtils.createTmpKNIMEArrowFile())) {

                        copyData(numChunks, store, copyStore);
                    }
                }
                return null;
            }).get();
        }
    }

    /** Store some data into the column store */
    @SuppressWarnings("resource")
    private static void storeData(final int numChunks, final int chunkSize, final int numColumns,
        final ColumnStore store) throws IOException {
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
                writer.write(batch.close(chunkSize));
                batch.release();
            }
        }
    }

    /** Read some data from one column store and copy it to the other */
    private static void copyData(final int numChunks, final ColumnStore store, final ColumnStore copyStore)
        throws IOException {
        // let's read some data back
        try (final BatchReader reader = store.createReader();
                final BatchWriter copyWriter = copyStore.getWriter()) {
            for (int c = 0; c < numChunks; c++) {
                final ReadBatch batch = reader.readRetained(c);
                copyWriter.write(batch);
                batch.release();
            }
        }
    }
}
