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
 *   Dec 22, 2023 (benjamin): created
 */
package org.knime.core.columnar.badger;

import static org.knime.core.columnar.BenchmarkUtils.createFileHandle;

import java.io.IOException;
import java.util.stream.IntStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.knime.core.columnar.BenchmarkUtils.MissingValues;
import org.knime.core.columnar.arrow.ArrowBatchStore;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.columnar.cursor.ColumnarWriteCursorFactory;
import org.knime.core.table.access.IntAccess.IntWriteAccess;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpecs;
import org.knime.core.table.schema.DataSpecs.DataSpecWithTraits;

/*
 * Benchmark Ideas:
 * - with one column to serialize and many not to serialize
 * - with many columns to serialize
 */
final class IntDataBenchmark {

    private static final int NUM_TRIALS = 10;

    // >>>>>>>>>>>>>>>>> BENCHMARK PARAMS
    private static final int[] NUM_ROWS_OPTIONS = new int[]{100, 10_000, 1_000_000};

    private static final int[] NUM_COLS_OPTIONS = new int[]{10, 100};

    private static final MissingValues[] MISSING_VALUES_OPTIONS = new MissingValues[]{MissingValues.NONE};

    private long m_numRows = 100;

    private int m_numCols = 10;

    private MissingValues m_missingValues = MissingValues.NONE;
    // <<<<<<<<<<<<<<<<<<<

    private static BufferAllocator m_allocator;

    private static ArrowColumnStoreFactory m_storeFactory;

    private ArrowBatchStore m_store;

    private WritableObjectCacheTestStore m_objectCache;

    @BeforeAll
    static void beforeAll() {
        m_allocator = new RootAllocator();
        m_storeFactory = new ArrowColumnStoreFactory(m_allocator, ArrowCompressionUtil.ARROW_LZ4_FRAME_COMPRESSION);
    }

    @AfterAll
    static void afterAll() {
        m_allocator.close();
    }

    // TODO setup before each trail
    void setup() throws IOException {
        var schema = ColumnarSchema
            .of(IntStream.range(0, m_numCols).mapToObj(i -> DataSpecs.INT).toArray(DataSpecWithTraits[]::new));
        var testFile = createFileHandle("benchIntColumns");
        m_store = m_storeFactory.createStore(schema, testFile);
        m_objectCache = new WritableObjectCacheTestStore(m_store);
    }

    // TODO tear down after each trail
    void tearDown() throws IOException {
        m_objectCache.close();
        m_store.close();
        m_store.getFileHandle().delete();
    }

    // TODO This is the benchmark method
    // @Benchmark
    void benchHeapBadger() throws IOException {
        var badger = new HeapBadger(m_store);
        try (var cursor = badger.getWriteCursor()) {
            var rowAccess = cursor.access();

            // Loop over rows
            for (long rowIdx = 0; rowIdx < m_numRows; rowIdx++) {
                cursor.forward();

                // Loop over columns
                for (int colIdx = 0; colIdx < m_numCols; colIdx++) {
                    var intAccess = (IntWriteAccess)rowAccess.getWriteAccess(colIdx);
                    var val = (int)(rowIdx + colIdx);
                    if (m_missingValues == MissingValues.ALL_
                        || (m_missingValues == MissingValues.SOME && val % 10 == 0)) {
                        intAccess.setMissing();
                    } else {
                        intAccess.setIntValue(val);
                    }
                }
            }
            cursor.flush();
        }
    }

    // TODO This is the benchmark method
    // @Benchmark
    void benchOldImpl() throws IOException {
        try (var cursor = ColumnarWriteCursorFactory.createWriteCursor(m_objectCache)) {
            var rowAccess = cursor.access();

            // Loop over rows
            for (long rowIdx = 0; rowIdx < m_numRows; rowIdx++) {
                cursor.forward();

                // Loop over columns
                for (int colIdx = 0; colIdx < m_numCols; colIdx++) {
                    var intAccess = (IntWriteAccess)rowAccess.getWriteAccess(colIdx);
                    var val = (int)(rowIdx + colIdx);
                    if (m_missingValues == MissingValues.ALL_
                        || (m_missingValues == MissingValues.SOME && val % 10 == 0)) {
                        intAccess.setMissing();
                    } else {
                        intAccess.setIntValue(val);
                    }
                }
            }
            cursor.flush();
            cursor.close();
            m_objectCache.flush();
        }
    }

    @Test
    void runBenchmarksHeapBadger() throws IOException {
        System.out.println("--------------------------------");
        System.out.println("IntData - HeapBadger implementation");
        runBenchmarks(() -> benchHeapBadger());
        System.out.println("--------------------------------");
    }

    @Test
    void runBenchmarksOldImpl() throws IOException {
        System.out.println("--------------------------------");
        System.out.println("IntData - Old implementation");
        runBenchmarks(() -> benchOldImpl());
        System.out.println("--------------------------------");
    }

    interface BenchmarkFn {
        void run() throws IOException;
    }

    void runBenchmarks(final BenchmarkFn benchmarkFn) throws IOException {

        for (var numRows : NUM_ROWS_OPTIONS) {
            m_numRows = numRows;
            for (var numCols : NUM_COLS_OPTIONS) {
                m_numCols = numCols;
                for (var missingValues : MISSING_VALUES_OPTIONS) {
                    m_missingValues = missingValues;

                    long totalTime = 0;
                    for (int i = 0; i < NUM_TRIALS; i++) {
                        setup();
                        long startTime = System.nanoTime();
                        benchmarkFn.run();
                        long endTime = System.nanoTime();
                        totalTime += endTime - startTime;
                        tearDown();
                    }

                    System.out.println(String.format("numRows: %7d, numCols: %4d, missing: %s, -- %10.2fms", numRows,
                        numCols, missingValues, (double)totalTime / NUM_TRIALS / 1e6));
                }
            }
        }
    }
}
