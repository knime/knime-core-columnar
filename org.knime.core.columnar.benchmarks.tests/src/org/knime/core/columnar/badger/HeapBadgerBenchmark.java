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
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.knime.core.columnar.BenchmarkUtils.MissingValues;
import org.knime.core.columnar.arrow.ArrowBatchStore;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.columnar.cursor.ColumnarWriteCursorFactory;
import org.knime.core.columnar.cursor.ColumnarWriteCursorFactory.ColumnarWriteCursor;
import org.knime.core.columnar.params.BatchingParam;
import org.knime.core.table.access.IntAccess.IntWriteAccess;
import org.knime.core.table.access.StringAccess.StringWriteAccess;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpecs;
import org.knime.core.table.schema.DataSpecs.DataSpecWithTraits;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmark the {@link HeapBadger} and compare it to the legacy batching implementation in multiple scenarios.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 10, timeUnit = TimeUnit.SECONDS)
@SuppressWarnings("javadoc")
public class HeapBadgerBenchmark {

    @State(Scope.Benchmark)
    public abstract static class BenchmarkState {

        @Param
        BatchingParam m_batching;

        @Param({"100", "10000"})
        long m_numRows;

        @Param({"10", "100"})
        int m_numCols;

        @Param({"NONE", "SOME"})
        MissingValues m_missingValues;

        BufferAllocator m_allocator;

        ArrowColumnStoreFactory m_storeFactory;

        ArrowBatchStore m_store;

        WritableObjectCacheTestStore m_objectCache;

        @Setup(Level.Iteration)
        public void setup() {
            m_allocator = new RootAllocator();
            m_storeFactory = new ArrowColumnStoreFactory(m_allocator, ArrowCompressionUtil.ARROW_LZ4_FRAME_COMPRESSION);
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            m_allocator.close();
        }

        abstract ColumnarSchema getSchema();

        /** Called before each call of the benchmark method */
        @Setup(Level.Invocation)
        public void setupInvocation() throws IOException {
            var schema = getSchema();
            var testFile = createFileHandle("bechmarkFile");
            m_store = m_storeFactory.createStore(schema, testFile);
            m_objectCache = new WritableObjectCacheTestStore(m_store);
        }

        /** Cleanup after each call of the benchmark method */
        @TearDown(Level.Invocation)
        public void tearDownInvocation() throws IOException {
            m_objectCache.close();
            m_store.close();
            m_store.getFileHandle().delete();
        }
    }

    /** State for benchmarking with primitive integer data. */
    @State(Scope.Benchmark)
    public static class IntBenchmarkState extends BenchmarkState {

        @Override
        ColumnarSchema getSchema() {
            return ColumnarSchema
                .of(IntStream.range(0, m_numCols).mapToObj(i -> DataSpecs.INT).toArray(DataSpecWithTraits[]::new));
        }
    }

    /** State for benchmarking with short string data. */
    @State(Scope.Benchmark)
    public static class StringBenchmarkState extends BenchmarkState {

        @Override
        ColumnarSchema getSchema() {
            return ColumnarSchema
                .of(IntStream.range(0, m_numCols).mapToObj(i -> DataSpecs.STRING).toArray(DataSpecWithTraits[]::new));
        }
    }

    // >>>>>>>>>>>>>>>>> PRIMITIVE DATA

    /** Benchmark writing integer data with different batchers. */
    @Benchmark
    public void intData(final IntBenchmarkState state) throws IOException {
        if (state.m_batching == BatchingParam.HEAP_BADGER) {
            var badger = new HeapBadger(state.m_store);
            try (var cursor = badger.getWriteCursor()) {
                var rowAccess = cursor.access();
                addIntData(state.m_numRows, state.m_numCols, state.m_missingValues, cursor, rowAccess);
            }
        } else {
            try (var cursor = ColumnarWriteCursorFactory.createWriteCursor(state.m_objectCache)) {
                var rowAccess = cursor.access();
                addIntData(state.m_numRows, state.m_numCols, state.m_missingValues, cursor, rowAccess);
                state.m_objectCache.flush();
            }
        }
    }

    private static void addIntData(final long numRows, final int numCols, final MissingValues missingValues,
        final ColumnarWriteCursor cursor, final WriteAccessRow rowAccess) throws IOException {
        // Loop over rows
        for (long rowIdx = 0; rowIdx < numRows; rowIdx++) {
            cursor.forward();

            // Loop over columns
            for (int colIdx = 0; colIdx < numCols; colIdx++) {
                var intAccess = (IntWriteAccess)rowAccess.getWriteAccess(colIdx);
                var val = (int)(rowIdx + colIdx);
                if (missingValues == MissingValues.ALL_ || (missingValues == MissingValues.SOME && val % 10 == 0)) {
                    intAccess.setMissing();
                } else {
                    intAccess.setIntValue(val);
                }
            }
        }
        cursor.finish();
    }

    // >>>>>>>>>>>>>>>>> SHORT STRING DATA

    private static final String[] VALUES = {"A", "B", "C", "D", "E", "F", "G", "H"};

    /** Benchmark writing short string data with different batchers. */
    @Benchmark
    public void shortStringData(final StringBenchmarkState state) throws IOException {
        if (state.m_batching == BatchingParam.HEAP_BADGER) {
            var badger = new HeapBadger(state.m_store);
            try (var cursor = badger.getWriteCursor()) {
                var rowAccess = cursor.access();
                addShortStringData(state.m_numRows, state.m_numCols, state.m_missingValues, cursor, rowAccess);
            }
        } else {
            try (var cursor = ColumnarWriteCursorFactory.createWriteCursor(state.m_objectCache)) {
                var rowAccess = cursor.access();
                addShortStringData(state.m_numRows, state.m_numCols, state.m_missingValues, cursor, rowAccess);
                state.m_objectCache.flush();
            }
        }
    }

    private static void addShortStringData(final long numRows, final int numCols, final MissingValues missingValues,
        final ColumnarWriteCursor cursor, final WriteAccessRow rowAccess) throws IOException {
        // Loop over rows
        for (long rowIdx = 0; rowIdx < numRows; rowIdx++) {
            cursor.forward();

            // Loop over columns
            for (int colIdx = 0; colIdx < numCols; colIdx++) {
                var stringAccess = (StringWriteAccess)rowAccess.getWriteAccess(colIdx);
                var val = VALUES[((int)rowIdx + colIdx) % VALUES.length];
                if (missingValues == MissingValues.ALL_ || (missingValues == MissingValues.SOME && rowIdx % 10 == 0)) {
                    stringAccess.setMissing();
                } else {
                    stringAccess.setStringValue(val);
                }
            }
        }
        cursor.finish();
    }

    // >>>>>>>>>>>>>>>>> SHORT + LONG STRING DATA

    static String randomString(final int length, final int seed) {
        var sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append((char)(i + seed));
        }
        return sb.toString();
    }

    static String[] randomStrings(final int length) {
        return IntStream.range(0, 10).mapToObj(i -> randomString(length, i)).toArray(String[]::new);
    }

    // 1 byte per value
    private static final String[] VALUES_SHORT = randomStrings(1);

    // 512 byte per value
    private static final String[] VALUES_MEDIUM = randomStrings(512);

    // 16 kilo-byte per value
    private static final String[] VALUES_LONG = randomStrings(1024 * 16);

    /** Benchmark writing short and long string data with different batchers. */
    @Benchmark
    public void shortLongStringData(final StringBenchmarkState state) throws IOException {
        if (state.m_batching == BatchingParam.HEAP_BADGER) {
            var badger = new HeapBadger(state.m_store);
            try (var cursor = badger.getWriteCursor()) {
                var rowAccess = cursor.access();
                addShortLongStringData(state.m_numRows, state.m_numCols, state.m_missingValues, cursor, rowAccess);
            }
        } else {
            try (var cursor = ColumnarWriteCursorFactory.createWriteCursor(state.m_objectCache)) {
                var rowAccess = cursor.access();
                addShortLongStringData(state.m_numRows, state.m_numCols, state.m_missingValues, cursor, rowAccess);
                state.m_objectCache.flush();
            }
        }
    }

    private static void addShortLongStringData(final long numRows, final int numCols, final MissingValues missingValues,
        final ColumnarWriteCursor cursor, final WriteAccessRow rowAccess) throws IOException {
        // Loop over rows
        for (long rowIdx = 0; rowIdx < numRows; rowIdx++) {
            cursor.forward();

            // Loop over columns
            for (int colIdx = 0; colIdx < numCols; colIdx++) {
                var stringAccess = (StringWriteAccess)rowAccess.getWriteAccess(colIdx);
                String val;
                if (rowIdx < numRows / 3) {
                    val = VALUES_SHORT[((int)rowIdx + colIdx) % VALUES_SHORT.length];
                } else if (rowIdx < (numRows / 3) * 2) {
                    val = VALUES_MEDIUM[((int)rowIdx + colIdx) % VALUES_MEDIUM.length];
                } else {
                    val = VALUES_LONG[((int)rowIdx + colIdx) % VALUES_LONG.length];
                }
                if (missingValues == MissingValues.ALL_ || (missingValues == MissingValues.SOME && rowIdx % 10 == 0)) {
                    stringAccess.setMissing();
                } else {
                    stringAccess.setStringValue(val);
                }
            }
        }
        cursor.finish();
    }
}
