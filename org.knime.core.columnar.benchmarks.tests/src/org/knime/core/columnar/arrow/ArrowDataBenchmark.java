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
 *   Nov 29, 2024 (benjamin): created
 */
package org.knime.core.columnar.arrow;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.RootAllocator;
import org.knime.core.columnar.BenchmarkUtils;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.store.FileHandle;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.DefaultDataTraits;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 10, timeUnit = TimeUnit.SECONDS)
@SuppressWarnings("javadoc")
public class ArrowDataBenchmark {

    // TODO make 2 benchmarks one that includes setting the data and one that only measures the writing
    // Currently, encoding strings moved from setting the data to writing the data
    // I would like to know the impact of this change but maybe not all benchmarks are needed in the future

    //    ReadBatch[] fillDataManyRows() {
    //    }

    //    public static enum DataSets {
    //        MANY_ROWS, MANY_COLUMNS;
    //    }

    private static ColumnarSchema MANY_ROWS_SCHEMA =
        new DefaultColumnarSchema(new DataSpec[]{DataSpec.intSpec(), DataSpec.intSpec()},
            new DataTraits[]{DefaultDataTraits.EMPTY, DefaultDataTraits.EMPTY});

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        RootAllocator allocator;

        FileHandle file;

        ArrowBatchStore batchStore;

        BatchWriter writer;

        @Setup(Level.Invocation)
        public void setup() throws IOException {
            allocator = new RootAllocator();
            file = BenchmarkUtils.createFileHandle("arrowWriterBenchmark");

            var columnStoreFactory =
                new ArrowColumnStoreFactory(allocator, ArrowCompressionUtil.ARROW_LZ4_FRAME_COMPRESSION);
            batchStore = columnStoreFactory.createStore(MANY_ROWS_SCHEMA, file);
            writer = batchStore.getWriter();
        }

        @TearDown(Level.Invocation)
        public void tearDown() throws IOException {
            batchStore.close();
            allocator.close();
            file.delete();
        }

    }

    @State(Scope.Benchmark)
    public static class BenchmarkStateWithData extends BenchmarkState {

        ReadBatch[] data;

        @Setup(Level.Invocation)
        public void setupData() throws IOException {
            data = fillDataManyRows(writer);
        }
    }

    @State(Scope.Benchmark)
    public static class BenchmarkStateWritten extends BenchmarkStateWithData {

        RandomAccessBatchReader reader;

        @Setup(Level.Invocation)
        public void setupWrite() throws IOException {
            writeData(writer, data);
            reader = batchStore.createRandomAccessReader();
        }
    }

    @Benchmark
    public void writeToDisk(final BenchmarkStateWithData state) throws IOException {
        writeData(state.writer, state.data);
    }

    // TODO This benchmark is important for comparison between off-heap and on-heap
    // It can be deleted when the decision is made
    @Benchmark
    public void createAndWriteArrowData(final BenchmarkState state) throws IOException {
        var data = fillDataManyRows(state.writer);
        writeData(state.writer, data);
    }

    @Benchmark
    public void readFromDisk(final BenchmarkStateWritten state) throws IOException {
        readData(state.reader, state.batchStore.numBatches());
    }

    @Benchmark
    public void readAndUseFromDisk(final BenchmarkStateWritten state, final Blackhole bh) throws IOException {
        useManyRows(state.reader, state.batchStore.numBatches(), bh);
    }

    // ==== Utities

    private static void writeData(final BatchWriter writer, final ReadBatch[] data) throws IOException {
        for (var batch : data) {
            writer.write(batch);
            batch.release();
        }
        writer.close();
    }

    private static void readData(final RandomAccessBatchReader reader, final int numBatches) throws IOException {
        for (int i = 0; i < numBatches; i++) {
            var batch = reader.readRetained(i);
            batch.release();
        }
    }

    // Filling data

    private static void useManyRows(final RandomAccessBatchReader reader, final int numBatches, final Blackhole bh)
        throws IOException {
        for (int i = 0; i < numBatches; i++) {
            var batch = reader.readRetained(i);
            for (int c = 0; c < batch.numData(); c++) {
                var col = batch.get(c);
                for (int r = 0; r < col.length(); r++) {
                    bh.consume(((IntReadData)col).getInt(r));
                }
            }
            batch.release();
        }
    }

    private static ReadBatch[] fillDataManyRows(final BatchWriter writer) {
        // 2 Mio Integers
        var numRowsPerBatch = 100_000;
        var numCols = 2;
        var numBatches = 10;

        var data = new ReadBatch[numBatches];
        for (int i = 0; i < numBatches; i++) {
            var writeBatch = writer.create(numRowsPerBatch);

            for (int c = 0; c < numCols; c++) {
                var col = writeBatch.get(c);
                for (int r = 0; r < numRowsPerBatch; r++) {
                    ((IntWriteData)col).setInt(r, i + c + r);
                }
            }

            data[i] = writeBatch.close(numRowsPerBatch);
        }
        return data;
    }
}
