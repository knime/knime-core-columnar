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
import java.util.stream.IntStream;

import org.apache.arrow.memory.RootAllocator;
import org.knime.core.columnar.BenchmarkUtils;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.params.OnHeapParam;
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
import org.openjdk.jmh.annotations.Param;
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
@Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 10, timeUnit = TimeUnit.SECONDS)
@SuppressWarnings("javadoc")
public class ArrowDataBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        @Param
        DataSets dataSet;

        // NOTE: Some of the benchmarks are important for the comparison between on-heap and off-heap storage
        // They can be deleted (or skipped) after we settled on an implementation
        @Param
        OnHeapParam onHeap;

        RootAllocator allocator;

        FileHandle file;

        ArrowBatchStore batchStore;

        BatchWriter writer;

        @Setup(Level.Invocation)
        public void setup() throws IOException {
            onHeap.apply();
            allocator = new RootAllocator();
            file = BenchmarkUtils.createFileHandle("arrowWriterBenchmark");

            var columnStoreFactory =
                new ArrowColumnStoreFactory(allocator, ArrowCompressionUtil.ARROW_LZ4_FRAME_COMPRESSION);
            batchStore = columnStoreFactory.createStore(dataSet.getSchema(), file);
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
            data = dataSet.fillData(writer);
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

    @Benchmark
    public void createAndWriteArrowData(final BenchmarkState state) throws IOException {
        var data = state.dataSet.fillData(state.writer);
        writeData(state.writer, data);
    }

    @Benchmark
    public void readFromDisk(final BenchmarkStateWritten state) throws IOException {
        readData(state.reader, state.batchStore.numBatches());
    }

    @Benchmark
    public void readAndUseFromDisk(final BenchmarkStateWritten state, final Blackhole bh) throws IOException {
        state.dataSet.useData(state.reader::readRetained, state.batchStore.numBatches(), bh);
    }

    @Benchmark
    public void createAndUseInMemory(final BenchmarkState state, final Blackhole bh) throws IOException {
        var data = state.dataSet.fillData(state.writer);
        state.dataSet.useData(i -> data[i], data.length, bh);
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

    interface BatchProvider {
        ReadBatch get(final int batchIndex) throws IOException;
    }

    interface DataFiller {
        ReadBatch[] fillData(final BatchWriter writer);
    }

    interface DataUser {
        void useData(final BatchProvider reader, final int numBatches, final Blackhole bh) throws IOException;
    }

    private static String[] STRING_TEST_DATA =
        IntStream.range(0, 100_000).mapToObj(i -> "Test String " + i).toArray(String[]::new);

    public static enum DataSets implements DataFiller, DataUser {
            MANY_ROWS( //
                new DefaultColumnarSchema( //
                    new DataSpec[]{DataSpec.intSpec(), DataSpec.intSpec()}, //
                    new DataTraits[]{DefaultDataTraits.EMPTY, DefaultDataTraits.EMPTY} //
                ),

                (writer) -> {
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
                },

                (reader, numBatches, bh) -> {
                    for (int i = 0; i < numBatches; i++) {
                        var batch = reader.get(i);
                        for (int c = 0; c < batch.numData(); c++) {
                            var col = batch.get(c);
                            for (int r = 0; r < col.length(); r++) {
                                bh.consume(((IntReadData)col).getInt(r));
                            }
                        }
                        batch.release();
                    }
                }), //

            STRINGS( //
                new DefaultColumnarSchema( //
                    new DataSpec[]{DataSpec.stringSpec()}, //
                    new DataTraits[]{DefaultDataTraits.EMPTY} //
                ),

                (writer) -> {
                    // 1 Mio Strings
                    var numRowsPerBatch = 100_000;
                    var numCols = 1;
                    var numBatches = 10;

                    var data = new ReadBatch[numBatches];
                    for (int i = 0; i < numBatches; i++) {
                        var writeBatch = writer.create(numRowsPerBatch);

                        for (int c = 0; c < numCols; c++) {
                            var col = writeBatch.get(c);
                            for (int r = 0; r < numRowsPerBatch; r++) {
                                ((StringWriteData)col).setString(r, STRING_TEST_DATA[i % STRING_TEST_DATA.length]);
                            }
                        }

                        data[i] = writeBatch.close(numRowsPerBatch);
                    }
                    return data;
                },

                (reader, numBatches, bh) -> {
                    for (int i = 0; i < numBatches; i++) {
                        var batch = reader.get(i);
                        for (int c = 0; c < batch.numData(); c++) {
                            var col = batch.get(c);
                            for (int r = 0; r < col.length(); r++) {
                                bh.consume(((StringReadData)col).getString(r));
                            }
                        }
                        batch.release();
                    }
                }), //
        ;

        private ColumnarSchema m_schema;

        private DataFiller m_filler;

        private DataUser m_user;

        private DataSets(final ColumnarSchema schema, final DataFiller filler, final DataUser user) {
            m_schema = schema;
            m_filler = filler;
            m_user = user;
        }

        public ColumnarSchema getSchema() {
            return m_schema;
        }

        @Override
        public ReadBatch[] fillData(final BatchWriter writer) {
            return m_filler.fillData(writer);
        }

        @Override
        public void useData(final BatchProvider reader, final int numBatches, final Blackhole bh) throws IOException {
            m_user.useData(reader, numBatches, bh);

        }
    }
}
