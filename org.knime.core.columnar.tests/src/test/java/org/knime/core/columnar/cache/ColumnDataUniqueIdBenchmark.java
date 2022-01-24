package org.knime.core.columnar.cache;

import static org.knime.core.columnar.TestBatchStoreUtils.createDefaultTestColumnStore;

import java.util.concurrent.TimeUnit;

import org.knime.core.columnar.store.BatchReadStore;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State( Scope.Benchmark )
@Warmup( iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS )
@Measurement( iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS )
@BenchmarkMode( Mode.AverageTime )
@OutputTimeUnit( TimeUnit.NANOSECONDS )
@Fork( 1 )
public class ColumnDataUniqueIdBenchmark {

    public static void main( String... args ) throws RunnerException
    {
        Options options = new OptionsBuilder().include( ColumnDataUniqueIdBenchmark.class.getSimpleName() ).build();
        new Runner( options ).run();
    }

    final BatchReadStore store = createDefaultTestColumnStore();
    final ColumnDataUniqueId id = new ColumnDataUniqueId(store, DataIndex.createColumnIndex(1).getChild(1), 0);
    final ColumnDataUniqueId eid = new ColumnDataUniqueId(store, DataIndex.createColumnIndex(1).getChild(1), 0);
    final ColumnDataUniqueIdOld id2 = new ColumnDataUniqueIdOld(store, DataIndexOld.createColumnIndex(1).getChild(1), 0);
    final ColumnDataUniqueIdOld eid2 = new ColumnDataUniqueIdOld(store, DataIndexOld.createColumnIndex(1).getChild(1), 0);

    @Benchmark
    public boolean benchmarkEquals()
    {
        return id.equals(eid);
    }

    @Benchmark
    public boolean benchmarkEquals2()
    {
        return id2.equals(eid2);
    }
}
