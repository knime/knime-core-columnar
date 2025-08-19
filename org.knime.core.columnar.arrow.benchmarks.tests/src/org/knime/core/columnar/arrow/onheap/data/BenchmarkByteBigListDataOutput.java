package org.knime.core.columnar.arrow.onheap.data;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import it.unimi.dsi.fastutil.bytes.ByteBigArrayBigList;

@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 50, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 50, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
public class BenchmarkByteBigListDataOutput {

    private ByteBigListDataOutput out;

    private int value;

    private byte[] values;

    private String string;

    @Setup(Level.Iteration)
    //	@Setup(Level.Invocation)
    public void setup() {
        out = new ByteBigListDataOutput(new ByteBigArrayBigList());
        value = 123456789;
        values = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        string = "a simple test string to serialize";
    }

    @Benchmark
    public void benchmarkWrite() throws IOException {
        out.write(value);
    }

    @Benchmark
    public void benchmarkWriteArray() throws IOException {
        out.write(values);
    }

    @Benchmark
    public void benchmarkWriteArraySlice() throws IOException {
        out.write(values, 2, 4);
    }

    @Benchmark
    public void benchmarkWriteBoolean() throws IOException {
        out.writeBoolean(true);
    }

    @Benchmark
    public void benchmarkWriteByte() throws IOException {
        out.writeByte(value);
    }

    @Benchmark
    public void benchmarkWriteShort() throws IOException {
        out.writeShort(value);
    }

    @Benchmark
    public void benchmarkWriteChar() throws IOException {
        out.writeChar(value);
    }

    @Benchmark
    public void benchmarkWriteInt() throws IOException {
        out.writeInt(value);
    }

    @Benchmark
    public void benchmarkWriteLong() throws IOException {
        out.writeLong(value);
    }

    @Benchmark
    public void benchmarkWriteFloat() throws IOException {
        out.writeFloat(value);
    }

    @Benchmark
    public void benchmarkWriteDouble() throws IOException {
        out.writeDouble(value);
    }

    @Benchmark
    public void benchmarkWriteStringBytes() throws IOException {
        out.writeBytes(string);
    }

    @Benchmark
    public void benchmarkWriteStringChars() throws IOException {
        out.writeChars(string);
    }

    public static void main(final String... args) throws RunnerException {
        Options options = new OptionsBuilder().include(BenchmarkByteBigListDataOutput.class.getSimpleName()).build();
        new Runner(options).run();
    }
}
