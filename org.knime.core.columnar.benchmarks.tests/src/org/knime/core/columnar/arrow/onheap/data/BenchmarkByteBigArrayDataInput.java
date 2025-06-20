package org.knime.core.columnar.arrow.onheap.data;

import java.io.IOException;
import java.util.Random;
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
public class BenchmarkByteBigArrayDataInput {

    private static final int SIZE = 1024 * 1024;

    private ByteBigArrayBigList data;

    private ByteBigArrayBigList stringData;

    private int i;

    private ByteBigArrayDataInput input;

    private int j;

    private ByteBigArrayDataInput stringInput;

    public BenchmarkByteBigArrayDataInput() {
        try {
            data = new ByteBigArrayBigList();
            ByteBigListDataOutput out = new ByteBigListDataOutput(data);
            final byte[] bytes = new byte[SIZE];
            new Random().nextBytes(bytes);
            out.write(bytes);

            stringData = new ByteBigArrayBigList();
            out = new ByteBigListDataOutput(stringData);
            final String string = "a simple test string\n" //
                + "another\r line \r\n" //
                + "and a third line\n" //
                + "finally, the fourth line is a bit longer than the previous lines (but still not very long)\n";
            for (int i = 0; i < 10000; ++i) {
                out.writeBytes(string);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Setup(Level.Iteration)
    //	@Setup(Level.Invocation)
    public void setup() throws IOException {
        reset();
        resetStringInput();
    }

    private void reset() {
        input = new ByteBigArrayDataInput(data.elements(), 0, SIZE);
        i = 0;
    }

    @Benchmark
    public short benchmarkReadShort() throws IOException {
        if (++i >= SIZE / 2) {
            reset();
        }
        return input.readShort();
    }

    @Benchmark
    public int benchmarkReadUnsignedShort() throws IOException {
        if (++i >= SIZE / 2) {
            reset();
        }
        return input.readUnsignedShort();
    }

    @Benchmark
    public int benchmarkReadInt() throws IOException {
        if (++i >= SIZE / 4) {
            reset();
        }
        return input.readInt();
    }

    @Benchmark
    public long benchmarkReadLong() throws IOException {
        if (++i >= SIZE / 8) {
            reset();
        }
        return input.readLong();
    }

    @Benchmark
    public byte benchmarkReadByte() throws IOException {
        if (++i >= SIZE) {
            reset();
        }
        return input.readByte();
    }

    @Benchmark
    public int benchmarkReadUnsignedByte() throws IOException {
        if (++i >= SIZE) {
            reset();
        }
        return input.readUnsignedByte();
    }

    @Benchmark
    public float benchmarkReadFloat() throws IOException {
        if (++i >= SIZE / 4) {
            reset();
        }
        return input.readFloat();
    }

    @Benchmark
    public double benchmarkReadDouble() throws IOException {
        if (++i >= SIZE / 8) {
            reset();
        }
        return input.readDouble();
    }

    private void resetStringInput() {
        stringInput = new ByteBigArrayDataInput(stringData.elements(), 0, stringData.size64());
        j = 0;
    }

    @Benchmark
    public String benchmarkReadLine() throws IOException {
        if (++j > 40000) {
            resetStringInput();
        }
        return stringInput.readLine();
    }

    public static void main(final String... args) throws RunnerException {
        Options options = new OptionsBuilder().include(BenchmarkByteBigArrayDataInput.class.getSimpleName()).build();
        new Runner(options).run();
    }
}
