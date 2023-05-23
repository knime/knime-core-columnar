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
 *   Sep 1, 2021 (Carsten Haubold, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.arrow;

import static org.junit.Assert.assertTrue;
import static org.knime.core.table.schema.DataSpecs.VARBINARY;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Ignore;
import org.junit.Test;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.columnar.store.FileHandle;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Here we try to recreate a situation in which we saw crashes in Arrow operations.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
@Ignore
public class ArrowConcurrentReallocTest {

    @Test(expected = ExecutionException.class)
    public void testStringReallocAndSetSafe() throws Exception {
        ArrowConcurrentReallocTest.<VarCharVector> testConcurrentReallocationAndModification(//
            MinorType.VARCHAR.getType(), //
            v -> v.allocateNew(1), //
            (v, i) -> v.setSafe(i, "foobar".getBytes()));
    }

    @Test(expected = ExecutionException.class)
    public void testIntConcurrentReallocAndModification() throws Exception {
        ArrowConcurrentReallocTest.<IntVector> testConcurrentReallocationAndModification(//
            MinorType.INT.getType(), //
            ValueVector::allocateNew, //
            (v, i) -> v.set(i, i + 1));
    }

    @Test(expected = ExecutionException.class)
    public void testDoubleConcurrentReallocAndModification() throws Exception {
        ArrowConcurrentReallocTest.<Float8Vector> testConcurrentReallocationAndModification(//
            MinorType.FLOAT8.getType(), //
            ValueVector::allocateNew, //
            (v, i) -> v.set(i, i + 1));
    }

    @SuppressWarnings("resource")
    private static <V extends ValueVector> void testConcurrentReallocationAndModification(final ArrowType type,
        final Consumer<V> initializer, final BiConsumer<V, Integer> modifier)
        throws InterruptedException, ExecutionException {
        try {
            var field = Field.nullable("vector", type);
            for (int j = 0; j < 500; j++) {
                try (var allocator = new RootAllocator(); @SuppressWarnings("unchecked")
                final V vector = (V)field.createVector(allocator)) {
                    initializer.accept(vector);
                    var writeFuture = CompletableFuture.runAsync(() -> {
                        for (int i = 0; i < 100; i++) {
                            System.out.println("Modify: " + i);
                            modifier.accept(vector, i);
                        }
                    });
                    for (int i = 0; i < 5; i++) {
                        System.out.println("Realloc: " + i);
                        vector.reAlloc();
                    }
                    writeFuture.get();
                }
            }
        } catch (IllegalStateException e) {
            // so we can expect one of two exception types
            throw new ExecutionException(e);
        } catch (IndexOutOfBoundsException e) {
            // so we can expect one of two exception types
            throw new ExecutionException(e);
        }
    }

    @Test(expected = ExecutionException.class)
    public void testConcurrentRealloc() throws Exception {
        try {
            var field = Field.nullable("vector", MinorType.LARGEVARBINARY.getType());
            for (int j = 0; j < 50; j++) {
                try (var bufferAllocator = new RootAllocator(); @SuppressWarnings("resource")
                final var vector = (LargeVarBinaryVector)field.createVector(bufferAllocator)) {
                    vector.allocateNew();
                    var future = CompletableFuture.runAsync(() -> {
                        for (int i = 0; i < 5; i++) {
                            System.out.println("First thread: " + i);
                            // mimics what ArrowBufIO#ArrowBufDataOutput#ensureCapacity does
                            //                            synchronized (vector) {
                            vector.reallocDataBuffer();
                            //                            }
                        }
                        System.out.println("First thread finished.");
                    });
                    for (int i = 0; i < 5; i++) {
                        System.out.println("Second thread: " + i);
                        // mimics the behavior of AbstractArrowWriteData#expand
                        //                    synchronized (vector) {
                        vector.reAlloc();
                        //                    }
                    }
                    System.out.println("Second thread finished.");
                    future.get();
                }
            }
        } catch (IllegalStateException e) {
            // so we can expect one of two exception types
            throw new ExecutionException(e);
        } catch (IndexOutOfBoundsException e) {
            // so we can expect one of two exception types
            throw new ExecutionException(e);
        }
    }

    /**
     * This test runs expand and serialize concurrently, as we do it in org.knime.core.columnar.cache.CachedWriteData.
     * In this test we show that synchronizing on the write and expand operations prevents concurrent modification of
     * the Arrow Vector. If we would not serialize, we would run into exceptions as in the tests above, or even crashes
     * in the worst case.
     *
     * @throws ExecutionException
     */
    @Test
    public void testSynchronizedConcurrentReallocInExpandAndSerialize()
        throws IOException, InterruptedException, ExecutionException {
        final var schema = ColumnarSchema.of(VARBINARY);

        final byte[] randomData = new byte[30000];
        new Random().nextBytes(randomData);
        final String randomString = new String(randomData);
        System.out.println(randomString.length());

        final int numValues = 64;
        final var storeFactory = new ArrowColumnStoreFactory();
        final Object lock = new Object();

        // Try 50 times to run into the race condition that crashes the JVM
        for (int iteration = 0; iteration < 1; iteration++) {

            final FileHandle path = ArrowTestUtils.createTmpKNIMEArrowFileSupplier();
            long cachedDataSize = 0;

            try (final ArrowBatchStore store = storeFactory.createStore(schema, path);
                    final BatchWriter writer = store.getWriter()) {
                final var writeBatch = writer.create(numValues);
                final var data = (VarBinaryWriteData)writeBatch.get(0);

                final var writingStartedLatch = new CountDownLatch(1);

                var future = CompletableFuture.runAsync(() -> {
                    // Fill data with large values in separate thread.
                    // Writing large data leads to frequent realloc calls in
                    // ArrowBufIO.ArrowBufDataOutput#ensureCapacity.
                    for (int i = 0; i < numValues; i++) {
                        synchronized (lock) {
                            System.out.println("Write value " + i);
                            data.setObject(i, randomString, (out, d) -> {
                                out.writeUTF(d);
                            });
                        }

                        if (i == 2) {
                            writingStartedLatch.countDown();
                        }
                    }
                });

                // Wait until writing has commenced, then interrupt this thread and call expand,
                // which seems to be the scenario in which Arrow causes a crash in an unsafe memory copy
                // operation. Was extremely reproducible on Windows, happened only very rarely on Linux.
                // See AP-16402.
                writingStartedLatch.await();
                //                Thread.currentThread().interrupt();
                synchronized (lock) {
                    System.out.println("Expand");
                    data.expand(numValues * 3);
                }
                //                assertTrue(Thread.interrupted()); // clears the interrupt flag!

                future.get();

                cachedDataSize = data.sizeOf(); // flushes
                final var readBatch = writeBatch.close(numValues);
                readBatch.release();
            }

            assertTrue(cachedDataSize > 0);
        }
    }
}
