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
 *   15 Dec 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.cache.object;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.knime.core.columnar.TestBatchStoreUtils.createDefaultTestBatchBuffer;
import static org.knime.core.columnar.TestBatchStoreUtils.readAndCompareTable;
import static org.knime.core.columnar.TestBatchStoreUtils.readSelectionAndCompareTable;
import static org.knime.core.columnar.TestBatchStoreUtils.readTwiceAndCompareTable;
import static org.knime.core.columnar.TestBatchStoreUtils.releaseTable;
import static org.knime.core.columnar.TestBatchStoreUtils.writeDefaultTable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.testing.TestBatchBuffer;
import org.knime.core.columnar.testing.data.TestStringData;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ObjectCacheTest {

    private static final ExecutorService SERIALIZATION_EXECUTOR = Executors.newSingleThreadExecutor();

    @AfterClass
    public static void after() {
        SERIALIZATION_EXECUTOR.shutdown();
    }

    static SharedObjectCache generateCache() {

        return new SharedObjectCache() {
            private final Map<ColumnDataUniqueId, Object[]> m_cache = new ConcurrentHashMap<>();

            @Override
            public Map<ColumnDataUniqueId, Object[]> getCache() {
                return m_cache;
            }
        };
    }

    @SuppressWarnings("resource")
    private static ObjectCache generateDefaultHeapCachedStore() {
        final TestBatchBuffer delegate = createDefaultTestBatchBuffer();
        return new ObjectCache(delegate, generateCache(), SERIALIZATION_EXECUTOR);
    }

    private static ColumnarSchema createSingleStringColumnSchema() {
        return new DefaultColumnarSchema(DataSpec.stringSpec());
    }

    private static CountDownLatch blockSerialization() {
        final CountDownLatch blockLatch = new CountDownLatch(1);
        SERIALIZATION_EXECUTOR.submit(() -> {
            try {
                blockLatch.await();
            } catch (InterruptedException ex) {
                Assert.fail();
            }
        });
        return blockLatch;
    }

    private static void resumeAndWaitForSerialization(final CountDownLatch blockLatch) throws InterruptedException {
        blockLatch.countDown();
        waitForSerialization();
    }

    private static void waitForSerialization() throws InterruptedException {
        final CountDownLatch waitLatch = new CountDownLatch(1);
        SERIALIZATION_EXECUTOR.submit(() -> waitLatch.countDown());
        waitLatch.await();
    }

    @Test
    public void testWriteRead() throws IOException {
        try (final ObjectCache store = generateDefaultHeapCachedStore()) {
            final List<NullableReadData[]> table = writeDefaultTable(store);
            readAndCompareTable(store, table);
            releaseTable(table);
        }
    }

    @Test
    public void testWriteMultiRead() throws IOException {
        try (final ObjectCache store = generateDefaultHeapCachedStore()) {
            final List<NullableReadData[]> table = writeDefaultTable(store);
            readTwiceAndCompareTable(store, table.size());
            releaseTable(table);
        }
    }

    @Test
    public void testWriteReadSelection() throws IOException {
        try (final ObjectCache store = generateDefaultHeapCachedStore()) {
            final List<NullableReadData[]> table = writeDefaultTable(store);
            for (int i = 0; i < store.getSchema().numColumns(); i++) {
                readSelectionAndCompareTable(store, table, i);
            }
            releaseTable(table);
        }
    }

    @Test
    public void testWriterSingleton() throws IOException {
        try (final ObjectCache store = generateDefaultHeapCachedStore();
                final BatchWriter writer1 = store.getWriter();
                final BatchWriter writer2 = store.getWriter()) {
            assertEquals(writer1, writer2);
        }
    }

    @Test
    public void testFlush() throws IOException, InterruptedException {
        try (final TestBatchBuffer delegate = TestBatchBuffer.create(createSingleStringColumnSchema());
                final ObjectCache store = new ObjectCache(delegate, generateCache(), SERIALIZATION_EXECUTOR);
                final BatchWriter writer = store.getWriter()) {
            final WriteBatch batch = writer.create(2);
            final CachedStringWriteData data = (CachedStringWriteData)batch.get(0);
            final TestStringData delegateData = (TestStringData)data.m_delegate;

            store.flush();

            // test that data is serialized after flush
            data.setString(0, "0");
            assertNull(delegateData.getString(0));
            store.flush();
            assertEquals("0", delegateData.getString(0));

            // test that data is serialized asynchronously after closing the writer
            data.setString(1, "1");
            assertEquals("0", delegateData.getString(0));
            assertNull(delegateData.getString(1));
            writer.write(batch.close(1));
            writer.close();
            waitForSerialization();

            assertEquals("0", delegateData.getString(0));
            assertEquals("1", delegateData.getString(1));

            // test that further invocations of flush have no effect
            store.flush();
            assertEquals("0", delegateData.getString(0));
            assertEquals("1", delegateData.getString(1));

            batch.release();
        }
    }

    @Test
    public void testSerializeAsync() throws IOException, InterruptedException {
        try (final TestBatchBuffer delegate = TestBatchBuffer.create(createSingleStringColumnSchema());
                final ObjectCache store = new ObjectCache(delegate, generateCache(), SERIALIZATION_EXECUTOR);
                final BatchWriter writer = store.getWriter()) {
            final WriteBatch batch = writer.create(1);
            final CachedStringWriteData data = (CachedStringWriteData)batch.get(0);
            final TestStringData delegateData = (TestStringData)data.m_delegate;

            final CountDownLatch blockLatch = blockSerialization();
            try {
                data.setString(0, "0");
                batch.close(1);
                assertNull(delegateData.getString(0));
            } finally {
                resumeAndWaitForSerialization(blockLatch);
            }
            assertEquals("0", delegateData.getString(0));

            batch.release();
        }
    }

    @Test
    public void testCloseDoesNotBlock() throws IOException, InterruptedException {
        try (final TestBatchBuffer delegate = TestBatchBuffer.create(createSingleStringColumnSchema());
                final ObjectCache store = new ObjectCache(delegate, generateCache(), SERIALIZATION_EXECUTOR);
                final BatchWriter writer = store.getWriter()) {

            final WriteBatch writeBatch1 = writer.create(1);
            final StringWriteData writeData1 = (StringWriteData)writeBatch1.get(0);
            writeData1.setString(0, "0");
            final ReadBatch writeReadBatch1 = writeBatch1.close(1);
            writer.write(writeReadBatch1);
            writeReadBatch1.release();

            final CountDownLatch blockLatch = blockSerialization();

            try {
                final WriteBatch writeBatch2 = writer.create(1);
                final StringWriteData writeData2 = (StringWriteData)writeBatch2.get(0);
                writeData2.setString(0, "1");
                final ReadBatch writeReadBatch2 = writeBatch2.close(1);
                writer.write(writeReadBatch2);
                writeReadBatch2.release();

                try (final RandomAccessBatchReader reader = store.createRandomAccessReader()) {
                    final ReadBatch readBatch1 = reader.readRetained(0);
                    final StringReadData readData1 = (StringReadData)readBatch1.get(0);
                    assertEquals("0", readData1.getString(0));
                    readBatch1.release();
                }

            } finally {
                resumeAndWaitForSerialization(blockLatch);
            }
        }
    }

}
