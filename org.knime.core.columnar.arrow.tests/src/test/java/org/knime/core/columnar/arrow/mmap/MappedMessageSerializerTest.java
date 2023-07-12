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
 *   Mar 30, 2021 (benjamin): created
 */
package org.knime.core.columnar.arrow.mmap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.knime.core.columnar.arrow.ArrowTestUtils.createTmpKNIMEArrowPath;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link MappedMessageSerializer}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class MappedMessageSerializerTest {

    private static final int MAX_BUFFER_SIZE = 20;

    private RootAllocator m_alloc;

    private Path m_path;

    /**
     * Create the root allocator for each test
     *
     * @throws IOException
     */
    @Before
    public void before() throws IOException {
        m_alloc = new RootAllocator();
        m_path = createTmpKNIMEArrowPath();
    }

    /**
     * Close the root allocator for each test
     *
     * @throws IOException
     */
    @After
    public void after() throws IOException {
        m_alloc.close();
        MappedMessageSerializerTestUtil.assertAllClosed();
        Files.delete(m_path);
    }

    /**
     * Test loading record batches by memory-mapping them.
     *
     * @throws IOException
     */
    @Test
    @SuppressWarnings("resource") // Closed manually and resources are check in #after
    public void testMappingRecordBatch() throws IOException {
        final int numBuffers = 10;
        final long seed0 = 4;
        final long seed1 = 5;

        // Write a record batch to the file
        final long offset0;
        final long offset1;
        try (final RandomAccessFile randomAccessFile = new RandomAccessFile(m_path.toFile(), "rw");
                final FileChannel channel = randomAccessFile.getChannel();
                final WriteChannel out = new WriteChannel(channel)) {
            // batch0
            try (final ArrowRecordBatch batch = createRecordBatch(m_alloc, numBuffers, seed0)) {
                offset0 = MessageSerializer.serialize(out, batch).getOffset();
            }
            // batch1
            try (final ArrowRecordBatch batch = createRecordBatch(m_alloc, numBuffers, seed1)) {
                offset1 = MessageSerializer.serialize(out, batch).getOffset();
            }
        }

        // Read the record batch from the file
        final ArrowRecordBatch batch00 = readRecordBatch(m_path, offset0);
        checkBatch(batch00, numBuffers, seed0);
        final ArrowRecordBatch batch10 = readRecordBatch(m_path, offset1);
        checkBatch(batch10, numBuffers, seed1);

        // Read batch0 again
        final ArrowRecordBatch batch01 = readRecordBatch(m_path, offset0);
        checkBatch(batch01, numBuffers, seed0);

        // Check that the batches use the same data
        assertBatchesSame(batch00, batch01);

        // Close all batch0
        batch00.close();
        batch01.close();
        assertReleased(batch00);
        assertReleased(batch01);

        // Read batch1
        final ArrowRecordBatch batch11 = readRecordBatch(m_path, offset1);
        checkBatch(batch11, numBuffers, seed1);

        // Check that the batches use the same data
        assertBatchesSame(batch10, batch11);

        // Close all batch1
        batch10.close();
        batch11.close();
        assertReleased(batch10);
        assertReleased(batch11);
    }

    /**
     * Test loading dictionary batches by memory-mapping them.
     *
     * @throws IOException
     */
    @Test
    @SuppressWarnings("resource") // Closed manually and resources are check in #after
    public void testMappingDictionaryBatch() throws IOException {
        final int numBuffers = 10;
        final long seedRB = 3;
        final long seed0 = 4;
        final long seed1 = 5;

        // Write a record batch to the file
        final long offsetRB;
        final long offset0;
        final long offset1;
        try (final RandomAccessFile randomAccessFile = new RandomAccessFile(m_path.toFile(), "rw");
                final FileChannel channel = randomAccessFile.getChannel();
                final WriteChannel out = new WriteChannel(channel)) {
            // rb
            try (final ArrowRecordBatch batch = createRecordBatch(m_alloc, numBuffers, seedRB)) {
                offsetRB = MessageSerializer.serialize(out, batch).getOffset();
            }
            // dict: batch0
            try (final ArrowDictionaryBatch batch = createDictionaryBatch(m_alloc, numBuffers, seed0)) {
                offset0 = MessageSerializer.serialize(out, batch).getOffset();
            }
            // dict: batch 1
            try (final ArrowDictionaryBatch batch = createDictionaryBatch(m_alloc, numBuffers, seed1)) {
                offset1 = MessageSerializer.serialize(out, batch).getOffset();
            }
        }

        // Read batch0
        final ArrowDictionaryBatch batch00 = readDictionaryBatch(m_path, offset0);
        checkBatch(batch00, numBuffers, seed0);

        // Read a record batch (to test if we can mix record batches and dictionary batches)
        final ArrowRecordBatch recordBatch = readRecordBatch(m_path, offsetRB);
        checkBatch(recordBatch, numBuffers, seedRB);

        // Read batch1
        final ArrowDictionaryBatch batch10 = readDictionaryBatch(m_path, offset1);
        checkBatch(batch10, numBuffers, seed1);

        // Read batch0 again
        final ArrowDictionaryBatch batch01 = readDictionaryBatch(m_path, offset0);
        checkBatch(batch01, numBuffers, seed0);

        // Check that the batches use the same data
        assertBatchesSame(batch00, batch01);

        // Close all batch0
        batch00.close();
        batch01.close();
        assertReleased(batch00);
        assertReleased(batch01);

        // Read batch1
        final ArrowDictionaryBatch batch11 = readDictionaryBatch(m_path, offset1);
        checkBatch(batch11, numBuffers, seed1);

        // Check that the batches use the same data
        assertBatchesSame(batch10, batch11);

        // Close all batch1
        batch10.close();
        batch11.close();
        assertReleased(batch10);
        assertReleased(batch11);

        // Close recordBatch
        recordBatch.getBuffers().forEach(ArrowBuf::close);
        assertReleased(recordBatch);
    }

    /**
     * Test memory-mapping the special case of an empty batch.
     *
     * @throws IOException
     */
    @Test
    @SuppressWarnings("resource") // Closed manually and resources are check in #after
    public void testMappingEmptyBatch() throws IOException {
        // Write a record batch to the file
        final long offsetRB;
        final long offsetDB;
        try (final RandomAccessFile randomAccessFile = new RandomAccessFile(m_path.toFile(), "rw");
                final FileChannel channel = randomAccessFile.getChannel();
                final WriteChannel out = new WriteChannel(channel)) {
            // rb
            try (final ArrowRecordBatch batch = createRecordBatch(m_alloc, 0, 0)) {
                offsetRB = MessageSerializer.serialize(out, batch).getOffset();
            }
            // dict: batch0
            try (final ArrowDictionaryBatch batch = createDictionaryBatch(m_alloc, 0, 1)) {
                offsetDB = MessageSerializer.serialize(out, batch).getOffset();
            }
        }

        // recordBatch
        final ArrowRecordBatch recordBatch = readRecordBatch(m_path, offsetRB);
        assertEquals(0, recordBatch.getBuffers().size());
        checkBatch(recordBatch, 0, 0);

        // dictBatch
        final ArrowDictionaryBatch dictBatch = readDictionaryBatch(m_path, offsetDB);
        assertEquals(0, dictBatch.getDictionary().getBuffers().size());
        checkBatch(dictBatch, 0, 1);

        // No buffers -> Memory should already be released
        assertReleased(recordBatch);
        assertReleased(dictBatch);
        recordBatch.close();
        dictBatch.close();
        assertReleased(recordBatch);
        assertReleased(dictBatch);
    }

    private static ArrowRecordBatch createRecordBatch(final BufferAllocator allocator, final int numBuffers,
        final long seed) {
        final Random random = new Random(seed);
        final List<ArrowFieldNode> nodes = new ArrayList<>();
        final List<ArrowBuf> buffers = new ArrayList<>();

        for (int i = 0; i < numBuffers; i++) {
            final int size = random.nextInt(MAX_BUFFER_SIZE);

            // Create the buffer
            @SuppressWarnings("resource")
            final ArrowBuf buf = allocator.buffer(size);
            for (int j = 0; j < size; j++) {
                buf.writeByte(random.nextInt(256));
            }
            buffers.add(buf);

            // Create the node
            final int length = random.nextInt(MAX_BUFFER_SIZE);
            final int nullCount = random.nextInt(MAX_BUFFER_SIZE);
            final ArrowFieldNode node = new ArrowFieldNode(length, nullCount);
            nodes.add(node);
        }
        final ArrowRecordBatch batch = new ArrowRecordBatch(10, nodes, buffers);
        buffers.forEach(ArrowBuf::close);
        return batch;
    }

    @SuppressWarnings("resource") // Record batch closed with dictionary batch
    private static ArrowDictionaryBatch createDictionaryBatch(final BufferAllocator allocator, final int numBuffers,
        final long seed) {
        final Random random = new Random(seed);
        final long dictId = random.nextLong();
        final long rbSeed = random.nextLong();
        final boolean isDelta = random.nextBoolean();
        return new ArrowDictionaryBatch(dictId, createRecordBatch(allocator, numBuffers, rbSeed), isDelta);
    }

    private static ArrowRecordBatch readRecordBatch(final Path path, final long offset) throws IOException {
        final File file = path.toFile();
        try (final MappableReadChannel in = new MappableReadChannel(file, "r")) {
            return MappedMessageSerializer.deserializeRecordBatch(in, offset);
        }
    }

    private static ArrowDictionaryBatch readDictionaryBatch(final Path path, final long offset) throws IOException {
        final File file = path.toFile();
        try (final MappableReadChannel in = new MappableReadChannel(file, "r")) {
            return MappedMessageSerializer.deserializeDictionaryBatch(in, offset);
        }
    }

    @SuppressWarnings("resource") // Buffers do not need to be closed here
    private static void checkBatch(final ArrowRecordBatch batch, final int numBuffers, final long seed) {
        final Random random = new Random(seed);
        final List<ArrowBuf> buffers = batch.getBuffers();
        final List<ArrowFieldNode> nodes = batch.getNodes();

        for (int i = 0; i < numBuffers; i++) {
            final int size = random.nextInt(MAX_BUFFER_SIZE);

            // Check the buffer content
            final ArrowBuf buf = buffers.get(i);
            buf.readerIndex(0);
            assertTrue(buf.getReferenceManager().getRefCount() > 0);
            assertEquals(size, buf.writerIndex());
            for (int j = 0; j < size; j++) {
                assertEquals((byte)random.nextInt(256), buf.readByte());
            }

            // Check the node
            assertEquals(random.nextInt(MAX_BUFFER_SIZE), nodes.get(i).getLength());
            assertEquals(random.nextInt(MAX_BUFFER_SIZE), nodes.get(i).getNullCount());
        }
    }

    @SuppressWarnings("resource") // Dictionary record batch does not need to be closed
    private static void checkBatch(final ArrowDictionaryBatch batch, final int numBuffers, final long seed) {
        final Random random = new Random(seed);
        final long dictId = random.nextLong();
        final long rbSeed = random.nextLong();
        final boolean isDelta = random.nextBoolean();

        assertEquals(dictId, batch.getDictionaryId());
        assertEquals(isDelta, batch.isDelta());
        checkBatch(batch.getDictionary(), numBuffers, rbSeed);
    }

    @SuppressWarnings("resource")
    private static void assertBatchesSame(final ArrowRecordBatch expected, final ArrowRecordBatch actual) {
        final List<ArrowBuf> expectedBuffers = expected.getBuffers();
        final List<ArrowBuf> actualBuffers = actual.getBuffers();
        assertEquals(expectedBuffers.size(), actualBuffers.size());
        for (int i = 0; i < expectedBuffers.size(); i++) {
            assertSame(expectedBuffers.get(i), actualBuffers.get(i));
        }
    }

    @SuppressWarnings("resource")
    private static void assertBatchesSame(final ArrowDictionaryBatch expected, final ArrowDictionaryBatch actual) {
        assertBatchesSame(expected.getDictionary(), actual.getDictionary());
    }

    /** Check that all buffers have 0 reference count */
    private static void assertReleased(final ArrowRecordBatch batch) {
        try {
            // Access buffers for closed batches using reflection
            final Field buffersField = batch.getClass().getDeclaredField("buffers");
            buffersField.setAccessible(true);
            @SuppressWarnings("unchecked")
            final List<ArrowBuf> buffers = (List<ArrowBuf>)buffersField.get(batch);

            // Check that all buffers are closed
            for (final ArrowBuf b : buffers) {
                assertEquals(0, b.getReferenceManager().getRefCount());
            }

        } catch (final NoSuchFieldException | SecurityException | IllegalArgumentException
                | IllegalAccessException ex) {
            throw new AssertionError(ex);
        }
    }

    /** Check that all buffers have 0 reference count */
    @SuppressWarnings("resource")
    private static void assertReleased(final ArrowDictionaryBatch batch) {
        assertReleased(batch.getDictionary());
    }
}
