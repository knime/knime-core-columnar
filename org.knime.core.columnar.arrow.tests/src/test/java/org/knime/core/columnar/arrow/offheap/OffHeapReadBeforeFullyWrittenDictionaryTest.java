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
 *   Jan 13, 2025 (benjamin): created
 */
package org.knime.core.columnar.arrow.offheap;

import static org.knime.core.columnar.arrow.compress.ArrowCompressionUtil.ARROW_NO_COMPRESSION;

import java.io.IOException;

import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.knime.core.columnar.arrow.ArrowTestUtils;
import org.knime.core.columnar.arrow.mmap.MappedMessageSerializerTestUtil;
import org.knime.core.columnar.arrow.offheap.OffHeapTestData.DictionaryEncodedData;
import org.knime.core.columnar.arrow.offheap.OffHeapTestData.DictionaryEncodedDataFactory;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.SequentialBatchReader;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.filter.DefaultColumnSelection;
import org.knime.core.columnar.store.FileHandle;

/**
 * Test reading from a partially written Arrow file with dictionaries.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class OffHeapReadBeforeFullyWrittenDictionaryTest {

    static FileHandle writePath;

    static FileHandle readPath;

    /**
     * Before running the tests, we create temporary paths where ArrowBatchStores can write to, or read from.
     *
     * @throws IOException
     */
    @BeforeAll
    public static void beforeClass() throws IOException {
        writePath = ArrowTestUtils.createTmpKNIMEArrowFileSupplier();
        readPath = ArrowTestUtils.createTmpKNIMEArrowFileSupplier();
    }

    /**
     * After each test, we clean up the temporary files so the next test can start from scratch
     *
     * @throws IOException
     */
    @AfterEach
    public void after() throws IOException {
        writePath.delete();
        readPath.delete();
        MappedMessageSerializerTestUtil.assertAllClosed();
    }

    /**
     * Test reading from an Arrow file before it is completely written (including dictionaries).
     *
     * @throws IOException
     */
    @Test
    void testReadBeforeFullyWrittenDictionary() throws IOException {
        // NOTE:
        // There is no data that makes use of dictionaries except the test data.
        // Therefore we cannot use the store.
        final int chunkSize = 64;
        final OffHeapArrowColumnDataFactory[] factories = new OffHeapArrowColumnDataFactory[]{new DictionaryEncodedDataFactory()};

        // Use the write store to write some data
        try (final RootAllocator allocator = new RootAllocator()) {

            @SuppressWarnings("resource")
            final OffHeapArrowBatchWriter writer = new OffHeapArrowBatchWriter(writePath, factories, ARROW_NO_COMPRESSION, allocator);
            ReadBatch batch;

            // Write batch 0
            batch = fillBatchDict(writer.create(chunkSize), chunkSize, 0);
            writer.write(batch);
            batch.release();

            // Write batch 1
            batch = fillBatchDict(writer.create(chunkSize), chunkSize, 1);
            writer.write(batch);
            batch.release();

            @SuppressWarnings("resource")
            final SequentialBatchReader reader = new OffHeapArrowPartialFileBatchReader(writePath.asFile(), allocator,
                factories, new DefaultColumnSelection(1), writer.getOffsetProvider());

            // Read back batch 0
            batch = reader.forward();
            assertBatchDataDict(batch, chunkSize, 0);
            batch.release();

            // Read back batch 1
            batch = reader.forward();
            assertBatchDataDict(batch, chunkSize, 1);
            batch.release();

            // Write batch 2
            batch = fillBatchDict(writer.create(chunkSize), chunkSize, 2);
            writer.write(batch);
            batch.release();

            // Write batch 3
            batch = fillBatchDict(writer.create(chunkSize), chunkSize, 3);
            writer.write(batch);
            batch.release();

            // Ignore batch 2
            reader.forward().release();

            // Read back batch 3
            batch = reader.forward();
            assertBatchDataDict(batch, chunkSize, 3);
            batch.release();

            // Write batch 4
            batch = fillBatchDict(writer.create(chunkSize), chunkSize, 4);
            writer.write(batch);
            batch.release();

            // Close the writer
            writer.close();

            // Read back batch 4
            batch = reader.forward();
            assertBatchDataDict(batch, chunkSize, 4);
            batch.release();

            // Close the reader
            reader.close();
        }
    }

    /** Fill the given batch (consisting of one dict encoded column) with some random data */
    private static ReadBatch fillBatchDict(final WriteBatch batch, final int chunkSize, final long seed) {
        final DictionaryEncodedData data = (DictionaryEncodedData)batch.get(0);
        OffHeapTestData.fillData(data, chunkSize, seed);
        return batch.close(chunkSize);
    }

    /**
     * Assert that the batch contains data written by {@link #fillBatchDict(WriteBatch, int, long)} with the same seed.
     */
    private static void assertBatchDataDict(final ReadBatch batch, final int chunkSize, final long seed) {
        final DictionaryEncodedData data = (DictionaryEncodedData)batch.get(0);
        OffHeapTestData.checkData(data, chunkSize, seed);
    }

}
