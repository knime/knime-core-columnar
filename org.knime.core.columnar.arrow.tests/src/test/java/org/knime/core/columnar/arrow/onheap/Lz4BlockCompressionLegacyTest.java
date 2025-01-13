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
 *   Oct 26, 2021 (benjamin): created
 */
package org.knime.core.columnar.arrow.onheap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.knime.core.columnar.arrow.compress.ArrowCompressionUtil.ARROW_LZ4_BLOCK_COMPRESSION;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.columnar.arrow.ArrowTestUtils;
import org.knime.core.columnar.arrow.mmap.MappedMessageSerializerTestUtil;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.filter.DefaultColumnSelection;
import org.knime.core.columnar.store.FileHandle;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpecs;

/**
 * Test reading of Arrow files compressed with LZ4 block compression.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class Lz4BlockCompressionLegacyTest {

    private static final FileHandle FILE =
        ArrowTestUtils.createFileSupplier(Path.of("test_data", "LZ4_block_compression", "data.arrow"));

    private static final ColumnarSchema SCHEMA = ColumnarSchema.of(DataSpecs.INT, DataSpecs.STRING);

    private static final int NUM_ROWS = 20;

    private static final int NUM_BATCHES = 3;

    private static final String[] STRINGS = {"foo", "bar", "car", "aaa", "bbb"};

    private RootAllocator m_alloc;

    /**
     * Create the root allocator for each test
     *
     * @throws IOException
     */
    @Before
    public void before() throws IOException {
        m_alloc = new RootAllocator();
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
    }

    /**
     * Test reading LZ4 Block compressed data.
     *
     * @throws IOException
     */
    @Test
    public void testReadLz4BlockData() throws IOException {
        final OnHeapArrowColumnDataFactory[] factories = OnHeapArrowSchemaMapper.map(SCHEMA);
        try (final OnHeapArrowBatchReader reader =
            new OnHeapArrowBatchReader(FILE.asFile(), m_alloc, factories, new DefaultColumnSelection(2))) {
            for (int b = 0; b < NUM_BATCHES; b++) {
                checkBatch(reader, b);
            }
        }
    }

    /**
     * Create Arrow files with the legacy LZ4 Block compression. The written files are committed to the repository and
     * used in the tests above.
     *
     * DO NOT OVERWRITE THE GENERATED FILES BY COMMITING CHANGES TO THEM!
     *
     * @param args command line args are ignored
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        final OnHeapArrowColumnDataFactory[] factories = OnHeapArrowSchemaMapper.map(SCHEMA);
        try (final RootAllocator alloc = new RootAllocator(); //
                @SuppressWarnings("deprecation")
                final OnHeapArrowBatchWriter writer =
                    new OnHeapArrowBatchWriter(FILE, factories, ARROW_LZ4_BLOCK_COMPRESSION, alloc)) {
            for (int b = 0; b < NUM_BATCHES; b++) {
                writeBatch(writer, b);
            }
        }

    }

    // =================================================================
    // HELPER FUNCTIONS
    // =================================================================

    private static void writeBatch(final OnHeapArrowBatchWriter writer, final int seed) throws IOException {
        final WriteBatch writeBatch = writer.create(NUM_ROWS);
        fillIntColumn((IntWriteData)writeBatch.get(0), seed);
        fillStringColumn((StringWriteData)writeBatch.get(1), seed);
        final ReadBatch readBatch = writeBatch.close(NUM_ROWS);
        writer.write(readBatch);
        readBatch.release();
    }

    private static void fillIntColumn(final IntWriteData writeData, final int seed) {
        for (int i = 0; i < NUM_ROWS; i++) {
            if (i % 7 == 0) {
                writeData.setMissing(i);
            } else {
                writeData.setInt(i, i % (10 * (seed + 1)));
            }
        }
    }

    private static void fillStringColumn(final StringWriteData writeData, final int seed) {
        for (int i = 0; i < NUM_ROWS; i++) {
            if (i % 13 == 0) {
                writeData.setMissing(i);
            } else {
                writeData.setString(i, STRINGS[(i + seed) % STRINGS.length]);
            }
        }
    }

    private static void checkBatch(final OnHeapArrowBatchReader reader, final int index) throws IOException {
        final ReadBatch batch = reader.readRetained(index);
        checkIntColumn((IntReadData)batch.get(0), index);
        checkStringColumn((StringReadData)batch.get(1), index);
        batch.release();
    }

    private static void checkIntColumn(final IntReadData readData, final int seed) {
        for (int i = 0; i < NUM_ROWS; i++) {
            if (i % 7 == 0) {
                assertTrue(readData.isMissing(i));
            } else {
                assertFalse(readData.isMissing(i));
                assertEquals(i % (10 * (seed + 1)), readData.getInt(i));
            }
        }
    }

    private static void checkStringColumn(final StringReadData readData, final int seed) {
        for (int i = 0; i < NUM_ROWS; i++) {
            if (i % 13 == 0) {
                assertTrue(readData.isMissing(i));
            } else {
                assertFalse(readData.isMissing(i));
                assertEquals(STRINGS[(i + seed) % STRINGS.length], readData.getString(i));
            }
        }
    }
}
