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
 *   Aug 10, 2021 (benjamin): created
 */
package org.knime.core.columnar.arrow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.knime.core.columnar.arrow.compress.ArrowCompressionUtil.ARROW_LZ4_BLOCK_COMPRESSION;
import static org.knime.core.columnar.arrow.compress.ArrowCompressionUtil.ARROW_NO_COMPRESSION;
import static org.knime.core.table.schema.DataSpecs.INT;
import static org.knime.core.table.schema.DataSpecs.LIST;
import static org.knime.core.table.schema.DataSpecs.STRUCT;
import static org.knime.core.table.schema.DataSpecs.ZONEDDATETIME;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.columnar.arrow.data.ArrowReadData;
import org.knime.core.columnar.arrow.data.ArrowWriteData;
import org.knime.core.columnar.arrow.data.ArrowZonedDateTimeData.ArrowZonedDateTimeDataFactory;
import org.knime.core.columnar.arrow.mmap.MappedMessageSerializerTestUtil;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;
import org.knime.core.columnar.data.ZonedDateTimeData.ZonedDateTimeReadData;
import org.knime.core.columnar.data.ZonedDateTimeData.ZonedDateTimeWriteData;
import org.knime.core.columnar.filter.DefaultColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Test reading of ArrowZonedDateTimeReadData if they were written with version 0 using dictionary encoding.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class ZonedDateTimeLegacyTest {

    private static File testFile(final String name) {
        return Path.of("test_data", "ZonedDateTime_legacy_files", name).toFile();
    }

    private static final File SINGLE_BATCH_FILE = testFile("single_batch.arrow");

    private static final File MULTIPLE_BATCHES_FILE = testFile("multiple_batches.arrow");

    private static final File MULTIPLE_COLUMNS_FILE = testFile("multiple_columns.arrow");

    private static final File COMPRESSED_FILE = testFile("compressed.arrow");

    private static final File STRUCT_LIST_FILE = testFile("struct_list.arrow");

    private static final ColumnarSchema SCHEMA_ONE_COLUMN = ColumnarSchema.of(ZONEDDATETIME);

    private static final ColumnarSchema SCHEMA_THREE_COLUMNS =
        ColumnarSchema.of(ZONEDDATETIME, ZONEDDATETIME, ZONEDDATETIME);

    private static final ColumnarSchema SCHEMA_STRUCT_LIST =
        ColumnarSchema.of(STRUCT.of(ZONEDDATETIME, INT, LIST.of(ZONEDDATETIME)), ZONEDDATETIME);

    private static final int NUM_ROWS = 20;

    private static final int NUM_BATCHES = 3;

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
     * Test reading a single batch.
     *
     * @throws IOException
     */
    @Test
    public void testSingleBatch() throws IOException {
        final ArrowColumnDataFactory[] factories =
            wrapVersionCheckingFactories(ArrowSchemaMapper.map(SCHEMA_ONE_COLUMN));
        try (final ArrowBatchReader reader =
            new ArrowBatchReader(SINGLE_BATCH_FILE, m_alloc, factories, new DefaultColumnSelection(1))) {
            readBatch(reader, 0, batch -> checkSimpleColumn(batch, 0, 0));
        }
    }

    /**
     * Test reading multiple batches.
     *
     * @throws IOException
     */
    @Test
    public void testMultipleBatches() throws IOException {
        final ArrowColumnDataFactory[] factories =
            wrapVersionCheckingFactories(ArrowSchemaMapper.map(SCHEMA_ONE_COLUMN));
        try (final ArrowBatchReader reader =
            new ArrowBatchReader(MULTIPLE_BATCHES_FILE, m_alloc, factories, new DefaultColumnSelection(1))) {
            for (int b = 0; b < NUM_BATCHES; b++) {
                final int seed = b;
                readBatch(reader, b, batch -> checkSimpleColumn(batch, 0, seed));
            }
        }
    }

    /**
     * Test reading multiple batches and multiple columns.
     *
     * @throws IOException
     */
    @Test
    public void testMultipleColumns() throws IOException {
        final ArrowColumnDataFactory[] factories =
            wrapVersionCheckingFactories(ArrowSchemaMapper.map(SCHEMA_THREE_COLUMNS));
        try (final ArrowBatchReader reader =
            new ArrowBatchReader(MULTIPLE_COLUMNS_FILE, m_alloc, factories, new DefaultColumnSelection(3))) {
            for (int b = 0; b < NUM_BATCHES; b++) {
                final int seed = b;
                readBatch(reader, b, batch -> {
                    checkSimpleColumn(batch, 0, seed);
                    checkSimpleColumn(batch, 1, seed + 1);
                    checkSimpleColumn(batch, 2, seed + 2);
                });
            }
        }
    }

    /**
     * Test reading from a compressed file.
     *
     * @throws IOException
     */
    @Test
    public void testCompressed() throws IOException {
        final ArrowColumnDataFactory[] factories =
            wrapVersionCheckingFactories(ArrowSchemaMapper.map(SCHEMA_THREE_COLUMNS));
        try (final ArrowBatchReader reader =
            new ArrowBatchReader(COMPRESSED_FILE, m_alloc, factories, new DefaultColumnSelection(3))) {
            for (int b = 0; b < NUM_BATCHES; b++) {
                final int seed = b;
                readBatch(reader, b, batch -> {
                    checkSimpleColumn(batch, 0, seed);
                    checkSimpleColumn(batch, 1, seed + 1);
                    checkSimpleColumn(batch, 2, seed + 2);
                });
            }
        }
    }

    /**
     * Test reading inside a struct and a list.
     *
     * @throws IOException
     */
    @Test
    public void testStructList() throws IOException {
        final ArrowColumnDataFactory[] factories =
            wrapVersionCheckingFactories(ArrowSchemaMapper.map(SCHEMA_STRUCT_LIST));
        try (final ArrowBatchReader reader =
            new ArrowBatchReader(STRUCT_LIST_FILE, m_alloc, factories, new DefaultColumnSelection(2))) {
            for (int b = 0; b < NUM_BATCHES; b++) {
                final int seed = b;
                readBatch(reader, b, batch -> {
                    checkStructListColumn(batch, 0, seed);
                    checkSimpleColumn(batch, 1, seed + 1);
                });
            }
        }
    }

    /**
     * Create Arrow files with the legacy ArrowZonedDateTime column. The written files are committed to the repository
     * and used in the tests above.
     *
     * DO NOT OVERWRITE THE GENERATED FILES BY COMMITING CHANGES TO THEM!
     *
     * @param args command line args are ignored
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        try (final RootAllocator alloc = new RootAllocator()) {

            // Single batch
            try (final ArrowBatchWriter writer = new ArrowBatchWriter(SINGLE_BATCH_FILE,
                ArrowSchemaMapper.map(SCHEMA_ONE_COLUMN), ARROW_NO_COMPRESSION, alloc)) {
                writeBatch(writer, writeBatch -> fillSimpleColumn(writeBatch, 0, 0));
            }

            // Multiple batches
            try (final ArrowBatchWriter writer = new ArrowBatchWriter(MULTIPLE_BATCHES_FILE,
                ArrowSchemaMapper.map(SCHEMA_ONE_COLUMN), ARROW_NO_COMPRESSION, alloc)) {
                for (int b = 0; b < NUM_BATCHES; b++) {
                    final int seed = b;
                    writeBatch(writer, writeBatch -> fillSimpleColumn(writeBatch, 0, seed));
                }
            }

            // Multiple columns
            try (final ArrowBatchWriter writer = new ArrowBatchWriter(MULTIPLE_COLUMNS_FILE,
                ArrowSchemaMapper.map(SCHEMA_THREE_COLUMNS), ARROW_NO_COMPRESSION, alloc)) {
                for (int b = 0; b < NUM_BATCHES; b++) {
                    final int seed = b;
                    writeBatch(writer, writeBatch -> {
                        fillSimpleColumn(writeBatch, 0, seed);
                        fillSimpleColumn(writeBatch, 1, seed + 1);
                        fillSimpleColumn(writeBatch, 2, seed + 2);
                    });
                }
            }

            // Compressed
            try (@SuppressWarnings("deprecation")
            final ArrowBatchWriter writer = new ArrowBatchWriter(COMPRESSED_FILE,
                ArrowSchemaMapper.map(SCHEMA_THREE_COLUMNS), ARROW_LZ4_BLOCK_COMPRESSION, alloc)) {
                for (int b = 0; b < NUM_BATCHES; b++) {
                    final int seed = b;
                    writeBatch(writer, writeBatch -> {
                        fillSimpleColumn(writeBatch, 0, seed);
                        fillSimpleColumn(writeBatch, 1, seed + 1);
                        fillSimpleColumn(writeBatch, 2, seed + 2);
                    });
                }
            }

            // Struct and List with ZonedDateTimeData
            try (final ArrowBatchWriter writer = new ArrowBatchWriter(STRUCT_LIST_FILE,
                ArrowSchemaMapper.map(SCHEMA_STRUCT_LIST), ARROW_NO_COMPRESSION, alloc)) {
                for (int b = 0; b < NUM_BATCHES; b++) {
                    final int seed = b;
                    writeBatch(writer, writeBatch -> {
                        fillStructListColumn(writeBatch, 0, seed);
                        fillSimpleColumn(writeBatch, 1, seed + 1);
                    });
                }
            }
        }
    }

    // =================================================================
    // HELPER FUNCTIONS
    // =================================================================

    /** Write a batch filled using the given consumer */
    private static void writeBatch(final ArrowBatchWriter writer, final Consumer<WriteBatch> fill) throws IOException {
        final WriteBatch writeBatch = writer.create(NUM_ROWS);
        fill.accept(writeBatch);
        final ReadBatch readBatch = writeBatch.close(NUM_ROWS);
        writer.write(readBatch);
        readBatch.release();
    }

    /** Read and check a batch using the given consumer */
    private static void readBatch(final ArrowBatchReader reader, final int index, final Consumer<ReadBatch> checker)
        throws IOException {
        final ReadBatch batch = reader.readRetained(index);
        checker.accept(batch);
        batch.release();
    }

    /** Fill one ZonedDateTimeData column of a batch with data */
    private static void fillSimpleColumn(final WriteBatch batch, final int column, final int seed) {
        final ZonedDateTimeWriteData writeData = (ZonedDateTimeWriteData)batch.get(column);
        for (int i = 0; i < NUM_ROWS; i++) {
            if (i % 7 == 0) {
                writeData.setMissing(i);
            } else {
                writeData.setZonedDateTime(i, valueFor(seed + i));
            }
        }
    }

    /** Check the data in one ZonedDateTimeData column of a batch */
    private static void checkSimpleColumn(final ReadBatch batch, final int column, final int seed) {
        final ZonedDateTimeReadData data = (ZonedDateTimeReadData)batch.get(column);
        for (int i = 0; i < NUM_ROWS; i++) {
            if (i % 7 == 0) {
                assertTrue(data.isMissing(i));
            } else {
                assertFalse(data.isMissing(i));
                assertEquals(valueFor(seed + i), data.getZonedDateTime(i));
            }
        }
    }

    /** Fill a complex Struct<ZonedDateTime, Int, List<ZonedDateTime>> column with data */
    private static void fillStructListColumn(final WriteBatch batch, final int column, final int seed) {
        final Random random = new Random(seed);

        final StructWriteData data = (StructWriteData)batch.get(column);
        final ZonedDateTimeWriteData zonedData = data.getWriteDataAt(0);
        final IntWriteData intData = data.getWriteDataAt(1);
        final ListWriteData listData = data.getWriteDataAt(2);

        for (int i = 0; i < NUM_ROWS; i++) {
            zonedData.setZonedDateTime(i, valueFor(random.nextInt()));

            intData.setInt(i, random.nextInt());

            final int listSize = random.nextInt(10);
            final ZonedDateTimeWriteData listZonedData = listData.createWriteData(i, listSize);
            for (int j = 0; j < listSize; j++) {
                listZonedData.setZonedDateTime(j, valueFor(random.nextInt()));
            }
        }
    }

    /** Check the data in a complex Struct<ZonedDateTime, Int, List<ZonedDateTime>> column */
    private static void checkStructListColumn(final ReadBatch batch, final int column, final int seed) {
        final Random random = new Random(seed);

        final StructReadData data = (StructReadData)batch.get(column);
        final ZonedDateTimeReadData zonedData = data.getReadDataAt(0);
        final IntReadData intData = data.getReadDataAt(1);
        final ListReadData listData = data.getReadDataAt(2);

        for (int i = 0; i < NUM_ROWS; i++) {
            assertFalse(data.isMissing(i));

            assertFalse(zonedData.isMissing(i));
            assertEquals(valueFor(random.nextInt()), zonedData.getZonedDateTime(i));

            assertFalse(intData.isMissing(i));
            assertEquals(random.nextInt(), intData.getInt(i));

            final int listSize = random.nextInt(10);
            assertFalse(listData.isMissing(i));
            final ZonedDateTimeReadData listZonedData = listData.createReadData(i);
            assertEquals(listSize, listZonedData.length());
            for (int j = 0; j < listSize; j++) {
                assertFalse(listZonedData.isMissing(j));
                assertEquals(valueFor(random.nextInt()), listZonedData.getZonedDateTime(j));
            }
        }
    }

    /** Generate a ZonedDateTime value for a given seed */
    private static ZonedDateTime valueFor(final int seed) {
        final int minZoneOffset = ZoneOffset.MIN.getTotalSeconds();
        final int maxZoneOffset = ZoneOffset.MAX.getTotalSeconds();
        final List<ZoneId> zoneIds = ZoneId.getAvailableZoneIds().stream().map(ZoneId::of).collect(Collectors.toList());
        final long minEpochDay = LocalDate.MIN.getLong(ChronoField.EPOCH_DAY);
        final long maxEpochDay = LocalDate.MAX.getLong(ChronoField.EPOCH_DAY);
        final long nanosPerDay = LocalTime.MAX.getLong(ChronoField.NANO_OF_DAY);

        // TODO other values for zoneId to make more use of the dict encoding?
        final Random random = new Random(seed);
        final long epochDay = minEpochDay + (long)(random.nextDouble() * (maxEpochDay - minEpochDay));
        final long nanoOfDay = (long)(random.nextDouble() * nanosPerDay);
        final LocalDateTime localDateTime =
            LocalDateTime.of(LocalDate.ofEpochDay(epochDay), LocalTime.ofNanoOfDay(nanoOfDay));
        final ZoneOffset zoneOffset =
            ZoneOffset.ofTotalSeconds(minZoneOffset + random.nextInt(maxZoneOffset - minZoneOffset));
        final ZoneId zoneId = zoneIds.get(random.nextInt(zoneIds.size()));
        return ZonedDateTime.ofInstant(localDateTime, zoneOffset, zoneId);
    }

    /** Wrap all ZonedDateTime factories in VersionCheckingFactoryWrapper to check the version of the read data is 0. */
    private static ArrowColumnDataFactory[] wrapVersionCheckingFactories(final ArrowColumnDataFactory[] factories) {
        final ArrowColumnDataFactoryVersion expectedVersion = ArrowColumnDataFactoryVersion.version(0);
        return Arrays.stream(factories).map(
            f -> f instanceof ArrowZonedDateTimeDataFactory ? new VersionCheckingFactoryWrapper(f, expectedVersion) : f)
            .toArray(ArrowColumnDataFactory[]::new);
    }

    /**
     * An {@link ArrowColumnDataFactory} that wraps another factory and checks that #createRead is called with the
     * expected version.
     */
    private static final class VersionCheckingFactoryWrapper implements ArrowColumnDataFactory {

        private final ArrowColumnDataFactory m_delegate;

        private final ArrowColumnDataFactoryVersion m_expectedVersion;

        public VersionCheckingFactoryWrapper(final ArrowColumnDataFactory delegate,
            final ArrowColumnDataFactoryVersion expectedVersion) {
            m_delegate = delegate;
            m_expectedVersion = expectedVersion;
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return m_delegate.getField(name, dictionaryIdSupplier);
        }

        @Override
        public ArrowWriteData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            return m_delegate.createWrite(vector, dictionaryIdSupplier, allocator, capacity);
        }

        @Override
        public ArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            assertEquals(m_expectedVersion, version);
            return m_delegate.createRead(vector, nullCount, provider, version);
        }

        @Override
        public FieldVector getVector(final NullableReadData data) {
            return m_delegate.getVector(data);
        }

        @Override
        public DictionaryProvider getDictionaries(final NullableReadData data) {
            return m_delegate.getDictionaries(data);
        }

        @Override
        public ArrowColumnDataFactoryVersion getVersion() {
            return m_delegate.getVersion();
        }
    }
}
