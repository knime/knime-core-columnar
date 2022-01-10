/*
 * ------------------------------------------------------------------------
 *
 *  Copyright (c) 2019 by KNIME AG, Switzerland and others.
 *  You may use this software for internal usage only.
 *  You may make copies of the software for internal installation and backup
 *  purposes only.
 *  All other rights are reserved by the respective copyright holders.
 *  You must not under no circumstances distribute any part of this software to
 *  any third party for any purpose.
 * ---------------------------------------------------------------------
 *
 * History
 *   20 Oct 2021 (Steffen Fissler, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import org.junit.Test;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cache.data.SharedReadDataCache;
import org.knime.core.columnar.cache.object.shared.WeakReferencedObjectCache;
import org.knime.core.columnar.cache.writable.SharedBatchWritableCache;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.testing.DefaultTestBatchStore;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.domain.DefaultDomainWritableConfig;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.DefaultColumnarBatchStore.ColumnarBatchStoreBuilder;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.DefaultDataTraits;

/**
 *
 * @author Steffen Fissler, KNIME GmbH, Konstanz, Germany
 */
public class ObjectCacheSerializationTaskStressTest {
    /**
     * In a previous iteration we had used Java's Phaser to synchronize the serialization
     * tasks in ObjectCache. The Phaser has a limit of 65535 entries it can keep track of,
     * which we did exceed with the following test. The new implementation does not have this
     * problem but we keep this stress test.
     */
    @SuppressWarnings("javadoc")
    @Test(timeout = 10_000)
    public void runTest() throws IOException, InterruptedException { // NOSONAR
        final int numChunks = 1; // number of batches
        final int chunkSize = 131072; // size of chunks: for 1024 it should not throw a phaser exception, for 1024 * 128 it should throw a phaser exception
        final int numDataColumnsOfChunks = 33;

        final ColumnarSchema schema =
            createWideSchema(DataSpec.stringSpec(), DefaultDataTraits.EMPTY, numDataColumnsOfChunks);
        try (BatchStore store = DefaultTestBatchStore.create(schema)) {
            int smallTableCachSizeInBytes = 32 << 20;
            int columnDataCachSizeInBytes = 32 << 20;
            int smallTableCacheSizeThresholdInBytes = smallTableCachSizeInBytes / 10;
            final int numThreadsPerExecutor = 1;
            final var persistExecutor = Executors.newFixedThreadPool(numThreadsPerExecutor);
            final var serializeExecutor = Executors.newFixedThreadPool(numThreadsPerExecutor);
            final var domainCalcExecutor = Executors.newFixedThreadPool(numThreadsPerExecutor);
            final var columnDataCache = new SharedReadDataCache(columnDataCachSizeInBytes, 2);
            final var smallTableCache =
                new SharedBatchWritableCache(smallTableCacheSizeThresholdInBytes, smallTableCachSizeInBytes, 2);
            final var builder = new ColumnarBatchStoreBuilder(store);
            builder.useColumnDataCache(columnDataCache, persistExecutor).useSmallTableCache(smallTableCache);
            builder.useHeapCache(new WeakReferencedObjectCache(), persistExecutor, serializeExecutor);
            builder.useDomainCalculation(
                new DefaultDomainWritableConfig(createColumnarValueSchema(numDataColumnsOfChunks), 100, true),
                domainCalcExecutor);
            try (BatchStore cachedStore = builder.build(); BatchWriter writer = cachedStore.getWriter()) {
                storeData(numChunks, chunkSize, numDataColumnsOfChunks, writer);
            }
        }
    }

    private static void storeData(final int numChunks, final int chunkSize, final int numDataColumnsOfChunks,
        final BatchWriter writer) throws IOException, InterruptedException {
        for (int c = 0; c < numChunks; c++) {
            final WriteBatch batch = writer.create(chunkSize);
            for (int i = 1; i <= numDataColumnsOfChunks; i++) { //
                final NullableWriteData data = batch.get(i);
                for (int j = 0; j < chunkSize; j++) {
                    ((StringWriteData)data).setString(j, "********");
                }
            }
            var readBatch = batch.close(chunkSize);
            writer.write(readBatch);
            readBatch.release();
        }
    }

    private static ColumnarSchema createWideSchema(final DataSpec type, final DataTraits trait, final int widthOfData) {
        // We add one column to the table for the rowKeys
        final int widthOfTable = widthOfData + 1;
        final DataSpec[] types = new DataSpec[widthOfTable];
        final DataTraits[] traits = new DataTraits[widthOfTable];
        types[0] = DataSpec.stringSpec(); // rowKey
        traits[0] = trait; // rowKey
        for (int i = 1; i <= widthOfData; i++) {
            types[i] = type;
            traits[i] = trait;
        }
        return new DefaultColumnarSchema(types, traits);
    }

    private static ColumnarValueSchema createColumnarValueSchema(final int numDataColumnsOfChunks) {
        final DataTableSpec dataTableSpec = createSpec(numDataColumnsOfChunks);
        final ValueSchema valueSchema =
            ValueSchemaUtils.create(dataTableSpec, RowKeyType.CUSTOM, NotInWorkflowWriteFileStoreHandler.create());
        final ColumnarValueSchema schema = ColumnarValueSchemaUtils.create(valueSchema);
        return schema;
    }

    private static DataTableSpec createSpec(final int numColumns) {
        return new DataTableSpec(IntStream.range(0, numColumns)
            .mapToObj(i -> new DataColumnSpecCreator(Integer.toString(i), StringCell.TYPE).createSpec())
            .toArray(DataColumnSpec[]::new));
    }

}
