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
 *   Sep 23, 2021 (Carsten Haubold, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import static org.junit.Assert.assertEquals;
import static org.knime.core.data.columnar.TestDataBatchStoreUtils.createSpec;

import java.io.IOException;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.knime.core.columnar.cache.data.ReadDataCache;
import org.knime.core.columnar.cache.data.SharedReadDataCache;
import org.knime.core.columnar.cache.object.ObjectCache;
import org.knime.core.columnar.cache.object.WeakReferencedObjectCache;
import org.knime.core.columnar.cache.writable.BatchWritableCache;
import org.knime.core.columnar.cache.writable.SharedBatchWritableCache;
import org.knime.core.columnar.data.dictencoding.DictEncodedBatchWritableReadable;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.columnar.testing.TestBatchStore;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.domain.DefaultDomainWritableConfig;
import org.knime.core.data.columnar.domain.DomainWritable;
import org.knime.core.data.columnar.domain.DuplicateCheckWritable;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.ColumnarBatchStore.ColumnarBatchStoreBuilder;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchemaUtils;

/**
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnarBatchStoreBuilderTest extends ColumnarTest {

    private static ColumnarValueSchema generateDefaultSchema() {
        final DataTableSpec spec = createSpec();
        final ValueSchema valueSchema =
            ValueSchemaUtils.create(spec, RowKeyType.CUSTOM, NotInWorkflowWriteFileStoreHandler.create());
        return ColumnarValueSchemaUtils.create(valueSchema);
    }

    static final ColumnarValueSchema SCHEMA = generateDefaultSchema();

    @Test
    public void testDefaultWrappedBatchStore() throws IOException {
        try (final TestBatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final var builder = new ColumnarBatchStoreBuilder(delegate);
            try (final var wrapped = builder.build()) {
                assertEquals(delegate, wrapped.getWriteDelegate());
            }
        }
    }

    @Test
    public void testDomainCalculation() throws IOException {
        final var exec = Executors.newSingleThreadExecutor();
        try (final TestBatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final var builder = new ColumnarBatchStoreBuilder(delegate)
                .useDomainCalculation(new DefaultDomainWritableConfig(SCHEMA, 5, false), exec);
            try (final var wrapped = builder.build()) {
                assertEquals(DomainWritable.class, wrapped.getWriteDelegate().getClass());
            }
        }
    }

    @Test
    public void testDictEncoding() throws IOException {
        try (final TestBatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final var builder = new ColumnarBatchStoreBuilder(delegate).enableDictEncoding(true);
            try (final var wrapped = builder.build()) {
                assertEquals(DictEncodedBatchWritableReadable.class, wrapped.getWriteDelegate().getClass());
            }
        }
    }

    @Test
    public void testDuplicateChecking() throws IOException {
        final var exec = Executors.newSingleThreadExecutor();
        try (final TestBatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final var builder = new ColumnarBatchStoreBuilder(delegate).useDuplicateChecking(exec);
            try (final var wrapped = builder.build()) {
                assertEquals(DuplicateCheckWritable.class, wrapped.getWriteDelegate().getClass());
            }
        }
    }

    @Test
    public void testHeapCache() throws IOException {
        final var exec = Executors.newSingleThreadExecutor();
        try (final TestBatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final var cache = new WeakReferencedObjectCache();
            final var builder = new ColumnarBatchStoreBuilder(delegate).useHeapCache(cache, exec, exec);
            try (final var wrappedStore = builder.build()) {
                assertEquals(ObjectCache.class, wrappedStore.getWriteDelegate().getClass());
            }
        }
    }

    @Test
    public void testColumnDataCache() throws IOException {
        final var exec = Executors.newSingleThreadExecutor();
        try (final TestBatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final var cache = new SharedReadDataCache(64, 2);
            final var builder = new ColumnarBatchStoreBuilder(delegate).useColumnDataCache(cache, exec);
            try (final var wrappedStore = builder.build()) {
                assertEquals(ReadDataCache.class, wrappedStore.getWriteDelegate().getClass());
            }
        }
    }

    @Test
    public void testSmallTableCache() throws IOException {
        try (final TestBatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final var cache = new SharedBatchWritableCache(200, 1000, 2);
            final var builder = new ColumnarBatchStoreBuilder(delegate).useSmallTableCache(cache);
            try (final var wrappedStore = builder.build()) {
                assertEquals(BatchWritableCache.class, wrappedStore.getWriteDelegate().getClass());
            }
        }
    }

    @Test
    public void testSmallTableCacheAfterColumnDataCache() throws IOException {
        final var exec = Executors.newSingleThreadExecutor();
        try (final TestBatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final var smallTableCache = new SharedBatchWritableCache(200, 1000, 2);
            final var columnDataCache = new SharedReadDataCache(64, 2);
            final var builder = new ColumnarBatchStoreBuilder(delegate).useSmallTableCache(smallTableCache)
                .useColumnDataCache(columnDataCache, exec);
            try (final var wrappedStore = builder.build()) {
                assertEquals(BatchWritableCache.class, wrappedStore.getWriteDelegate().getClass());
            }
        }
    }

    @Test
    public void testHeapCacheAfterSmallTableCache() throws IOException {
        final var exec = Executors.newSingleThreadExecutor();
        try (final TestBatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final var heapCache = new WeakReferencedObjectCache();
            final var smallTableCache = new SharedBatchWritableCache(200, 1000, 2);
            final var builder = new ColumnarBatchStoreBuilder(delegate).useHeapCache(heapCache, exec, exec)
                    .useSmallTableCache(smallTableCache);
            try (final var wrappedStore = builder.build()) {
                assertEquals(ObjectCache.class, wrappedStore.getWriteDelegate().getClass());
            }
        }
    }

    @Test
    public void testDictEncodingAfterHeapCache() throws IOException {
        final var exec = Executors.newSingleThreadExecutor();
        try (final TestBatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final var heapCache = new WeakReferencedObjectCache();
            final var builder =
                new ColumnarBatchStoreBuilder(delegate).useHeapCache(heapCache, exec, exec).enableDictEncoding(true);
            try (final var wrappedStore = builder.build()) {
                assertEquals(DictEncodedBatchWritableReadable.class, wrappedStore.getWriteDelegate().getClass());
            }
        }
    }

    @Test
    public void testDuplicateCheckAfterDictEncoding() throws IOException {
        final var exec = Executors.newSingleThreadExecutor();
        try (final TestBatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final var builder =
                new ColumnarBatchStoreBuilder(delegate).useDuplicateChecking(exec).enableDictEncoding(true);
            try (final var wrappedStore = builder.build()) {
                assertEquals(DuplicateCheckWritable.class, wrappedStore.getWriteDelegate().getClass());
            }
        }
    }

    @Test
    public void testDomainCalculationAfterDuplicateCheck() throws IOException {
        final var exec = Executors.newSingleThreadExecutor();
        try (final TestBatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final var builder =
                new ColumnarBatchStoreBuilder(delegate).useDuplicateChecking(exec)
                .useDomainCalculation(new DefaultDomainWritableConfig(SCHEMA, 5, false), exec);
            try (final var wrappedStore = builder.build()) {
                assertEquals(DomainWritable.class, wrappedStore.getWriteDelegate().getClass());
            }
        }
    }
}
