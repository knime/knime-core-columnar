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

import org.junit.Test;
import org.knime.core.columnar.cache.data.ReadDataReadCache;
import org.knime.core.columnar.cache.data.SharedReadDataCache;
import org.knime.core.columnar.cache.object.ObjectReadCache;
import org.knime.core.columnar.cache.object.WeakReferencedObjectCache;
import org.knime.core.columnar.data.dictencoding.DictEncodedBatchReadable;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.columnar.testing.TestBatchStore;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.ColumnarBatchReadStore.ColumnarBatchReadStoreBuilder;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchemaUtils;

/**
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnarBatchReadStoreBuilderTest extends ColumnarTest {

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
            final var builder = new ColumnarBatchReadStoreBuilder(delegate);
            try (final var wrappedStore = builder.build();
                    final var wrapped = wrappedStore.getDelegate()) {
                assertEquals(delegate, wrapped);
            }
        }
    }

    @Test
    public void testDictEncoding() throws IOException {
        try (final TestBatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final var builder = new ColumnarBatchReadStoreBuilder(delegate).enableDictEncoding(true);
            try (final var wrappedStore = builder.build();
                    final var wrapped = wrappedStore.getDelegate()) {
                assertEquals(DictEncodedBatchReadable.class, wrapped.getClass());
            }
        }
    }

    @Test
    public void testHeapCache() throws IOException {
        try (final TestBatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final var cache = new WeakReferencedObjectCache();
            final var builder = new ColumnarBatchReadStoreBuilder(delegate)
                    .useHeapCache(cache);
            try (final var wrappedStore = builder.build();
                    final var wrapped = wrappedStore.getDelegate()) {
                assertEquals(ObjectReadCache.class, wrapped.getClass());
            }
        }
    }

    @Test
    public void testColumnDataCache() throws IOException {
        try (final TestBatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final var cache = new SharedReadDataCache(64, 2);
            final var builder = new ColumnarBatchReadStoreBuilder(delegate)
                    .useColumnDataCache(cache);
            try (final var wrappedStore = builder.build();
                    final var wrapped = wrappedStore.getDelegate()) {
                assertEquals(ReadDataReadCache.class, wrapped.getClass());
            }
        }
    }

    @Test
    public void testHeapCacheAfterColumnDataCache() throws IOException {
        try (final TestBatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final var heapCache = new WeakReferencedObjectCache();
            final var columnDataCache = new SharedReadDataCache(64, 2);
            final var builder = new ColumnarBatchReadStoreBuilder(delegate)
                    .useHeapCache(heapCache)
                    .useColumnDataCache(columnDataCache);
            try (final var wrappedStore = builder.build();
                    final var wrapped = wrappedStore.getDelegate()) {
                assertEquals(ObjectReadCache.class, wrapped.getClass());
            }
        }
    }

    @Test
    public void testDictEncodingAfterHeapCache() throws IOException {
        try (final TestBatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final var heapCache = new WeakReferencedObjectCache();
            final var builder = new ColumnarBatchReadStoreBuilder(delegate)
                    .useHeapCache(heapCache)
                    .enableDictEncoding(true);
            try (final var wrappedStore = builder.build();
                    final var wrapped = wrappedStore.getDelegate()) {
                assertEquals(DictEncodedBatchReadable.class, wrapped.getClass());
            }
        }
    }
}
