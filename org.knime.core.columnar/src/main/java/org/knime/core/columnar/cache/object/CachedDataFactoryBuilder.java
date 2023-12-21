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
 *   Dec 7, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cache.object;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.cache.DataIndex;
import org.knime.core.columnar.cache.object.CachedData.CachedDataFactory;
import org.knime.core.columnar.cache.object.CachedData.CachedWriteData;
import org.knime.core.columnar.cache.object.CachedStringData.CachedStringDataFactory;
import org.knime.core.columnar.cache.object.CachedStructData.CachedStructDataFactory;
import org.knime.core.columnar.cache.object.CachedVarBinaryData.CachedVarBinaryDataFactory;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.ListDataTraits;
import org.knime.core.table.schema.traits.StructDataTraits;

/**
 * Builds the CachedDataFactories which in turn manage the caching (or not caching) of individual data instances both on
 * a column level and nested.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class CachedDataFactoryBuilder {

    private final ExecutorService m_persistExecutor;

    private final Set<CachedWriteData> m_unclosedData;

    private final CacheManager m_cacheManager;

    private final CountUpDownLatch m_serializationLatch;

    private final ExecutorService m_serializationExecutor;

    interface ColumnDataUniqueIdFactory {
        ColumnDataUniqueId create(final DataIndex dataIndex, int batchIndex);
    }

    public static CachedDataFactoryBuilder createForWriting(final ExecutorService persistExecutor,
        final Set<CachedWriteData> unclosedData, final CacheManager cacheManager,
        final CountUpDownLatch serializationLatch, final ExecutorService serializationExecutor) {
        return new CachedDataFactoryBuilder(persistExecutor, unclosedData, cacheManager, serializationLatch,
            serializationExecutor);
    }

    public static CachedDataFactoryBuilder createForReading(final CacheManager cacheManager) {
        return new CachedDataFactoryBuilder(null, null, cacheManager, null, null);
    }

    private CachedDataFactoryBuilder(final ExecutorService persistExecutor, final Set<CachedWriteData> unclosedData,
        final CacheManager cacheManager, final CountUpDownLatch serializationLatch,
        final ExecutorService serializationExecutor) {
        m_persistExecutor = persistExecutor;
        m_unclosedData = unclosedData;
        m_cacheManager = cacheManager;
        m_serializationLatch = serializationLatch;
        m_serializationExecutor = serializationExecutor;
    }

    public CachedDataFactory[] build(final ColumnarSchema schema) {
        return createCachedDataFactories(schema);
    }

    private CachedDataFactory[] createCachedDataFactories(final ColumnarSchema schema) {
        final var factories = new CachedDataFactory[schema.numColumns()];
        Arrays.setAll(factories,
            i -> createCachedDataFactory(schema.getSpec(i), schema.getTraits(i), DataIndex.createColumnIndex(i)));
        return factories;
    }

    private CachedDataFactory createCachedDataFactory(final DataSpec spec, final DataTraits traits,
        final DataIndex index) {
        if (spec instanceof StringDataSpec) {
            return new CachedStringDataFactory(m_persistExecutor, m_unclosedData, m_cacheManager,
                m_serializationExecutor, m_serializationLatch);
        } else if (spec instanceof VarBinaryDataSpec) {
            return new CachedVarBinaryDataFactory(m_persistExecutor, m_unclosedData, m_cacheManager,
                m_serializationExecutor, m_serializationLatch);
        } else if (spec instanceof StructDataSpec) {
            var structSpec = (StructDataSpec)spec;
            var structTraits = (StructDataTraits)traits;
            return createCachedStructDataFactory(index, structSpec, structTraits);
        } else if (spec instanceof ListDataSpec) {
            var listSpec = (ListDataSpec)spec;
            var listTraits = (ListDataTraits)traits;
            return createCachedListDataFactory(index, listSpec, listTraits);
        } else {
            return UncachedDataFactory.INSTANCE;
        }
    }

    @SuppressWarnings({"static-method", "unused"}) // TODO implement AP-18333 (see below)
    private CachedDataFactory createCachedListDataFactory(final DataIndex index, final ListDataSpec listSpec,
        final ListDataTraits listTraits) {
        return UncachedDataFactory.INSTANCE;
        // TODO AP-18333: Properly implement caching lists of objects
//        var elementFactory = createCachedDataFactory(listSpec.getInner(), listTraits.getInner(), index.getChild(0));
//        if (elementFactory == UncachedDataFactory.INSTANCE) {
//            return UncachedDataFactory.INSTANCE;
//        } else {
//            return new CachedListDataFactory(elementFactory, m_persistExecutor, m_unclosedData);
//        }
    }

    private CachedDataFactory createCachedStructDataFactory(final DataIndex index, final StructDataSpec structSpec,
        final StructDataTraits structTraits) {
        var innerFactories = new CachedDataFactory[structSpec.size()];
        Arrays.setAll(innerFactories,
            i -> createCachedDataFactory(structSpec.getDataSpec(i), structTraits.getDataTraits(i), index.getChild(i)));
        if (Stream.of(innerFactories).anyMatch(c -> c == UncachedDataFactory.INSTANCE)) {
            // we can either cache all children or none
            return UncachedDataFactory.INSTANCE;
        } else {
            return new CachedStructDataFactory(innerFactories, m_persistExecutor, m_unclosedData);
        }
    }

    private enum UncachedDataFactory implements CachedDataFactory {
            INSTANCE;

        @Override
        public NullableWriteData createWriteData(final NullableWriteData data, final ColumnDataUniqueId id) {
            return data;
        }

        @Override
        public NullableReadData createReadData(final NullableReadData data, final ColumnDataUniqueId id) {
            return data;
        }

        @Override
        public CompletableFuture<NullableReadData> getCachedDataFuture(final NullableReadData data) {
            return CompletableFuture.completedFuture(data);
        }

    }

}
