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
 *   Jul 2, 2021 (Carsten Haubold, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.data.dictencoding;

import java.io.IOException;
import java.util.Arrays;

import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.DefaultWriteBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cache.DataIndex;
import org.knime.core.columnar.data.DecoratingData.DecoratingNullableReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.StructData.StructWriteData;
import org.knime.core.columnar.data.dictencoding.AbstractDictDecodedData.AbstractDictDecodedReadData;
import org.knime.core.columnar.data.dictencoding.DecoratedListData.DecoratedListReadData;
import org.knime.core.columnar.data.dictencoding.DecoratedStructData.DecoratedStructReadData;
import org.knime.core.columnar.data.dictencoding.DictDecodedStringData.DictDecodedStringWriteData;
import org.knime.core.columnar.data.dictencoding.DictDecodedVarBinaryData.DictDecodedVarBinaryWriteData;
import org.knime.core.columnar.data.dictencoding.DictElementCache.ColumnDictElementCache;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedStringWriteData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedVarBinaryWriteData;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait;
import org.knime.core.table.schema.traits.DataTraitUtils;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.ListDataTraits;
import org.knime.core.table.schema.traits.StructDataTraits;

/**
 * A {@link DictEncodedBatchWriter} wraps a delegate BatchWriter and converts user-facing data objects into their
 * dictionary encoded representation
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class DictEncodedBatchWriter implements BatchWriter {

    private final BatchWriter m_delegate;

    private final ColumnarSchema m_schema;

    private final DictElementCache m_cache;

    /**
     * Create a batch writer
     *
     * @param delegate The delegate BatchWriter
     * @param schema The schema
     * @param dictElementCache The table-wide {@link DictElementCache}
     */
    public DictEncodedBatchWriter(final BatchWriter delegate, final ColumnarSchema schema,
        final DictElementCache dictElementCache) {
        m_delegate = delegate;
        m_schema = schema;
        m_cache = dictElementCache;
    }

    @Override
    public void close() throws IOException {
        m_delegate.close();
    }

    @Override
    public WriteBatch create(final int capacity) {
        final WriteBatch batch = m_delegate.create(capacity);
        final var data = new NullableWriteData[m_schema.numColumns()];

        for (int i = 0; i < data.length; i++) {
            final NullableWriteData d = batch.get(i);

            if (DataTraitUtils.containsDataTrait(DictEncodingTrait.class, m_schema.getTraits(i))) {
                data[i] = wrapDictEncodedData(i, d);
            } else {
                data[i] = d;
            }

        }

        return new DefaultWriteBatch(data);
    }

    private NullableWriteData wrapDictEncodedData(final int i, final NullableWriteData d) {
        return wrapDictEncodedData(DataIndex.createColumnIndex(i), d, m_schema.getSpec(i), m_schema.getTraits(i));
    }

    private NullableWriteData wrapDictEncodedData(final DataIndex index, final NullableWriteData d, final DataSpec spec,
        final DataTraits traits) {
        if (spec instanceof ListDataSpec) {
            return wrapListData(index, d, spec, traits);
        } else if (spec instanceof StructDataSpec) {
            return wrapStructData(index, d, spec, traits);
        } else if (traits.hasTrait(DictEncodingTrait.class)) {
            var keyType = DictEncodingTrait.keyType(traits);
            if (spec instanceof StringDataSpec && !(d instanceof DictDecodedStringWriteData)) {
                if (!(d instanceof DictEncodedStringWriteData)) {
                    throw new IllegalArgumentException(
                        "Expected DictEncodedStringWriteData to construct DictDecodedStringWriteData");
                }
                return wrapDictEncodedStringData(d, m_cache.get(index, keyType));
            } else if (spec instanceof VarBinaryDataSpec && !(d instanceof DictDecodedVarBinaryWriteData)) {
                if (!(d instanceof DictEncodedVarBinaryWriteData)) {
                    throw new IllegalArgumentException(
                        "Expected DictEncodedVarBinaryWriteData to construct DictDecodedVarBinaryWriteData");
                }
                return wrapDictEncodedVarBinaryData(d, m_cache.get(index, keyType));
            }
        }
        return d;
    }

    private NullableWriteData wrapStructData(final DataIndex index, final NullableWriteData d, final DataSpec spec,
        final DataTraits traits) {
        final var structSpec = (StructDataSpec)spec;
        final var structTraits = (StructDataTraits)traits;
        return new DecoratedStructData.DecoratedStructWriteData(structSpec, (StructWriteData)d,
            (i, x) -> wrapDictEncodedData(index.getChild(i), x, structSpec.getDataSpec(i),
                structTraits.getDataTraits(i)));
    }

    /**
     * When wrapping lists we use the list index and not the sub-list index to query a {@link ColumnDictElementCache}
     * for the contained data, because within slices of a list we share the dictionary and the key generator.
     */
    private NullableWriteData wrapListData(final DataIndex index, final NullableWriteData d, final DataSpec spec,
        final DataTraits traits) {
        final var childTraits = ((ListDataTraits)traits).getInner();
        final var childSpec = ((ListDataSpec)spec).getInner();
        return new DecoratedListData.DecoratedListWriteData((ListWriteData)d,
            x -> wrapDictEncodedData(index, x, childSpec, childTraits));
    }

    @SuppressWarnings("unchecked")
    private static <K> NullableWriteData wrapDictEncodedStringData(final NullableWriteData data,
        final ColumnDictElementCache<K> cache) {
        return new DictDecodedStringWriteData<K>((DictEncodedStringWriteData<K>)data, cache);
    }

    @SuppressWarnings("unchecked")
    private static <K> NullableWriteData wrapDictEncodedVarBinaryData(final NullableWriteData data,
        final ColumnDictElementCache<K> cache) {
        return new DictDecodedVarBinaryWriteData<K>((DictEncodedVarBinaryWriteData<K>)data, cache);
    }

    @Override
    public void write(final ReadBatch batch) throws IOException {
        // Replace all DictEncoded Data objects by the content that they wrap
        var data = new NullableReadData[batch.numData()];
        Arrays.setAll(data, i -> unpack(batch.get(i)));
        var dictEncodedBatch = new DefaultReadBatch(data);
        m_delegate.write(dictEncodedBatch);
//        dictEncodedBatch.release();
    }

    private static NullableReadData unpack(NullableReadData data) {
        while (isDictDecodingWrapper(data)) {
            data = ((DecoratingNullableReadData)data).getReadDelegate();
        }
//        data.retain();
        return data;
    }

    private static boolean isDictDecodingWrapper(final NullableReadData data) {
        return data instanceof AbstractDictDecodedReadData //
                || data instanceof DecoratedListReadData //
                || data instanceof DecoratedStructReadData;
    }

}
