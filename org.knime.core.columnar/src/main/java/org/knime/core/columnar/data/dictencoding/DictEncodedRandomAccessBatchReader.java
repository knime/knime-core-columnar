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

import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.dictencoding.DecoratedListData.DecoratedListReadData;
import org.knime.core.columnar.data.dictencoding.DecoratedStructData.DecoratedStructReadData;
import org.knime.core.columnar.data.dictencoding.DictDecodedStringData.DictDecodedStringReadData;
import org.knime.core.columnar.data.dictencoding.DictDecodedVarBinaryData.DictDecodedVarBinaryReadData;
import org.knime.core.columnar.data.dictencoding.DictElementCache.ColumnDictElementCache;
import org.knime.core.columnar.data.dictencoding.DictElementCache.DataIndex;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedStringReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedVarBinaryReadData;
import org.knime.core.columnar.filter.ColumnSelection;
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
 * Wraps a delegate{@link RandomAccessBatchReader} and upon access wraps dictionary encoded data coming from the backend
 * and provides plain-looking Data objects to the frontend.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class DictEncodedRandomAccessBatchReader implements RandomAccessBatchReader {
    private final RandomAccessBatchReader m_delegate;

    private final ColumnarSchema m_schema;

    private final ColumnSelection m_selection;

    private final DictElementCache m_cache;

    /**
     * Create a {@link DictEncodedRandomAccessBatchReader} from a delegate, {@link ColumnSelection} and
     * {@link ColumnarSchema}.
     *
     * @param delegate The delegate {@link RandomAccessBatchReadable}
     * @param selection The column selection
     * @param schema The columnar schema
     * @param cache The table-wide {@link DictElementCache}
     */
    public DictEncodedRandomAccessBatchReader(final RandomAccessBatchReadable delegate, final ColumnSelection selection,
        final ColumnarSchema schema, final DictElementCache cache) {
        m_delegate = delegate.createRandomAccessReader(selection);
        m_selection = selection;
        m_schema = schema;
        m_cache = cache;
    }

    @Override
    public void close() throws IOException {
        m_delegate.close();
    }

    @Override
    public ReadBatch readRetained(final int index) throws IOException {
        final ReadBatch batch = m_delegate.readRetained(index);
        return m_selection.createBatch(i -> wrapDictEncodedData(batch, i));
    }

    private NullableReadData wrapDictEncodedData(final ReadBatch batch, final int index) {
        final var data = batch.get(index);
        final var traits = m_schema.getTraits(index);

        if (!DataTraitUtils.containsDataTrait(DictEncodingTrait.class, traits)) {
            return data;
        }

        final var spec = m_schema.getSpec(index);
        return wrapDictEncodedData(DataIndex.createColumnIndex(index), data, spec, traits);
    }

    private NullableReadData wrapDictEncodedData(final DataIndex index, final NullableReadData data,
        final DataSpec spec, final DataTraits traits) {

        if (spec instanceof ListDataSpec) {
            return wrapListData(index, data, spec, traits);
        } else if (spec instanceof StructDataSpec) {
            return wrapStructData(index, data, spec, traits);
        } else if (DictEncodingTrait.isEnabled(traits)) {
            var keyType = DictEncodingTrait.keyType(traits);
            if (spec instanceof StringDataSpec && !(data instanceof DictDecodedStringReadData)) {
                if (!(data instanceof DictEncodedStringReadData)) {
                    throw new IllegalArgumentException(
                        "Expected DictEncodedStringReadData to construct DictDecodedStringReadData");
                }
                return wrapDictEncodedStringData(data, m_cache.get(index, keyType));
            } else if (spec instanceof VarBinaryDataSpec && !(data instanceof DictDecodedVarBinaryReadData)) {
                if (!(data instanceof DictEncodedVarBinaryReadData)) {
                    throw new IllegalArgumentException(
                        "Expected DictEncodedVarBinaryReadData to construct DictDecodedVarBinaryReadData");
                }
                return wrapDictEncodedVarBinaryData(data, m_cache.get(index, keyType));
            }
        }
        return data;
    }

    private NullableReadData wrapStructData(final DataIndex index, final NullableReadData data, final DataSpec spec,
        final DataTraits traits) {
        final var structSpec = (StructDataSpec)spec;
        final var structTraits = (StructDataTraits)traits;
        return new DecoratedStructReadData((StructReadData)data, (i, x) -> wrapDictEncodedData(index.createChild(i),
            x, structSpec.getDataSpec(i), structTraits.getDataTraits(i)));
    }

    /**
     * When wrapping lists we use the list index and not the sub-list index to query a {@link ColumnDictElementCache}
     * for the contained data, because within slices of a list we share the dictionary and the key generator.
     */
    private NullableReadData wrapListData(final DataIndex index, final NullableReadData data, final DataSpec spec,
        final DataTraits traits) {
        final var childSpec = ((ListDataSpec)spec).getInner();
        final var childTraits = ((ListDataTraits)traits).getInner();
        return new DecoratedListReadData((ListReadData)data,
            x -> wrapDictEncodedData(index, x, childSpec, childTraits));
    }


    @SuppressWarnings("unchecked")
    private static <K> NullableReadData wrapDictEncodedStringData(final NullableReadData data,
        final ColumnDictElementCache<K> cache) {
        return new DictDecodedStringReadData<K>((DictEncodedStringReadData<K>)data, cache);
    }

    @SuppressWarnings("unchecked")
    private static <K> NullableReadData wrapDictEncodedVarBinaryData(final NullableReadData data,
        final ColumnDictElementCache<K> cache) {
        return new DictDecodedVarBinaryReadData<K>((DictEncodedVarBinaryReadData<K>)data, cache);
    }
}
