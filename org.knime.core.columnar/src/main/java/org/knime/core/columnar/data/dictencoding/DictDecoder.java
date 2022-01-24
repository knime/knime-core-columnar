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
 *   Dec 20, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.data.dictencoding;

import org.knime.core.columnar.cache.DataIndex;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.dictencoding.DecoratedListData.DecoratedListReadData;
import org.knime.core.columnar.data.dictencoding.DecoratedStructData.DecoratedStructReadData;
import org.knime.core.columnar.data.dictencoding.DictDecodedStringData.DictDecodedStringReadData;
import org.knime.core.columnar.data.dictencoding.DictDecodedVarBinaryData.DictDecodedVarBinaryReadData;
import org.knime.core.columnar.data.dictencoding.DictElementCache.ColumnDictElementCache;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedStringReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedVarBinaryReadData;
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
 * Handles decoding of dict encoded {@link NullableReadData} objects.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class DictDecoder {

    private final DictElementCache m_cache;

    /**
     * Constructs a DictDecoder with the provided cache.
     *
     * @param cache for caching
     */
    public DictDecoder(final DictElementCache cache) {
        m_cache = cache;
    }

    /**
     * Decodes the provided data (or returns it unchanged if it isn't dictionary encoded).
     *
     * @param index of data within the nested data structure
     * @param data potentially dict encoded
     * @param spec of data
     * @param traits of data
     * @return the decoded data or data if it wasn't dictionary encoded
     */
    public NullableReadData decode(final DataIndex index, final NullableReadData data,
        final DataSpec spec, final DataTraits traits) {
        if (!DataTraitUtils.containsDataTrait(DictEncodingTrait.class, traits)) {
            return data;
        }

        if (spec instanceof ListDataSpec) {
            return wrapListData(index, data, spec, traits);
        } else if (spec instanceof StructDataSpec) {
            return wrapStructData(index, data, spec, traits);
        } else if (DictEncodingTrait.isEnabled(traits)) {
            var keyType = DictEncodingTrait.keyType(traits);
            if (spec instanceof StringDataSpec && !(data instanceof DictDecodedStringReadData)) {
                return wrapDictEncodedStringData(data, m_cache.get(index, keyType));
            } else if (spec instanceof VarBinaryDataSpec && !(data instanceof DictDecodedVarBinaryReadData)) {
                return wrapDictEncodedVarBinaryData(data, m_cache.get(index, keyType));
            }
        }
        throw new IllegalArgumentException("Dictionary encoding isn't supported for " + spec);
    }

    private NullableReadData wrapStructData(final DataIndex index, final NullableReadData data, final DataSpec spec,
        final DataTraits traits) {
        final var structSpec = (StructDataSpec)spec;
        final var structTraits = (StructDataTraits)traits;
        return new DecoratedStructReadData(structSpec, (StructReadData)data, (i,
            x) -> decode(index.getChild(i), x, structSpec.getDataSpec(i), structTraits.getDataTraits(i)));
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
            x -> decode(index, x, childSpec, childTraits));
    }


    @SuppressWarnings("unchecked")
    private static <K> NullableReadData wrapDictEncodedStringData(final NullableReadData data,
        final ColumnDictElementCache<K> cache) {
        if (!(data instanceof DictEncodedStringReadData)) {
            throw new IllegalArgumentException(
                "Expected DictEncodedStringReadData to construct DictDecodedStringReadData but received "
                    + data.getClass());
        }
        return new DictDecodedStringReadData<K>((DictEncodedStringReadData<K>)data);
    }

    @SuppressWarnings("unchecked")
    private static <K> NullableReadData wrapDictEncodedVarBinaryData(final NullableReadData data,
        final ColumnDictElementCache<K> cache) {
        if (!(data instanceof DictEncodedVarBinaryReadData)) {
            throw new IllegalArgumentException(
                "Expected DictEncodedVarBinaryReadData to construct DictDecodedVarBinaryReadData but received "
                    + data.getClass());
        }
        return new DictDecodedVarBinaryReadData<K>((DictEncodedVarBinaryReadData<K>)data);
    }
}
