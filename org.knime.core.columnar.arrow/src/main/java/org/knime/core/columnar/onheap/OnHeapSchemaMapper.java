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
 *   Nov 22, 2024 (benjamin): created
 */
package org.knime.core.columnar.onheap;

import java.util.stream.IntStream;

import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.onheap.data.OnHeapDoubleData;
import org.knime.core.columnar.onheap.data.OnHeapStringData;
import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.FloatDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VoidDataSpec;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.ListDataTraits;
import org.knime.core.table.schema.traits.StructDataTraits;

/**
 * TODO caching TODO extension types Just see ArrowSchemaMapper
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class OnHeapSchemaMapper implements DataSpec.MapperWithTraits<OnHeapDataFactory> {

    private static final OnHeapSchemaMapper INSTANCE = new OnHeapSchemaMapper();

    private OnHeapSchemaMapper() {
    }

    /**
     * Map each column of the {@link ColumnarSchema} to the according {@link ArrowColumnDataFactory}. The factory can be
     * used to create, read or write the Arrow implementation of {@link NullableReadData} and {@link NullableWriteData}.
     *
     * @param schema the schema of the column store
     * @return the factories
     */
    static OnHeapDataFactory[] map(final ColumnarSchema schema) {
        return IntStream.range(0, schema.numColumns()) //
            .mapToObj(i -> map(schema.getSpec(i), schema.getTraits(i))) //
            .toArray(OnHeapDataFactory[]::new);
    }

    /**
     * Map a single {@link DataSpec} to the according {@link ArrowColumnDataFactory}. The factory can be used to create,
     * read or write the Arrow implementation of {@link NullableReadData} and {@link NullableWriteData}.
     *
     * @param spec the spec of the column
     * @return the factory
     */
    static OnHeapDataFactory map(final DataSpec spec, final DataTraits traits) {
        return spec.accept(INSTANCE, traits);
    }

    @Override
    public OnHeapDataFactory visit(final BooleanDataSpec spec, final DataTraits traits) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public OnHeapDataFactory visit(final ByteDataSpec spec, final DataTraits traits) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public OnHeapDataFactory visit(final DoubleDataSpec spec, final DataTraits traits) {
        return OnHeapDoubleData.FACTORY;
    }

    @Override
    public OnHeapDataFactory visit(final FloatDataSpec spec, final DataTraits traits) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public OnHeapDataFactory visit(final IntDataSpec spec, final DataTraits traits) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public OnHeapDataFactory visit(final LongDataSpec spec, final DataTraits traits) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public OnHeapDataFactory visit(final VarBinaryDataSpec spec, final DataTraits traits) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public OnHeapDataFactory visit(final VoidDataSpec spec, final DataTraits traits) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public OnHeapDataFactory visit(final StructDataSpec spec, final StructDataTraits traits) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public OnHeapDataFactory visit(final ListDataSpec listDataSpec, final ListDataTraits traits) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public OnHeapDataFactory visit(final StringDataSpec spec, final DataTraits traits) {
        return OnHeapStringData.FACTORY;
    }
}
