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
 */
package org.knime.core.columnar.arrow;

import java.util.stream.IntStream;

import org.knime.core.columnar.arrow.data.ArrowBooleanData.ArrowBooleanDataFactory;
import org.knime.core.columnar.arrow.data.ArrowByteData.ArrowByteDataFactory;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedStringData.ArrowDictEncodedStringDataFactory;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedVarBinaryData.ArrowDictEncodedVarBinaryDataFactory;
import org.knime.core.columnar.arrow.data.ArrowDoubleData.ArrowDoubleDataFactory;
import org.knime.core.columnar.arrow.data.ArrowDurationData.ArrowDurationDataFactory;
import org.knime.core.columnar.arrow.data.ArrowFloatData.ArrowFloatDataFactory;
import org.knime.core.columnar.arrow.data.ArrowIntData.ArrowIntDataFactory;
import org.knime.core.columnar.arrow.data.ArrowListData.ArrowListDataFactory;
import org.knime.core.columnar.arrow.data.ArrowLocalDateData.ArrowLocalDateDataFactory;
import org.knime.core.columnar.arrow.data.ArrowLocalDateTimeData.ArrowLocalDateTimeDataFactory;
import org.knime.core.columnar.arrow.data.ArrowLocalTimeData.ArrowLocalTimeDataFactory;
import org.knime.core.columnar.arrow.data.ArrowLongData.ArrowLongDataFactory;
import org.knime.core.columnar.arrow.data.ArrowPeriodData.ArrowPeriodDataFactory;
import org.knime.core.columnar.arrow.data.ArrowStringData.ArrowStringDataFactory;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructDataFactory;
import org.knime.core.columnar.arrow.data.ArrowVarBinaryData.ArrowVarBinaryDataFactory;
import org.knime.core.columnar.arrow.data.ArrowVoidData.ArrowVoidDataFactory;
import org.knime.core.columnar.arrow.data.ArrowZonedDateTimeData.ArrowZonedDateTimeDataFactory;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DataSpec.MapperWithTraits;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.DurationDataSpec;
import org.knime.core.table.schema.FloatDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.LocalDateDataSpec;
import org.knime.core.table.schema.LocalDateTimeDataSpec;
import org.knime.core.table.schema.LocalTimeDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.PeriodDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VoidDataSpec;
import org.knime.core.table.schema.ZonedDateTimeDataSpec;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.ListDataTraits;
import org.knime.core.table.schema.traits.StructDataTraits;

/**
 * Utility class to map a {@link ColumnarSchema} to an array of {@link ArrowColumnDataFactory}. The factories can be
 * used to create, read or write the Arrow implementations of {@link NullableReadData} and {@link NullableWriteData}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
final class ArrowSchemaMapper implements MapperWithTraits<ArrowColumnDataFactory> {

    private static final ArrowSchemaMapper INSTANCE = new ArrowSchemaMapper();

    private ArrowSchemaMapper() {
        // Singleton and the instance is only used in #map
    }

    /**
     * Map each column of the {@link ColumnarSchema} to the according {@link ArrowColumnDataFactory}. The factory can
     * be used to create, read or write the Arrow implementation of {@link NullableReadData} and {@link NullableWriteData}.
     *
     * @param schema the schema of the column store
     * @return the factories
     */
    static ArrowColumnDataFactory[] map(final ColumnarSchema schema) {
        return IntStream.range(0, schema.numColumns()) //
            .mapToObj(i -> map(schema.getSpec(i), schema.getTraits(i)))
            .toArray(ArrowColumnDataFactory[]::new);
    }

    /**
     * Map a single {@link DataSpec} to the according {@link ArrowColumnDataFactory}. The factory can be used to
     * create, read or write the Arrow implementation of {@link NullableReadData} and {@link NullableWriteData}.
     *
     * @param spec the spec of the column
     * @return the factory
     */
    static ArrowColumnDataFactory map(final DataSpec spec, final DataTraits traits) {
        return spec.accept(INSTANCE, traits);
    }

    @Override
    public ArrowBooleanDataFactory visit(final BooleanDataSpec spec, final DataTraits traits) {
        return ArrowBooleanDataFactory.INSTANCE;
    }

    @Override
    public ArrowByteDataFactory visit(final ByteDataSpec spec, final DataTraits traits) {
        return ArrowByteDataFactory.INSTANCE;
    }

    @Override
    public ArrowDoubleDataFactory visit(final DoubleDataSpec spec, final DataTraits traits) {
        return ArrowDoubleDataFactory.INSTANCE;
    }

    @Override
    public ArrowDurationDataFactory visit(final DurationDataSpec spec, final DataTraits traits) {
        return ArrowDurationDataFactory.INSTANCE;
    }

    @Override
    public ArrowFloatDataFactory visit(final FloatDataSpec spec, final DataTraits traits) {
        return ArrowFloatDataFactory.INSTANCE;
    }

    @Override
    public ArrowIntDataFactory visit(final IntDataSpec spec, final DataTraits traits) {
        return ArrowIntDataFactory.INSTANCE;
    }

    @Override
    public ArrowLocalDateDataFactory visit(final LocalDateDataSpec spec, final DataTraits traits) {
        return ArrowLocalDateDataFactory.INSTANCE;
    }

    @Override
    public ArrowLocalDateTimeDataFactory visit(final LocalDateTimeDataSpec spec, final DataTraits traits) {
        return ArrowLocalDateTimeDataFactory.INSTANCE;
    }

    @Override
    public ArrowLocalTimeDataFactory visit(final LocalTimeDataSpec spec, final DataTraits traits) {
        return ArrowLocalTimeDataFactory.INSTANCE;
    }

    @Override
    public ArrowLongDataFactory visit(final LongDataSpec spec, final DataTraits traits) {
        return ArrowLongDataFactory.INSTANCE;
    }

    @Override
    public ArrowPeriodDataFactory visit(final PeriodDataSpec spec, final DataTraits traits) {
        return ArrowPeriodDataFactory.INSTANCE;
    }

    @Override
    public ArrowColumnDataFactory visit(final VarBinaryDataSpec spec, final DataTraits traits) {
        if (DictEncodingTrait.isEnabled(traits)) {
            return new ArrowDictEncodedVarBinaryDataFactory(traits);
        }
        return new ArrowVarBinaryDataFactory(traits);
    }

    @Override
    public ArrowVoidDataFactory visit(final VoidDataSpec spec, final DataTraits traits) {
        return ArrowVoidDataFactory.INSTANCE;
    }

    @Override
    public ArrowStructDataFactory visit(final StructDataSpec spec, final StructDataTraits traits) {

        final ArrowColumnDataFactory[] innerFactories = new ArrowColumnDataFactory[spec.size()];
        for (int i = 0; i < spec.size(); i++) {
            innerFactories[i] = ArrowSchemaMapper.map(spec.getDataSpec(i), traits.getDataTraits(i));
        }
        return new ArrowStructDataFactory(traits, innerFactories);
    }

    @Override
    public ArrowColumnDataFactory visit(final ListDataSpec listDataSpec, final ListDataTraits traits) {
        final ArrowColumnDataFactory inner = ArrowSchemaMapper.map(listDataSpec.getInner(), traits.getInner());
        return new ArrowListDataFactory(traits, inner);
    }

    @Override
    public ArrowZonedDateTimeDataFactory visit(final ZonedDateTimeDataSpec spec, final DataTraits traits) {
        return ArrowZonedDateTimeDataFactory.INSTANCE;
    }

    @Override
    public ArrowColumnDataFactory visit(final StringDataSpec spec, final DataTraits traits) {
        if (DictEncodingTrait.isEnabled(traits)) {
            return new ArrowDictEncodedStringDataFactory(traits);
        }
        return ArrowStringDataFactory.INSTANCE;
    }
}