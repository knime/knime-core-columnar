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

import org.knime.core.columnar.arrow.data.ArrowDoubleData.ArrowDoubleDataFactory;
import org.knime.core.columnar.arrow.data.ArrowFloatData.ArrowFloatDataFactory;
import org.knime.core.columnar.arrow.data.ArrowIntData.ArrowIntDataFactory;
import org.knime.core.columnar.arrow.data.ArrowLongData.ArrowLongDataFactory;
import org.knime.core.columnar.arrow.data.ArrowObjectData.ArrowObjectDataFactory;
import org.knime.core.columnar.arrow.data.ArrowVarBinaryData.ArrowVarBinaryDataFactory;
import org.knime.core.columnar.arrow.data.ArrowVoidData.ArrowVoidDataFactory;
import org.knime.core.columnar.data.BooleanData.BooleanDataSpec;
import org.knime.core.columnar.data.ByteData.ByteDataSpec;
import org.knime.core.columnar.data.ColumnDataSpec.Mapper;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.data.DoubleData.DoubleDataSpec;
import org.knime.core.columnar.data.DurationData.DurationDataSpec;
import org.knime.core.columnar.data.FloatData.FloatDataSpec;
import org.knime.core.columnar.data.IntData.IntDataSpec;
import org.knime.core.columnar.data.LocalDateData.LocalDateDataSpec;
import org.knime.core.columnar.data.LocalDateTimeData.LocalDateTimeDataSpec;
import org.knime.core.columnar.data.LocalTimeData.LocalTimeDataSpec;
import org.knime.core.columnar.data.LongData.LongDataSpec;
import org.knime.core.columnar.data.ObjectData.ObjectDataSpec;
import org.knime.core.columnar.data.PeriodData.PeriodDataSpec;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryDataSpec;
import org.knime.core.columnar.data.VoidData.VoidDataSpec;
import org.knime.core.columnar.store.ColumnStoreSchema;

/**
 * Utility class to map a {@link ColumnStoreSchema} to an array of {@link ArrowColumnDataFactory}. The factories can be
 * used to create, read or write the Arrow implementations of {@link ColumnReadData} and {@link ColumnWriteData}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
final class ArrowSchemaMapper implements Mapper<ArrowColumnDataFactory> {

    private static final ArrowSchemaMapper INSTANCE = new ArrowSchemaMapper();

    private ArrowSchemaMapper() {
        // Singleton and the instance is only used in #map
    }

    /**
     * Map each column of the {@link ColumnStoreSchema} to the according {@link ArrowColumnDataFactory}. The factory can
     * be used to create, read or write the Arrow implementation of {@link ColumnReadData} and {@link ColumnWriteData}.
     *
     * @param schema the schema of the column store
     * @return the factories
     */
    static ArrowColumnDataFactory[] map(final ColumnStoreSchema schema) {
        return IntStream.range(0, schema.getNumColumns()) //
            .mapToObj(schema::getColumnDataSpec) //
            .map(spec -> spec.accept(INSTANCE)) //
            .toArray(ArrowColumnDataFactory[]::new);
    }

    @Override
    public ArrowColumnDataFactory visit(final BooleanDataSpec spec) {
        throw new IllegalArgumentException("ColumnDataSpec " + spec.getClass().getName() + " not supported.");
    }

    @Override
    public ArrowColumnDataFactory visit(final ByteDataSpec spec) {
        throw new IllegalArgumentException("ColumnDataSpec " + spec.getClass().getName() + " not supported.");
    }

    @Override
    public ArrowDoubleDataFactory visit(final DoubleDataSpec spec) {
        return ArrowDoubleDataFactory.INSTANCE;
    }

    @Override
    public ArrowColumnDataFactory visit(final DurationDataSpec spec) {
        throw new IllegalArgumentException("ColumnDataSpec " + spec.getClass().getName() + " not supported.");
    }

    @Override
    public ArrowFloatDataFactory visit(final FloatDataSpec spec) {
        return ArrowFloatDataFactory.INSTANCE;
    }

    @Override
    public ArrowIntDataFactory visit(final IntDataSpec spec) {
        return ArrowIntDataFactory.INSTANCE;
    }

    @Override
    public ArrowColumnDataFactory visit(final LocalDateDataSpec spec) {
        throw new IllegalArgumentException("ColumnDataSpec " + spec.getClass().getName() + " not supported.");
    }

    @Override
    public ArrowColumnDataFactory visit(final LocalDateTimeDataSpec spec) {
        throw new IllegalArgumentException("ColumnDataSpec " + spec.getClass().getName() + " not supported.");
    }

    @Override
    public ArrowColumnDataFactory visit(final LocalTimeDataSpec spec) {
        throw new IllegalArgumentException("ColumnDataSpec " + spec.getClass().getName() + " not supported.");
    }

    @Override
    public ArrowLongDataFactory visit(final LongDataSpec spec) {
        return ArrowLongDataFactory.INSTANCE;
    }

    @Override
    public ArrowColumnDataFactory visit(final PeriodDataSpec spec) {
        throw new IllegalArgumentException("ColumnDataSpec " + spec.getClass().getName() + " not supported.");
    }

    @Override
    public ArrowVarBinaryDataFactory visit(final VarBinaryDataSpec spec) {
        return ArrowVarBinaryDataFactory.INSTANCE;
    }

    @Override
    public ArrowVoidDataFactory visit(final VoidDataSpec spec) {
        return ArrowVoidDataFactory.INSTANCE;
    }

    @Override
    public ArrowColumnDataFactory visit(final ObjectDataSpec<?> spec) {
        return new ArrowObjectDataFactory<>(spec.getSerializer());
    }
}