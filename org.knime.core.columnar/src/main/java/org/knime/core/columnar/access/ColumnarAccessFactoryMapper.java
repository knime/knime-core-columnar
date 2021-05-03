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
 *   Oct 8, 2020 (dietzc): created
 */
package org.knime.core.columnar.access;

import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
import org.knime.core.table.schema.DataSpec;
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

/**
 * Mapping AccessSpec to ColumnarValueFactory.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
public final class ColumnarAccessFactoryMapper implements DataSpec.Mapper<ColumnarAccessFactory> {

    static final ColumnarAccessFactoryMapper INSTANCE = new ColumnarAccessFactoryMapper();

    private ColumnarAccessFactoryMapper() {
    }

    /**
     * Creates a {@link ColumnarAccessFactory} for the provided {@link DataSpec}.
     *
     * @param dataSpec  {@link DataSpec} to create an {@link ColumnarAccessFactory} for
     * @return the {@link ColumnarAccessFactory} for {@link DataSpec}
     */
    public static ColumnarAccessFactory createAccessFactory(final DataSpec dataSpec) {
        return dataSpec.accept(INSTANCE);
    }

    @Override
    public ColumnarAccessFactory visit(final StructDataSpec spec) {
        return new ColumnarStructAccessFactory(spec.getInner());
    }

    @Override
    public ColumnarAccessFactory visit(final BooleanDataSpec spec) {
        return ColumnarBooleanAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory visit(final DoubleDataSpec spec) {
        return ColumnarDoubleAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory visit(final IntDataSpec spec) {
        return ColumnarIntAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory visit(final LongDataSpec spec) {
        return ColumnarLongAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory visit(final VoidDataSpec spec) {
        return ColumnarVoidAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory visit(final VarBinaryDataSpec spec) {
        return ColumnarVarBinaryAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory visit(final ListDataSpec spec) {
        return new ColumnarListAccessFactory<>(spec);
    }

    @Override
    public ColumnarAccessFactory visit(final LocalDateDataSpec spec) {
        return ColumnarLocalDateAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory visit(final LocalTimeDataSpec spec) {
        return ColumnarLocalTimeAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory visit(final LocalDateTimeDataSpec spec) {
        return ColumnarLocalDateTimeAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory visit(final DurationDataSpec spec) {
        return ColumnarDurationAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory visit(final PeriodDataSpec spec) {
        return ColumnarPeriodAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory visit(final ZonedDateTimeDataSpec spec) {
        return ColumnarZonedDateTimeAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory visit(final StringDataSpec spec) {
        return ColumnarStringAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory visit(final ByteDataSpec spec) {
        throw new UnsupportedOperationException("NYI");
    }

    @Override
    public ColumnarAccessFactory visit(final FloatDataSpec spec) {
        throw new UnsupportedOperationException("NYI");
    }

}
