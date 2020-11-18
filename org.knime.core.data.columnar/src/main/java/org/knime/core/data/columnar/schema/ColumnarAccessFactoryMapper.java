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
package org.knime.core.data.columnar.schema;

import org.knime.core.data.v2.access.AccessSpec.AccessSpecMapper;
import org.knime.core.data.v2.access.BooleanAccess.BooleanAccessSpec;
import org.knime.core.data.v2.access.ByteArrayAccess.ByteArrayAccessSpec;
import org.knime.core.data.v2.access.DoubleAccess.DoubleAccessSpec;
import org.knime.core.data.v2.access.DurationAccess.DurationAccessSpec;
import org.knime.core.data.v2.access.IntAccess.IntAccessSpec;
import org.knime.core.data.v2.access.ListAccess.ListAccessSpec;
import org.knime.core.data.v2.access.LocalDateAccess.LocalDateAccessSpec;
import org.knime.core.data.v2.access.LocalDateTimeAccess.LocalDateTimeAccessSpec;
import org.knime.core.data.v2.access.LocalTimeAccess.LocalTimeAccessSpec;
import org.knime.core.data.v2.access.LongAccess.LongAccessSpec;
import org.knime.core.data.v2.access.ObjectAccess.ObjectAccessSpec;
import org.knime.core.data.v2.access.PeriodAccess.PeriodAccessSpec;
import org.knime.core.data.v2.access.StructAccess.StructAccessSpec;
import org.knime.core.data.v2.access.VoidAccess.VoidAccessSpec;
import org.knime.core.data.v2.access.ZonedDateTimeAccess.ZonedDateTimeAccessSpec;

/**
 * Mapping AccessSpec to ColumnarValueFactory.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
class ColumnarAccessFactoryMapper implements AccessSpecMapper<ColumnarAccessFactory<?, ?, ?, ?>> {

    public final static ColumnarAccessFactoryMapper INSTANCE = new ColumnarAccessFactoryMapper();

    private ColumnarAccessFactoryMapper() {
    }

    @Override
    public ColumnarAccessFactory<?, ?, ?, ?> visit(final StructAccessSpec spec) {
        return new ColumnarStructAccessFactory(spec.getInnerSpecs());
    }

    @Override
    public ColumnarAccessFactory<?, ?, ?, ?> visit(final ObjectAccessSpec<?> spec) {
        return new ColumnarObjectAccessFactory<>(spec);
    }

    @Override
    public ColumnarAccessFactory<?, ?, ?, ?> visit(final BooleanAccessSpec spec) {
        return ColumnarBooleanAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory<?, ?, ?, ?> visit(final DoubleAccessSpec spec) {
        return ColumnarDoubleAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory<?, ?, ?, ?> visit(final IntAccessSpec spec) {
        return ColumnarIntAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory<?, ?, ?, ?> visit(final LongAccessSpec spec) {
        return ColumnarLongAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory<?, ?, ?, ?> visit(final VoidAccessSpec spec) {
        return ColumnarVoidAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory<?, ?, ?, ?> visit(final ByteArrayAccessSpec spec) {
        // TODO
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public ColumnarAccessFactory<?, ?, ?, ?> visit(final ListAccessSpec<?, ?> spec) {
        return new ColumnarListAccessFactory<>(spec);
    }

    @Override
    public ColumnarAccessFactory<?, ?, ?, ?> visit(final LocalDateAccessSpec spec) {
        return ColumnarLocalDateAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory<?, ?, ?, ?> visit(final LocalTimeAccessSpec spec) {
        return ColumnarLocalTimeAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory<?, ?, ?, ?> visit(final LocalDateTimeAccessSpec spec) {
        return ColumnarLocalDateTimeAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory<?, ?, ?, ?> visit(final DurationAccessSpec spec) {
        return ColumnarDurationAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory<?, ?, ?, ?> visit(final PeriodAccessSpec spec) {
        return ColumnarPeriodAccessFactory.INSTANCE;
    }

    @Override
    public ColumnarAccessFactory<?, ?, ?, ?> visit(final ZonedDateTimeAccessSpec spec) {
        return ColumnarZonedDateTimeAccessFactory.INSTANCE;
    }
}
