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
package org.knime.core.columnar.data;

import org.knime.core.columnar.data.BooleanData.BooleanDataSpec;
import org.knime.core.columnar.data.ByteData.ByteDataSpec;
import org.knime.core.columnar.data.DoubleData.DoubleDataSpec;
import org.knime.core.columnar.data.DurationData.DurationDataSpec;
import org.knime.core.columnar.data.FloatData.FloatDataSpec;
import org.knime.core.columnar.data.IntData.IntDataSpec;
import org.knime.core.columnar.data.ListData.ListDataSpec;
import org.knime.core.columnar.data.LocalDateData.LocalDateDataSpec;
import org.knime.core.columnar.data.LocalDateTimeData.LocalDateTimeDataSpec;
import org.knime.core.columnar.data.LocalTimeData.LocalTimeDataSpec;
import org.knime.core.columnar.data.LongData.LongDataSpec;
import org.knime.core.columnar.data.ObjectData.GenericObjectDataSpec;
import org.knime.core.columnar.data.PeriodData.PeriodDataSpec;
import org.knime.core.columnar.data.StringData.StringDataSpec;
import org.knime.core.columnar.data.StructData.StructDataSpec;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryDataSpec;
import org.knime.core.columnar.data.VoidData.VoidDataSpec;
import org.knime.core.columnar.data.ZonedDateTimeData.ZonedDateTimeDataSpec;

@SuppressWarnings("javadoc")
public interface ColumnDataSpec {

    public static BooleanDataSpec booleanSpec() {
        return BooleanDataSpec.INSTANCE;
    }

    public static ByteDataSpec byteSpec() {
        return ByteDataSpec.INSTANCE;
    }

    public static DoubleDataSpec doubleSpec() {
        return DoubleDataSpec.INSTANCE;
    }

    public static DurationDataSpec durationSpec() {
        return DurationDataSpec.INSTANCE;
    }

    public static FloatDataSpec floatSpec() {
        return FloatDataSpec.INSTANCE;
    }

    public static IntDataSpec intSpec() {
        return IntDataSpec.INSTANCE;
    }

    public static LocalDateDataSpec localDateSpec() {
        return LocalDateDataSpec.INSTANCE;
    }

    public static LocalDateTimeDataSpec localDateTimeSpec() {
        return LocalDateTimeDataSpec.INSTANCE;
    }

    public static LongDataSpec longSpec() {
        return LongDataSpec.INSTANCE;
    }

    public static PeriodDataSpec periodSpec() {
        return PeriodDataSpec.INSTANCE;
    }

    public static VarBinaryDataSpec varBinarySpec() {
        return VarBinaryDataSpec.INSTANCE;
    }

    public static interface Mapper<R> {

        R visit(final BooleanDataSpec spec);

        R visit(final ByteDataSpec spec);

        R visit(final DoubleDataSpec spec);

        R visit(final DurationDataSpec spec);

        R visit(final FloatDataSpec spec);

        R visit(final IntDataSpec spec);

        R visit(final LocalDateDataSpec spec);

        R visit(final LocalDateTimeDataSpec spec);

        R visit(final LocalTimeDataSpec spec);

        R visit(final LongDataSpec spec);

        R visit(final PeriodDataSpec spec);

        R visit(final VarBinaryDataSpec spec);

        R visit(final VoidDataSpec spec);

        R visit(final GenericObjectDataSpec<?> spec);

        R visit(final StructDataSpec spec);

        R visit(final ListDataSpec listDataSpec);

        R visit(final ZonedDateTimeDataSpec spec);

        R visit(final StringDataSpec spec);
    }

    <R> R accept(Mapper<R> v);

}
