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

import org.knime.core.columnar.ReferencedData;
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

/**
 * Specification / configuration of implementations of {@link ReferencedData data}.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public interface DataSpec {

    /**
     * @return singleton boolean spec
     */
    static BooleanDataSpec booleanSpec() {
        return BooleanDataSpec.INSTANCE;
    }

    /**
     * @return singleton byte spec
     */
    static ByteDataSpec byteSpec() {
        return ByteDataSpec.INSTANCE;
    }

    /**
     * @return singleton double spec
     */
    static DoubleDataSpec doubleSpec() {
        return DoubleDataSpec.INSTANCE;
    }

    /**
     * @return singleton Duration spec
     */
    static DurationDataSpec durationSpec() {
        return DurationDataSpec.INSTANCE;
    }

    /**
     * @return singleton float spec
     */
    static FloatDataSpec floatSpec() {
        return FloatDataSpec.INSTANCE;
    }

    /**
     * @return singleton int spec
     */
    static IntDataSpec intSpec() {
        return IntDataSpec.INSTANCE;
    }

    /**
     * @return singleton LocalDate spec
     */
    static LocalDateDataSpec localDateSpec() {
        return LocalDateDataSpec.INSTANCE;
    }

    /**
     * @return singleton LocalDateTime spec
     */
    static LocalDateTimeDataSpec localDateTimeSpec() {
        return LocalDateTimeDataSpec.INSTANCE;
    }

    /**
     * @return singleton LocalTime spec
     */
    static LocalTimeDataSpec localTimeSpec() {
        return LocalTimeDataSpec.INSTANCE;
    }

    /**
     * @return singleton long spec
     */
    static LongDataSpec longSpec() {
        return LongDataSpec.INSTANCE;
    }

    /**
     * @return singleton Period spec
     */
    static PeriodDataSpec periodSpec() {
        return PeriodDataSpec.INSTANCE;
    }

    /**
     * @return singleton String spec
     */
    static StringDataSpec stringSpec() {
        return StringDataSpec.INSTANCE;
    }

    /**
     * @return singleton VarBinary spec
     */
    static VarBinaryDataSpec varBinarySpec() {
        return VarBinaryDataSpec.INSTANCE;
    }

    /**
     * @return singleton void spec
     */
    static VoidDataSpec voidSpec() {
        return VoidDataSpec.INSTANCE;
    }

    /**
     * @return singleton ZonedDateTime spec
     */
    static ZonedDateTimeDataSpec zonedDateTimeSpec() {
        return ZonedDateTimeDataSpec.INSTANCE;
    }

    /**
     * A visitor that visits {@link DataSpec DataSpecs}, mapping them to other objects of a certain type R.
     *
     * @param <R> the return type of the mapping
     */
    static interface Mapper<R> {

        R visit(BooleanDataSpec spec);

        R visit(ByteDataSpec spec);

        R visit(DoubleDataSpec spec);

        R visit(DurationDataSpec spec);

        R visit(FloatDataSpec spec);

        R visit(IntDataSpec spec);

        R visit(LocalDateDataSpec spec);

        R visit(LocalDateTimeDataSpec spec);

        R visit(LocalTimeDataSpec spec);

        R visit(LongDataSpec spec);

        R visit(PeriodDataSpec spec);

        R visit(VarBinaryDataSpec spec);

        R visit(VoidDataSpec spec);

        R visit(GenericObjectDataSpec<?> spec);

        R visit(StructDataSpec spec);

        R visit(ListDataSpec listDataSpec);

        R visit(ZonedDateTimeDataSpec spec);

        R visit(StringDataSpec spec);

    }

    /**
     * Accept the visit of a {@link Mapper}, returning the result of the mapper's visit.
     *
     * @param <R> the return type of the mapping
     * @param mapper the visiting mapper
     * @return other an object of type R
     */
    <R> R accept(Mapper<R> mapper);

}
