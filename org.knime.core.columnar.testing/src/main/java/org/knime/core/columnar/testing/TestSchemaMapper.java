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
 *   29 Oct 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.testing;

import java.util.Arrays;

import org.knime.core.columnar.testing.data.TestBooleanData.TestBooleanDataFactory;
import org.knime.core.columnar.testing.data.TestByteData.TestByteDataFactory;
import org.knime.core.columnar.testing.data.TestDataFactory;
import org.knime.core.columnar.testing.data.TestDoubleData.TestDoubleDataFactory;
import org.knime.core.columnar.testing.data.TestDurationData.TestDurationDataFactory;
import org.knime.core.columnar.testing.data.TestFloatData.TestFloatDataFactory;
import org.knime.core.columnar.testing.data.TestIntData.TestIntDataFactory;
import org.knime.core.columnar.testing.data.TestListData.TestListDataFactory;
import org.knime.core.columnar.testing.data.TestLocalDateData.TestLocalDateDataFactory;
import org.knime.core.columnar.testing.data.TestLocalDateTimeData.TestLocalDateTimeDataFactory;
import org.knime.core.columnar.testing.data.TestLocalTimeData.TestLocalTimeDataFactory;
import org.knime.core.columnar.testing.data.TestLongData.TestLongDataFactory;
import org.knime.core.columnar.testing.data.TestPeriodData.TestPeriodDataFactory;
import org.knime.core.columnar.testing.data.TestStringData.TestStringDataFactory;
import org.knime.core.columnar.testing.data.TestStructData.TestStructDataFactory;
import org.knime.core.columnar.testing.data.TestVarBinaryData.TestVarBinaryDataFactory;
import org.knime.core.columnar.testing.data.TestVoidData.TestVoidDataFactory;
import org.knime.core.columnar.testing.data.TestZonedDateTimeData.TestZonedDateTimeDataFactory;
import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
import org.knime.core.table.schema.DataSpec.Mapper;
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
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
final class TestSchemaMapper implements Mapper<TestDataFactory> {

    static final TestSchemaMapper INSTANCE = new TestSchemaMapper();

    private TestSchemaMapper() {
    }

    @Override
    public TestDataFactory visit(final BooleanDataSpec spec) {
        return TestBooleanDataFactory.INSTANCE;
    }

    @Override
    public TestDataFactory visit(final ByteDataSpec spec) {
        return TestByteDataFactory.INSTANCE;
    }

    @Override
    public TestDataFactory visit(final DoubleDataSpec spec) {
        return TestDoubleDataFactory.INSTANCE;
    }

    @Override
    public TestDataFactory visit(final DurationDataSpec spec) {
        return TestDurationDataFactory.INSTANCE;
    }

    @Override
    public TestDataFactory visit(final FloatDataSpec spec) {
        return TestFloatDataFactory.INSTANCE;
    }

    @Override
    public TestDataFactory visit(final IntDataSpec spec) {
        return TestIntDataFactory.INSTANCE;
    }

    @Override
    public TestDataFactory visit(final LocalDateDataSpec spec) {
        return TestLocalDateDataFactory.INSTANCE;
    }

    @Override
    public TestDataFactory visit(final LocalDateTimeDataSpec spec) {
        return TestLocalDateTimeDataFactory.INSTANCE;
    }

    @Override
    public TestDataFactory visit(final LocalTimeDataSpec spec) {
        return TestLocalTimeDataFactory.INSTANCE;
    }

    @Override
    public TestDataFactory visit(final LongDataSpec spec) {
        return TestLongDataFactory.INSTANCE;
    }

    @Override
    public TestDataFactory visit(final PeriodDataSpec spec) {
        return TestPeriodDataFactory.INSTANCE;
    }

    @Override
    public TestDataFactory visit(final VarBinaryDataSpec spec) {
        return TestVarBinaryDataFactory.INSTANCE;
    }

    @Override
    public TestDataFactory visit(final VoidDataSpec spec) {
        return TestVoidDataFactory.INSTANCE;
    }

    @Override
    public TestDataFactory visit(final StructDataSpec spec) {
        return new TestStructDataFactory(
            Arrays.stream(spec.getInner()).map(s -> s.accept(this)).toArray(TestDataFactory[]::new));
    }

    @Override
    public TestDataFactory visit(final ListDataSpec listDataSpec) {
        return new TestListDataFactory(listDataSpec.getInner().accept(this));
    }

    @Override
    public TestDataFactory visit(final ZonedDateTimeDataSpec spec) {
        return TestZonedDateTimeDataFactory.INSTANCE;
    }

    @Override
    public TestDataFactory visit(final StringDataSpec spec) {
        return TestStringDataFactory.INSTANCE;
    }
}
