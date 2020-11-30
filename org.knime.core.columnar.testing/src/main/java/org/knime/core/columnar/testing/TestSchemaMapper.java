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

import org.knime.core.columnar.data.BooleanData.BooleanDataSpec;
import org.knime.core.columnar.data.ByteData.ByteDataSpec;
import org.knime.core.columnar.data.ColumnDataSpec.Mapper;
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
import org.knime.core.columnar.testing.TestDoubleData.TestDoubleDataFactory;
import org.knime.core.columnar.testing.TestVoidData.TestVoidDataFactory;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
final class TestSchemaMapper implements Mapper<TestDataFactory> {

    static final TestSchemaMapper INSTANCE = new TestSchemaMapper();

    private TestSchemaMapper() {
    }

    @Override
    public TestDataFactory visit(final BooleanDataSpec spec) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    @Override
    public TestDataFactory visit(final ByteDataSpec spec) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    @Override
    public TestDataFactory visit(final DoubleDataSpec spec) {
        return TestDoubleDataFactory.INSTANCE;
    }

    @Override
    public TestDataFactory visit(final DurationDataSpec spec) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    @Override
    public TestDataFactory visit(final FloatDataSpec spec) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    @Override
    public TestDataFactory visit(final IntDataSpec spec) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    @Override
    public TestDataFactory visit(final LocalDateDataSpec spec) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    @Override
    public TestDataFactory visit(final LocalDateTimeDataSpec spec) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    @Override
    public TestDataFactory visit(final LocalTimeDataSpec spec) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    @Override
    public TestDataFactory visit(final LongDataSpec spec) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    @Override
    public TestDataFactory visit(final PeriodDataSpec spec) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    @Override
    public TestDataFactory visit(final VarBinaryDataSpec spec) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    @Override
    public TestDataFactory visit(final VoidDataSpec spec) {
        return TestVoidDataFactory.INSTANCE;
    }

    @Override
    public TestDataFactory visit(final GenericObjectDataSpec<?> spec) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    @Override
    public TestDataFactory visit(final StructDataSpec spec) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    @Override
    public TestDataFactory visit(final ListDataSpec listDataSpec) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    @Override
    public TestDataFactory visit(final ZonedDateTimeDataSpec spec) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    @Override
    public TestDataFactory visit(final StringDataSpec spec) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }
}
