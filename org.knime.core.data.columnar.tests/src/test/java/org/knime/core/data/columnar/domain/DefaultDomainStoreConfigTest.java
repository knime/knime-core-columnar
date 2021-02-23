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
 *   16 Feb 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.data.BoundedValue;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.NominalValue;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.ValueSchema;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings({"serial", "javadoc"})
public class DefaultDomainStoreConfigTest {

    private abstract static class DefaultDomainStoreConfigTestCell extends DataCell {
        @Override
        public String toString() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected boolean equalsDataCell(final DataCell dc) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int hashCode() { // NOSONAR
            throw new UnsupportedOperationException();
        }
    }

    private static class BoundedCell extends DefaultDomainStoreConfigTestCell implements BoundedValue {
    }

    private static class NominalCell extends DefaultDomainStoreConfigTestCell implements NominalValue {
    }

    private static class BoundedNominalCell extends DefaultDomainStoreConfigTestCell
        implements BoundedValue, NominalValue {
    }

    private static class NoDomainCell extends DefaultDomainStoreConfigTestCell {
    }

    private static ColumnarValueSchema createSchema(final DataColumnSpec... specs) {
        final DataTableSpec spec = new DataTableSpec(specs);
        final ValueSchema valueSchema =
            ValueSchema.create(spec, RowKeyType.CUSTOM, NotInWorkflowWriteFileStoreHandler.create());
        return ColumnarValueSchemaUtils.create(valueSchema);
    }

    private static final DataColumnSpec INT = new DataColumnSpecCreator("int", IntCell.TYPE).createSpec();

    private static final DataColumnSpec LONG = new DataColumnSpecCreator("long", LongCell.TYPE).createSpec();

    private static final DataColumnSpec DOUBLE = new DataColumnSpecCreator("double", DoubleCell.TYPE).createSpec();

    private static final DataColumnSpec STRING = new DataColumnSpecCreator("string", StringCell.TYPE).createSpec();

    private static final DataColumnSpec BOOLEAN = new DataColumnSpecCreator("boolean", BooleanCell.TYPE).createSpec();

    private static final DataColumnSpec BOUNDED =
        new DataColumnSpecCreator("bounded", DataType.getType(BoundedCell.class)).createSpec();

    private static final DataColumnSpec NOMINAL =
        new DataColumnSpecCreator("nominal", DataType.getType(NominalCell.class)).createSpec();

    private static final DataColumnSpec BOUNDED_NOMINAL =
        new DataColumnSpecCreator("bounded_nominal", DataType.getType(BoundedNominalCell.class)).createSpec();

    private static final DataColumnSpec NO_DOMAIN =
        new DataColumnSpecCreator("no_domain", DataType.getType(NoDomainCell.class)).createSpec();

    @Test
    public void testDomainCalculators() {
        final DefaultDomainStoreConfig config = new DefaultDomainStoreConfig(
            createSchema(INT, LONG, DOUBLE, STRING, BOOLEAN, BOUNDED, NOMINAL, BOUNDED_NOMINAL, NO_DOMAIN), 0, true);
        final Map<Integer, ColumnarDomainCalculator<? extends NullableReadData, DataColumnDomain>> calculators =
            config.createDomainCalculators();
        assertTrue(calculators.get(1) instanceof ColumnarIntDomainCalculator);
        assertTrue(calculators.get(2) instanceof ColumnarLongDomainCalculator);
        assertTrue(calculators.get(3) instanceof ColumnarDoubleDomainCalculator);
        assertTrue(calculators.get(4) instanceof ColumnarStringDomainCalculator);
        assertTrue(calculators.get(5) instanceof ColumnarBooleanDomainCalculator);
        assertTrue(calculators.get(6) instanceof ColumnarBoundedDomainCalculator);
        assertTrue(calculators.get(7) instanceof ColumnarNominalDomainCalculator);
        assertTrue(calculators.get(8) instanceof ColumnarCombinedDomainCalculator);
        assertFalse(calculators.containsKey(9));
        assertEquals(calculators, config.createDomainCalculators());
    }

    @Test
    public void testInitDomain() {
        final Set<DataCell> values = Stream.of(new IntCell(42)).collect(Collectors.toSet());
        final DataColumnDomainCreator domainCreator = new DataColumnDomainCreator();
        domainCreator.setValues(values);
        final DataColumnDomain domain = domainCreator.createDomain();
        final DataColumnSpecCreator specCreator = new DataColumnSpecCreator("int", IntCell.TYPE);
        specCreator.setDomain(domain);
        final DataColumnSpec spec = specCreator.createSpec();
        final DefaultDomainStoreConfig config = new DefaultDomainStoreConfig(createSchema(spec), 0, true);
        assertEquals(values, config.createDomainCalculators().get(1).createDomain().getValues());
    }

}
