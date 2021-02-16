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
import static org.junit.Assert.assertNull;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.knime.core.columnar.testing.data.TestDoubleData.TestDoubleDataFactory;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.DataType;
import org.knime.core.data.def.DoubleCell;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnarDoubleDomainCalculatorTest {

    private static final DataCell MISSING_CELL = DataType.getMissingCell();

    private static final DoubleCell D0 = new DoubleCell(0d);

    private static final DoubleCell D1 = new DoubleCell(1d);

    private static final DoubleCell D2 = new DoubleCell(2d);

    @Test
    public void testUpdateWithData() {
        final ColumnarDoubleDomainCalculator calc = new ColumnarDoubleDomainCalculator();
        calc.update(TestDoubleDataFactory.INSTANCE.createReadData(new Double[]{null, 1d, 0d, 2d}));
        final DataColumnDomain domain = calc.get();
        assertEquals(D0, domain.getLowerBound());
        assertEquals(D2, domain.getUpperBound());
        assertNull(domain.getValues());
    }

    @Test
    public void testUpdateVoid() {
        final ColumnarDoubleDomainCalculator calc = new ColumnarDoubleDomainCalculator();
        calc.update(new DataColumnDomainCreator().createDomain());
        final DataColumnDomain domain = calc.get();
        assertNull(domain.getLowerBound());
        assertNull(domain.getUpperBound());
        assertNull(domain.getValues());
    }

    @Test
    public void testUpdateValues() {
        final ColumnarDoubleDomainCalculator calc = new ColumnarDoubleDomainCalculator();
        final DataColumnDomainCreator creator = new DataColumnDomainCreator();
        final Set<DoubleCell> set = Stream.of(D1).collect(Collectors.toSet());
        creator.setValues(set);
        calc.update(creator.createDomain());
        final DataColumnDomain domain = calc.get();
        assertNull(domain.getLowerBound());
        assertNull(domain.getUpperBound());
        assertEquals(set, domain.getValues());
    }

    @Test
    public void testUpdateLowerBoundMissing() {
        final ColumnarDoubleDomainCalculator calc = new ColumnarDoubleDomainCalculator();
        final DataColumnDomainCreator creator = new DataColumnDomainCreator();
        creator.setLowerBound(MISSING_CELL);
        creator.setUpperBound(D0);
        calc.update(creator.createDomain());
        final DataColumnDomain domain = calc.get();
        assertNull(domain.getLowerBound());
        assertNull(domain.getUpperBound());
        assertNull(domain.getValues());
    }

    @Test
    public void testUpdateUpperBoundMissing() {
        final ColumnarDoubleDomainCalculator calc = new ColumnarDoubleDomainCalculator();
        final DataColumnDomainCreator creator = new DataColumnDomainCreator();
        creator.setLowerBound(D0);
        creator.setUpperBound(MISSING_CELL);
        calc.update(creator.createDomain());
        final DataColumnDomain domain = calc.get();
        assertNull(domain.getLowerBound());
        assertNull(domain.getUpperBound());
        assertNull(domain.getValues());
    }

    @Test
    public void testUpdateAllThrice() {
        final ColumnarDoubleDomainCalculator calc = new ColumnarDoubleDomainCalculator();
        final DataColumnDomainCreator creator = new DataColumnDomainCreator();

        creator.setLowerBound(D1);
        creator.setUpperBound(D1);
        calc.update(creator.createDomain());

        creator.setLowerBound(D0);
        creator.setUpperBound(D2);
        creator.setValues(Stream.of(D0, D2).collect(Collectors.toSet()));
        calc.update(creator.createDomain());

        creator.setLowerBound(D1);
        creator.setUpperBound(D1);
        creator.setValues(Stream.of(D1, MISSING_CELL).collect(Collectors.toSet()));
        calc.update(creator.createDomain());

        final DataColumnDomain domain = calc.get();
        assertEquals(D0, domain.getLowerBound());
        assertEquals(D2, domain.getUpperBound());
        assertEquals(Stream.of(D0, D1, D2).collect(Collectors.toSet()), domain.getValues());
    }

}
