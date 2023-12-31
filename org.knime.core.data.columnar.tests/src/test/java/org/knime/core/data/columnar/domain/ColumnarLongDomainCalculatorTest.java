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
import org.knime.core.columnar.testing.data.TestLongData.TestLongDataFactory;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.DataType;
import org.knime.core.data.def.LongCell;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnarLongDomainCalculatorTest {

    private static final DataCell MISSING_CELL = DataType.getMissingCell();

    private static final LongCell L0 = new LongCell(0L);

    private static final LongCell L1 = new LongCell(1L);

    private static final LongCell L2 = new LongCell(2L);

    @Test
    public void testUpdateWithData() {
        final ColumnarLongDomainCalculator calc = new ColumnarLongDomainCalculator();
        calc.update(TestLongDataFactory.INSTANCE.createReadData(new Long[]{null, 1L, 0L, 2L}));
        final DataColumnDomain domain = calc.createDomain();
        assertEquals(L0, domain.getLowerBound());
        assertEquals(L2, domain.getUpperBound());
        assertNull(domain.getValues());
    }

    @Test
    public void testUpdateVoid() {
        final ColumnarLongDomainCalculator calc = new ColumnarLongDomainCalculator();
        calc.update(new DataColumnDomainCreator().createDomain());
        final DataColumnDomain domain = calc.createDomain();
        assertNull(domain.getLowerBound());
        assertNull(domain.getUpperBound());
        assertNull(domain.getValues());
    }

    @Test
    public void testUpdateValues() {
        final ColumnarLongDomainCalculator calc = new ColumnarLongDomainCalculator();
        final DataColumnDomainCreator creator = new DataColumnDomainCreator();
        final Set<LongCell> set = Stream.of(L1).collect(Collectors.toSet());
        creator.setValues(set);
        calc.update(creator.createDomain());
        final DataColumnDomain domain = calc.createDomain();
        assertNull(domain.getLowerBound());
        assertNull(domain.getUpperBound());
        assertEquals(set, domain.getValues());
    }

    @Test
    public void testUpdateLowerBoundMissing() {
        final ColumnarLongDomainCalculator calc = new ColumnarLongDomainCalculator();
        final DataColumnDomainCreator creator = new DataColumnDomainCreator();
        creator.setLowerBound(MISSING_CELL);
        creator.setUpperBound(L0);
        calc.update(creator.createDomain());
        final DataColumnDomain domain = calc.createDomain();
        assertNull(domain.getLowerBound());
        assertNull(domain.getUpperBound());
        assertNull(domain.getValues());
    }

    @Test
    public void testUpdateUpperBoundMissing() {
        final ColumnarLongDomainCalculator calc = new ColumnarLongDomainCalculator();
        final DataColumnDomainCreator creator = new DataColumnDomainCreator();
        creator.setLowerBound(L0);
        creator.setUpperBound(MISSING_CELL);
        calc.update(creator.createDomain());
        final DataColumnDomain domain = calc.createDomain();
        assertNull(domain.getLowerBound());
        assertNull(domain.getUpperBound());
        assertNull(domain.getValues());
    }

    @Test
    public void testUpdateAllThrice() {
        final ColumnarLongDomainCalculator calc = new ColumnarLongDomainCalculator();
        final DataColumnDomainCreator creator = new DataColumnDomainCreator();

        creator.setLowerBound(L1);
        creator.setUpperBound(L1);
        calc.update(creator.createDomain());

        creator.setLowerBound(L0);
        creator.setUpperBound(L2);
        creator.setValues(Stream.of(L0, L2).collect(Collectors.toSet()));
        calc.update(creator.createDomain());

        creator.setLowerBound(L1);
        creator.setUpperBound(L1);
        creator.setValues(Stream.of(L1, MISSING_CELL).collect(Collectors.toSet()));
        calc.update(creator.createDomain());

        final DataColumnDomain domain = calc.createDomain();
        assertEquals(L0, domain.getLowerBound());
        assertEquals(L2, domain.getUpperBound());
        assertEquals(Stream.of(L0, L1, L2).collect(Collectors.toSet()), domain.getValues());
    }

}
