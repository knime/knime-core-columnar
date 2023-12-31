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

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.testing.data.TestIntData.TestIntDataFactory;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.DataValueComparatorDelegator;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.v2.ReadValue;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnarCombinedDomainCalculatorTest {

    private static final IntCell I0 = new IntCell(0);

    private static final IntCell I1 = new IntCell(1);

    private static final IntCell I2 = new IntCell(2);

    private static ColumnarCombinedDomainCalculator<IntReadData> createCalculator(final int maxNumvalues) {
        return new ColumnarCombinedDomainCalculator<>((data, index) -> new ReadValue() {

            @Override
            public DataCell getDataCell() {
                return new IntCell(data.getInt(index.getIndex()));
            }
        }, new DataValueComparatorDelegator<>(IntCell.TYPE.getComparator()), maxNumvalues);
    }

    @Test
    public void testUpdateWithData() {
        final ColumnarCombinedDomainCalculator<IntReadData> calc = createCalculator(60);
        calc.update(TestIntDataFactory.INSTANCE.createReadData(new Integer[]{null, 1, 0, 2}));
        final DataColumnDomain domain = calc.createDomain();
        assertEquals(I0, domain.getLowerBound());
        assertEquals(I2, domain.getUpperBound());
        assertEquals(Stream.of(I0, I1, I2).collect(Collectors.toSet()), domain.getValues());
    }

    @Test
    public void testUpdate() {
        final ColumnarCombinedDomainCalculator<IntReadData> calc = createCalculator(60);
        final DataColumnDomainCreator creator = new DataColumnDomainCreator();
        final Set<IntCell> set = Stream.of(I0, I1, I2).collect(Collectors.toSet());
        creator.setLowerBound(I0);
        creator.setUpperBound(I2);
        creator.setValues(set);
        calc.update(creator.createDomain());
        final DataColumnDomain domain = calc.createDomain();
        assertEquals(I0, domain.getLowerBound());
        assertEquals(I2, domain.getUpperBound());
        assertEquals(Stream.of(I0, I1, I2).collect(Collectors.toSet()), domain.getValues());
    }

}
