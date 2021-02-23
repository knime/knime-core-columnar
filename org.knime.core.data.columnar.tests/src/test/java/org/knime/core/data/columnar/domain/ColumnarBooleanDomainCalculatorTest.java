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
import static org.knime.core.data.def.BooleanCell.FALSE;
import static org.knime.core.data.def.BooleanCell.TRUE;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.knime.core.columnar.testing.data.TestBooleanData.TestBooleanDataFactory;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.DataType;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnarBooleanDomainCalculatorTest {

    private static final DataCell MISSING_CELL = DataType.getMissingCell();

    @Test
    public void testUpdateWithData() {
        final ColumnarBooleanDomainCalculator calc = new ColumnarBooleanDomainCalculator();

        calc.update(TestBooleanDataFactory.INSTANCE.createReadData(new Boolean[]{null}));
        assertEquals(Collections.emptySet(), calc.createDomain().getValues());

        calc.update(TestBooleanDataFactory.INSTANCE.createReadData(new Boolean[]{false, false}));
        assertEquals(Stream.of(FALSE).collect(Collectors.toSet()), calc.createDomain().getValues());

        final Set<DataCell> falseTrue = Stream.of(FALSE, TRUE).collect(Collectors.toSet());
        calc.update(TestBooleanDataFactory.INSTANCE.createReadData(new Boolean[]{true, false}));
        assertEquals(falseTrue, calc.createDomain().getValues());

        calc.update(TestBooleanDataFactory.INSTANCE.createReadData(new Boolean[]{true}));
        assertEquals(falseTrue, calc.createDomain().getValues());
    }

    @Test
    public void testUpdateVoid() {
        final ColumnarBooleanDomainCalculator calc = new ColumnarBooleanDomainCalculator();
        calc.update(new DataColumnDomainCreator().createDomain());
        assertEquals(Collections.emptySet(), calc.createDomain().getValues());
    }

    @Test
    public void testUpdate() {
        final ColumnarBooleanDomainCalculator calc = new ColumnarBooleanDomainCalculator();
        final DataColumnDomainCreator creator = new DataColumnDomainCreator();

        creator.setValues(Stream.of(MISSING_CELL).collect(Collectors.toSet()));
        calc.update(creator.createDomain());
        assertEquals(Collections.emptySet(), calc.createDomain().getValues());

        creator.setValues(Stream.of(FALSE, FALSE).collect(Collectors.toSet()));
        calc.update(creator.createDomain());
        assertEquals(Stream.of(FALSE).collect(Collectors.toSet()), calc.createDomain().getValues());

        final Set<DataCell> falseTrue = Stream.of(FALSE, TRUE).collect(Collectors.toSet());
        creator.setValues(Stream.of(TRUE, FALSE).collect(Collectors.toSet()));
        calc.update(creator.createDomain());
        assertEquals(falseTrue, calc.createDomain().getValues());

        creator.setValues(Stream.of(TRUE).collect(Collectors.toSet()));
        calc.update(creator.createDomain());
        assertEquals(falseTrue, calc.createDomain().getValues());
    }

}
