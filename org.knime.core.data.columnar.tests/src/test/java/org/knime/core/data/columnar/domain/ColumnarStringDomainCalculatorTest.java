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

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.testing.data.TestStringData.TestStringDataFactory;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.DataType;
import org.knime.core.data.def.StringCell;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnarStringDomainCalculatorTest {

    private static final DataCell MISSING_CELL = DataType.getMissingCell();

    private static final StringCell S1 = new StringCell("1");

    private static final StringCell S2 = new StringCell("2");

    @Test
    public void testUpdateWithData() {
        testUpdateWithDataInternal(new ColumnarStringDomainCalculator(1));
    }

    static void
        testUpdateWithDataInternal(final ColumnarDomainCalculator<StringReadData, DataColumnDomain> calc) {
        calc.update(TestStringDataFactory.INSTANCE.createReadData(new String[]{null}));
        assertEquals(Collections.emptySet(), calc.createDomain().getValues());

        final String[] arr1 = new String[]{"1"};
        final Set<DataCell> set1 = Stream.of(S1).collect(Collectors.toSet());
        calc.update(TestStringDataFactory.INSTANCE.createReadData(arr1));
        assertEquals(set1, calc.createDomain().getValues());

        calc.update(TestStringDataFactory.INSTANCE.createReadData(arr1));
        assertEquals(set1, calc.createDomain().getValues());

        calc.update(TestStringDataFactory.INSTANCE.createReadData(new String[]{"2"}));
        assertNull(calc.createDomain().getValues());

        calc.update(TestStringDataFactory.INSTANCE.createReadData(arr1));
        assertNull(calc.createDomain().getValues());
    }

    @Test
    public void testUpdateVoid() {
        testUpdateVoidInternal(new ColumnarStringDomainCalculator(60));
    }

    static void testUpdateVoidInternal(final ColumnarDomainCalculator<StringReadData, DataColumnDomain> calc) {
        calc.update(new DataColumnDomainCreator().createDomain());
        assertEquals(Collections.emptySet(), calc.createDomain().getValues());
    }

    @Test
    public void testUpdate() {
        testUpdateInternal(new ColumnarStringDomainCalculator(1));
    }

    static void testUpdateInternal(final ColumnarDomainCalculator<StringReadData, DataColumnDomain> calc) {
        final DataColumnDomainCreator creator = new DataColumnDomainCreator();

        creator.setValues(Stream.of(MISSING_CELL).collect(Collectors.toSet()));
        calc.update(creator.createDomain());
        assertEquals(Collections.emptySet(), calc.createDomain().getValues());

        final Set<StringCell> set1 = Stream.of(S1).collect(Collectors.toSet());
        creator.setValues(set1);
        calc.update(creator.createDomain());
        assertEquals(set1, calc.createDomain().getValues());

        creator.setValues(set1);
        calc.update(creator.createDomain());
        assertEquals(set1, calc.createDomain().getValues());

        creator.setValues(Stream.of(S2).collect(Collectors.toSet()));
        calc.update(creator.createDomain());
        assertNull(calc.createDomain().getValues());

        creator.setValues(set1);
        calc.update(creator.createDomain());
        assertNull(calc.createDomain().getValues());
    }

}
