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
 *  aBoolean with this program; if not, see <http://www.gnu.org/licenses>.
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
 *   8 Feb 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.knime.core.columnar.access.ColumnarListAccessFactory.ColumnarListReadAccess;
import org.knime.core.columnar.access.ColumnarListAccessFactory.ColumnarListWriteAccess;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.testing.data.TestListData;
import org.knime.core.columnar.testing.data.TestListData.TestListDataFactory;
import org.knime.core.columnar.testing.data.TestStringData.TestStringDataFactory;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.access.StringAccess.StringWriteAccess;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.StringDataSpec;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@RunWith(Parameterized.class)
@SuppressWarnings("javadoc")
public class ColumnarListAccessFactoryTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays
            .asList(new Object[][]{{Collections.emptyList()}, {Arrays.asList("test")}, {Arrays.asList("", "")}}); // NOSONAR
    }

    private List<String> m_values;

    public ColumnarListAccessFactoryTest(final List<String> values) {
        m_values = values;
    }

    @Test
    public void testAccesses() {

        final ListDataSpec spec = new ListDataSpec(StringDataSpec.INSTANCE);
        @SuppressWarnings("unchecked")
        final ColumnarListAccessFactory<StringReadData, StringWriteData> accessFactory =
            (ColumnarListAccessFactory<StringReadData, StringWriteData>)ColumnarAccessFactoryMapper.INSTANCE
                .visit(spec);
        final TestListDataFactory dataFactory = new TestListDataFactory(TestStringDataFactory.INSTANCE);

        final TestListData listData = dataFactory.createWriteData(1);
        final ColumnarListWriteAccess<StringWriteData> listWriteAccess = accessFactory.createWriteAccess(() -> 0);
        listWriteAccess.setData(listData);
        final ColumnarListReadAccess<StringReadData> listReadAccess = accessFactory.createReadAccess(() -> 0);
        listReadAccess.setData(listData);

        assertTrue(listReadAccess.isMissing());
        listWriteAccess.setMissing();
        assertTrue(listReadAccess.isMissing());

        listWriteAccess.create(m_values.size());
        assertFalse(listReadAccess.isMissing());
        assertEquals(m_values.size(), listReadAccess.size());

        for (int i = 0; i < m_values.size(); i++) {
            listWriteAccess.setWriteIndex(i);
            final StringWriteAccess writeValue = listWriteAccess.getWriteAccess();
            assertEquals(writeValue, listWriteAccess.getWriteAccess());
            listReadAccess.setIndex(i);
            final StringReadAccess readValue = listReadAccess.getAccess();
            assertEquals(readValue, listReadAccess.getAccess());

            assertTrue(listReadAccess.isMissing(i));
            assertNull(readValue.getStringValue());

            writeValue.setStringValue(m_values.get(i));
            assertFalse(listReadAccess.isMissing(i));
            assertEquals(m_values.get(i), readValue.getStringValue());

            writeValue.setStringValue(null);
            assertTrue(listReadAccess.isMissing(i));
            assertNull(readValue.getStringValue());
        }

        listWriteAccess.setMissing();
        assertTrue(listReadAccess.isMissing());

    }

}
