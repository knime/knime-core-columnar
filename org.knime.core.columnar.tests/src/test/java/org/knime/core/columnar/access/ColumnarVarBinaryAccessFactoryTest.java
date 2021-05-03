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
 *  aInt with this program; if not, see <http://www.gnu.org/licenses>.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.knime.core.columnar.access.ColumnarVarBinaryAccessFactory.ColumnarVarBinaryReadAccess;
import org.knime.core.columnar.access.ColumnarVarBinaryAccessFactory.ColumnarVarBinaryWriteAccess;
import org.knime.core.columnar.testing.data.TestVarBinaryData;
import org.knime.core.columnar.testing.data.TestVarBinaryData.TestVarBinaryDataFactory;
import org.knime.core.table.schema.VarBinaryDataSpec;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@RunWith(Parameterized.class)
@SuppressWarnings("javadoc")
public class ColumnarVarBinaryAccessFactoryTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{new byte[0]}, // NOSONAR
            {new byte[]{Byte.MIN_VALUE, (byte)-1, (byte)0, Byte.MAX_VALUE}}});
    }

    private byte[] m_value;

    public ColumnarVarBinaryAccessFactoryTest(final byte[] value) {
        m_value = value;
    }

    @Test
    public void testAccesses() {

        final VarBinaryDataSpec spec = VarBinaryDataSpec.INSTANCE;
        final ColumnarVarBinaryAccessFactory factory =
            (ColumnarVarBinaryAccessFactory)ColumnarAccessFactoryMapper.INSTANCE.visit(spec);
        final TestVarBinaryData data = TestVarBinaryDataFactory.INSTANCE.createWriteData(1);
        final ColumnarVarBinaryWriteAccess writeAccess = factory.createWriteAccess(() -> 0);
        writeAccess.setData(data);
        final ColumnarVarBinaryReadAccess readAccess = factory.createReadAccess(() -> 0);
        readAccess.setData(data);

        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getByteArray());

        writeAccess.setByteArray(m_value);
        assertFalse(readAccess.isMissing());
        assertArrayEquals(m_value, readAccess.getByteArray());

        if (m_value.length > 2) {
            writeAccess.setByteArray(m_value, 1, m_value.length - 2);
            final byte[] subArray = new byte[m_value.length - 2];
            System.arraycopy(m_value, 1, subArray, 0, m_value.length - 2);
            assertFalse(readAccess.isMissing());
            assertArrayEquals(subArray, readAccess.getByteArray());
        }

        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getByteArray());

    }

}
