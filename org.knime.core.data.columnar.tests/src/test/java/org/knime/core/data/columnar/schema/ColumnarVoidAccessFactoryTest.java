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
 *  aVoid with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME Voideroperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the Voiderpretation of the
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
 *  NodeDialog, and NodeView) and that only Voideroperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for Voideroperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for Voideroperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   8 Feb 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.knime.core.columnar.data.DataSpec;
import org.knime.core.columnar.testing.data.TestVoidData;
import org.knime.core.columnar.testing.data.TestVoidData.TestVoidDataFactory;
import org.knime.core.data.columnar.schema.ColumnarVoidAccessFactory.VoidReadAccess;
import org.knime.core.data.columnar.schema.ColumnarVoidAccessFactory.VoidWriteAccess;
import org.knime.core.data.v2.access.VoidAccess.VoidAccessSpec;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnarVoidAccessFactoryTest {

    @Test
    public void testAccesses() {

        final VoidAccessSpec spec = VoidAccessSpec.INSTANCE;
        final ColumnarVoidAccessFactory factory =
            (ColumnarVoidAccessFactory)ColumnarAccessFactoryMapper.INSTANCE.visit(spec);
        assertEquals(DataSpec.voidSpec(), factory.getColumnDataSpec());
        final TestVoidData data = TestVoidDataFactory.INSTANCE.createWriteData(1);
        final VoidWriteAccess writeAccess = factory.createWriteAccess(data, () -> 0);
        final VoidReadAccess readAccess = factory.createReadAccess(data, () -> 0);

        // set cell
        assertTrue(readAccess.isMissing());
        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());

    }

}
