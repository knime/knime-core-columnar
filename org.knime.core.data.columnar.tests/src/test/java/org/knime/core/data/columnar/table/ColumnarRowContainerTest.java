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
 *   29 Jan 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar.table;

import static org.junit.Assert.assertEquals;
import static org.knime.core.data.columnar.table.ColumnarTableTestUtils.createColumnarRowContainer;

import java.io.IOException;

import org.junit.Test;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.node.ExtensionTable;
import org.knime.core.util.DuplicateKeyException;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnarRowContainerTest extends ColumnarTest {

    private static final int CAPACITY = ColumnarRowWriteCursor.CAPACITY_MAX_DEF;

    @Test
    public void testClose() {
        try (final ColumnarRowContainer container = createColumnarRowContainer()) { // NOSONAR
        }
    }

    @Test
    public void testFinishInternalSingleton() {
        try (final ColumnarRowContainer container = createColumnarRowContainer();
                final ExtensionTable table1 = container.finishInternal();
                final ExtensionTable table2 = container.finishInternal()) {
            assertEquals(table1, table2);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testNPEOnFinish() throws IOException {
        try (final ColumnarRowContainer container = createColumnarRowContainer()) {
            container.finish();
        }
    }

    @Test(expected = DuplicateKeyException.class, timeout = 500)
    public void testDuplicateKeyExceptionOnClose() {
        try (final ColumnarRowContainer container = createColumnarRowContainer();
                final ColumnarRowWriteCursor cursor = container.createCursor()) {
            for (int i = 0; i < CAPACITY; i++) {
                cursor.forward().setRowKey("1");
            }
            cursor.forward().setRowKey("1");
            // since heap badger we need to flush before close to run into the duplicate key error.
            // But that would've made sense before too
            cursor.finish();
        }
    }

}
