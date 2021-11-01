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
 *   Nov 1, 2021 (marcel): created
 */
package org.knime.core.data.columnar.table;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

/**
 * Only tests functionality not yet covered (indirectly) by {@link ColumnarContainerTableTest} or
 * {@link ColumnarRowCursorTest}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
@RunWith(MockitoJUnitRunner.class)
public final class ColumnarRowReadTableTest {

    @Mock
    private ColumnarBatchReadStore m_store;

    @Test
    public void testClosesStore() throws IOException {
        final AtomicBoolean isStoreClosed = new AtomicBoolean();
        doAnswer(new Answer<Void>() {

            @Override
            public Void answer(final InvocationOnMock invocation) throws Throwable {
                isStoreClosed.set(true);
                return null;
            }
        }).when(m_store).close();

        try (final var table = new ColumnarRowReadTable(null, null, m_store, -1)) {
            // Nothing to do. We just wan't to confirm that closing the table also closes the underlying store.
        }
        assertTrue(isStoreClosed.get());
    }
}
