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
 *   Oct 13, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.parallel.exec;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

final class ColumnWriteTask implements Runnable {

    private final int m_colIdx;

    private final RowWriteTask m_task;

    private final DataWriter m_writer;

    private int m_readIdx;

    private final ExecutorService m_executor;

    private final CountDownLatch m_doneLatch;

    ColumnWriteTask(final RowWriteTask task, final int colIdx, final DataWriter writer,
        final ExecutorService executor, final CountDownLatch doneLatch) {
        m_task = task;
        m_colIdx = colIdx;
        m_writer = writer;
        m_executor = executor;
        m_doneLatch = doneLatch;
    }

    @Override
    public void run() {
        for (; m_readIdx < m_task.size(); m_readIdx++) { //NOSONAR
            var readData = m_task.getBatch(m_readIdx).get(m_colIdx);
            if (!m_writer.accept(readData, m_task.getBatchIndex(m_readIdx))) {
                m_executor.submit(this);
                return;
            }
        }
        m_doneLatch.countDown();
    }

}