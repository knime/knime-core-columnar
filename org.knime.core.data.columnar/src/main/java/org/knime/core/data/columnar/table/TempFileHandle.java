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
 *   Nov 30, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.knime.core.columnar.store.FileHandle;
import org.knime.core.columnar.store.FileHandleUtils;
import org.knime.core.data.container.DataContainer;
import org.knime.core.node.workflow.NodeContext;

/**
 * {@link FileHandle} that creates its file lazily when it is first needed.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class TempFileHandle implements FileHandle {

    private File m_file = null;

    private Path m_path = null;

    private final NodeContext m_context = NodeContext.getContext();

    @Override
    public synchronized File asFile() {
        init();
        return m_file;
    }

    @Override
    public synchronized void delete() {
        if (m_path != null) {
            FileHandleUtils.deleteAndRetry(m_path, ColumnarRowWriteTable.LOGGER::debug,
                ColumnarRowWriteTable.LOGGER::error);
        }
    }

    private synchronized void init() {
        if (m_file == null) {
            NodeContext.pushContext(m_context);
            try {
                m_file = DataContainer.createTempFile(".knable");
                m_path = m_file.toPath();
            } catch (IOException ex) {
                throw new IllegalStateException(ex);
            } finally {
                NodeContext.removeLastContext();
            }
        }
    }

    @Override
    public Path asPath() {
        init();
        return m_path;
    }

}