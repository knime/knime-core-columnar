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
 */
package org.knime.core.columnar.store;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.filter.ColumnSelection;

/**
 * A data structure for storing and obtaining columnar data. Data can be written to and read from the store. The life
 * cycle of a store is as follows:
 * <ol>
 * <li>The singleton {@link BatchFactory} is obtained via {@link #getFactory()}.</li>
 * <li>The singleton {@link BatchWriter} is obtained via {@link #getWriter()}.</li>
 * <li>Data is created by iterating over the following steps:</li>
 * <ol>
 * <li>A new {@link WriteBatch} is {@link BatchFactory#create(int) created} using the factory.</li>
 * <li>Data is {@link WriteBatch#get(int) written} into the batch and {@link WriteBatch#close(int) closed}, which
 * creates a {@link ReadBatch}.</li>
 * <li>This batch is {@link BatchWriter#write(ReadBatch) written} into the store using the writer.</li>
 * </ol>
 * <li>The writer is {@link BatchWriter#close() closed}.</li>
 * <li>Any number of {@link BatchReader readers} are {@link #createReader(ColumnSelection) created}.</li>
 * <li>Data is read independently by each of these readers by iterating over the following steps:
 * <ol>
 * <li>A {@link ReadBatch} is {@link BatchReader#readRetained(int) obtained}.</li>
 * <li>Data is {@link ReadBatch#get(int) read} from the batch.</li>
 * </ol>
 * <li>The readers are {@link BatchReader#close() closed}.</li>
 * <li>The store itself is {@link #close() closed}, upon which any underlying resources are relinquished.</li>
 * </ol>
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public interface ColumnStore extends ColumnWriteStore, ColumnReadStore {
}
