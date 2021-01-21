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

import java.io.Closeable;
import java.io.IOException;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.filter.DefaultColumnSelection;

/**
 * A data structure for obtaining columnar data. Data can be read from the store. The life cycle of a store is as
 * follows:
 * <ol>
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
public interface ColumnReadStore extends Closeable {

    /**
     * Creates a new {@link BatchReader} that reads {@link ReadBatch ReadBatches}, in which columns are guaranteed to be
     * materialized according to a given {@link ColumnSelection}.
     *
     * @param selection a selection of columns that determines which columns are guaranteed to be materialized in
     *            batches read by the reader
     * @return a new reader
     */
    BatchReader createReader(ColumnSelection selection);

    /**
     * Creates a new {@link BatchReader} that reads {@link ReadBatch ReadBatches} in which all columns are materialized.
     *
     * @return a new batch reader
     */
    default BatchReader createReader() {
        return createReader(new DefaultColumnSelection(getSchema().numColumns()));
    }

    /**
     * Obtains the {@link ColumnStoreSchema} of this store.
     *
     * @return the schema of this store
     */
    ColumnStoreSchema getSchema();

    @Override
    void close() throws IOException;

}
