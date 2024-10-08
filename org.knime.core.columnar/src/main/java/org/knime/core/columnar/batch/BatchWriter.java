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
package org.knime.core.columnar.batch;

import java.io.Closeable;
import java.io.IOException;

import org.knime.core.columnar.WriteData;

/**
 * A writer that is associated with a {@link BatchWritable} and that can be used to (i) create {@link WriteBatch
 * WriteBatches} with a certain minimum {@link WriteData#capacity() capacity} and (ii) write {@link ReadBatch
 * ReadBatches}, i.e., {@link WriteBatch#close(int) closed} WriteBatches.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public interface BatchWriter extends Closeable {

    /**
     * Create a new {@link WriteBatch} with a certain minimum {@link WriteData#capacity() capacity}.
     *
     * @param capacity the minimum capacity of data in the created batch
     * @return a new batch that can be populated with data
     */
    WriteBatch create(int capacity);

    /**
     * Write a {@link ReadBatch}, i.e., a {@link WriteBatch#close(int) closed} {@link WriteBatch WriteBatches}.
     *
     * @param batch a batch to which data can no longer be written
     * @throws IOException if any I/O problem occurs
     */
    void write(ReadBatch batch) throws IOException;

    /**
     * @return How many bytes will be allocated per element. The prefix initial indicates that this size does not
     *         necessarily reflect the final size, as string and binary blobs can have variable lengths.
     */
    int initialNumBytesPerElement();
}
