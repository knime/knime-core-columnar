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
 *   Apr 21, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cache.batch;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.knime.core.columnar.batch.ReadBatch;

/**
 * Helper cache that does not retain batches itself and only holds weak references to them.
 * The purpose of this cache is to lessen the load on the shared cache which does all the book keeping.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class LocalReadBatchCache {

    private final AtomicReferenceArray<WeakReference<ReadBatch>> m_localCache;

    private final IndexBatchLoader m_loader;

    LocalReadBatchCache(final IndexBatchLoader loader, final int numBatches) {
        if (numBatches == 0) {
            throw new IllegalArgumentException("No batches to read.");
        }
        m_loader = loader;
        m_localCache = new AtomicReferenceArray<>(numBatches);
    }

    public interface IndexBatchLoader {
        ReadBatch load(int batchIndex) throws IOException;
    }

    ReadBatch readRetained(final int index) throws IOException {
        var locallyCached = m_localCache.get(index);
        if (locallyCached != null) {
            var batch = locallyCached.get();
            if (batch != null && batch.tryRetain()) {
                return batch;
            }
        }
        // we accept that multiple threads might load the same batch to avoid synchronization costs
        // since this batch doesn't retain the batches, it is fine if the WeakReferences for the same index
        // are overwritten in such a scenario
        var batch = m_loader.load(index);
        m_localCache.set(index, new WeakReference<>(batch));
        return batch;
    }

}
