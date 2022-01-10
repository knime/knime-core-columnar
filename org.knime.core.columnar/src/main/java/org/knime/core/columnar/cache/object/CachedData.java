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
 *   Dec 17, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cache.object;

import java.io.Flushable;
import java.util.concurrent.CompletableFuture;

import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;

/**
 * Contains the interfaces used by the {@link ObjectCache}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class CachedData {

    CachedData() {
    }

    /**
     * Factory that handles object caching for the ObjectCache on the (column) data level.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    interface CachedDataFactory {

        /**
         * Creates either a {@link CachedWriteData} if the column is cached or returns {@link NullableWriteData data} if
         * it isn't.
         *
         * @param data to potentially decorate with a cache
         * @param id of data
         * @return either a {@link CachedWriteData} decorating data or data itself
         */
        NullableWriteData createWriteData(final NullableWriteData data, ColumnDataUniqueId id);

        /**
         * Creates a {@link CompletableFuture} that represents the serialization process of the object data in
         * {@link NullableReadData data}.
         *
         * @param data to get a future for
         * @return the future representing the serialization process of data
         */
        CompletableFuture<NullableReadData> getCachedDataFuture(final NullableReadData data);

        /**
         * Creates either a {@link CachedLoadingReadData} if the column is cached or returns {@link NullableReadData
         * data} if it isn't.
         *
         * @param data to potentially decorate with a cache
         * @param id of data
         * @return either a {@link CachedLoadingReadData} decorating data or data itself
         */
        NullableReadData createReadData(final NullableReadData data, final ColumnDataUniqueId id);
    }

    /**
     * Data that either contains other (nested) CachedWriteData objects or itself caches object data.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    interface CachedWriteData extends NullableWriteData, Flushable {

        /**
         * Cancels any ongoing write operations.
         */
        void cancel();

        /**
         * Called after the underlying {@link NullableWriteData} has been expanded, so that the cache can be adjusted to
         * the new capacity.
         */
        void expandCache();

        /**
         * Locks writing for any other thread. Used to synchronize {@link #expand(int)} and any write operations.
         */
        void lockWriting();

        /**
         * Unlocks writing. Must be called only after a {@link #lockWriting()} has been called by the same thread
         * before. Used to synchronize {@link #expand(int)} and any write operations.
         */
        void unlockWriting();

        /**
         * Waits for the currently outstanding processing.
         */
        void waitForAndGetFuture();

        @Override
        void flush();

        @Override
        CachedReadData close(int length);

    }

    /**
     * {@link NullableReadData} created by a CachedWriteData that is potentially still serializing asynchronously.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    interface CachedReadData extends NullableReadData {

        /**
         * Called for column-level data object i.e. data objects that are not a child of a struct or list
         *
         * @return the read delegate
         */
        NullableReadData closeWriteDelegate();

        /**
         * @return the {@link CachedWriteData} that produced this instance
         */
        CachedWriteData getWriteData();

        /**
         * Called for data objects that are not column-level i.e. that are a child of a struct or list
         *
         * @param readDelegate the read delegate for this CachedReadData
         */
        void setReadDelegate(NullableReadData readDelegate);

    }

    /**
     * CachedReadData that caches actual Java objects.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    interface CachedValueReadData extends CachedReadData {

        /**
         * @return the Objects held by this CachedReadData.
         */
        Object[] getData();

    }

    /**
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    interface CachedLoadingReadData extends NullableReadData {

        /**
         * Similar to {@link #retain()}, just for the cached data.
         */
        void retainCache();

        /**
         * Similar to {@link #release()}, just for the cached data.
         */
        void releaseCache();
    }

}
