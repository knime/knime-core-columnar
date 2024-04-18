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
package org.knime.core.data.columnar.table;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.v2.RowContainer;

/**
 * Settings for columnar {@link RowContainer RowContainers}.
 * <P>
 * Renamed from {@code ColumnarRowContainerSettings} which has existed since 4.3.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 * @since 4.5.
 */
public final class ColumnarRowWriteTableSettings {

    /** a system property to disable all caches in a write table for testing */
    private static final boolean DISABLE_CACHES = Boolean.getBoolean("knime.columnar.disablecaches.writetable");

    /** a system property to enforce using the heap badger as it is not the default yet */
    private static final boolean USE_HEAP_BADGER = Boolean.getBoolean("knime.columnar.heapbadger.enable");

    private final boolean m_initializeDomains;

    private final boolean m_calculateDomains;

    private final int m_maxPossibleNominalDomainValues;

    private final boolean m_checkDuplicateRowKeys;

    private final boolean m_forceSynchronousIO;

    private final boolean m_useCaching;

    /**
     * The amount of rows to be processed by a single thread when not forced to handle rows sequentially.
     */
    private final int m_rowBatchSize;

    /**
     * The maximum number of batches to queue for processing before adding further rows is blocked. Together with
     * {@link #m_rowBatchSize}, this determines the maximum number of rows that are held by a container before they are
     * processed.
     */
    private final int m_maxPendingBatches;

    /**
     * @param initializeDomains if {@code true}, domains will be initialized via domain values provided through an
     *            incoming {@link DataTableSpec}.
     * @param maxPossibleNominalDomainValues maximum number of values for nominal domains.
     * @param checkDuplicateRowKeys whether to check for duplicates among row keys.
     * @param forceSynchronousIO whether writing a new row to the table should block until all previously written rows
     *            have been persisted.
     * @param rowBatchSize number of rows to be processed by a single thread when not forced to handle rows
     *            sequentially.
     * @param maxPendingBatches maximum number of batches to queue for processing before adding further rows is blocked.
     */
    public ColumnarRowWriteTableSettings(final boolean initializeDomains, final int maxPossibleNominalDomainValues,
        final boolean checkDuplicateRowKeys, final boolean forceSynchronousIO, final int rowBatchSize,
        final int maxPendingBatches) {
        this(initializeDomains, true, maxPossibleNominalDomainValues, checkDuplicateRowKeys, true, forceSynchronousIO,
            rowBatchSize, maxPendingBatches);
    }

    /**
     * @param initializeDomains if {@code true}, domains will be initialized via domain values provided through an
     *            incoming {@link DataTableSpec}.
     * @param calculateDomains whether to perform domain calculation based on the data written to the table. It can be
     *            useful to set this option to {@code false} while setting {@code initializeDomains} to {@code true} if
     *            it is known that the initial domains will also be the final domains of the table. Note that it is not
     *            allowed to set both {@code initializeDomains} and {@code calculateDomains} to {@code false}.
     * @param maxPossibleNominalDomainValues maximum number of values for nominal domains. Ignored if
     *            {@code calculateDomains} is {@code false}.
     * @param checkDuplicateRowKeys whether to check for duplicates among row keys.
     * @param useCaching whether data written to the table should be cached in memory. It can be useful to set this
     *            option to {@code false} if e.g. it is known that the created table will exclusively be used for
     *            (disk-based) interprocess communication where in-memory caching does not provide any benefits.
     * @param forceSynchronousIO whether writing a new row to the table should block until all previously written rows
     *            have been persisted.
     * @param rowBatchSize number of rows to be processed by a single thread when not forced to handle rows
     *            sequentially.
     * @param maxPendingBatches maximum number of batches to queue for processing before adding further rows is blocked.
     * @throws IllegalArgumentException When both {@code initializeDomains} and {@code calculateDomains} are
     *             {@code false}.
     */
    public ColumnarRowWriteTableSettings(final boolean initializeDomains, final boolean calculateDomains,
        final int maxPossibleNominalDomainValues, final boolean checkDuplicateRowKeys, final boolean useCaching,
        final boolean forceSynchronousIO, final int rowBatchSize, final int maxPendingBatches) {
        if (!initializeDomains && !calculateDomains) {
            throw new IllegalArgumentException("initializeDomains and calculateDomains cannot both be false.");
        }
        m_initializeDomains = initializeDomains;
        m_calculateDomains = calculateDomains;
        m_maxPossibleNominalDomainValues = maxPossibleNominalDomainValues;
        m_checkDuplicateRowKeys = checkDuplicateRowKeys;
        m_useCaching = useCaching;
        m_forceSynchronousIO = forceSynchronousIO;
        m_rowBatchSize = rowBatchSize;
        m_maxPendingBatches = maxPendingBatches;
    }

    boolean isInitializeDomains() {
        return m_initializeDomains;
    }

    boolean isCalculateDomains() {
        return m_calculateDomains;
    }

    int getMaxPossibleNominalDomainValues() {
        return m_maxPossibleNominalDomainValues;
    }

    boolean isCheckDuplicateRowKeys() {
        return m_checkDuplicateRowKeys;
    }

    boolean isUseCaching() {
        return !DISABLE_CACHES && m_useCaching;
    }

    boolean isForceSynchronousIO() {
        return m_forceSynchronousIO;
    }

    static boolean useHeapBadger() {
        return USE_HEAP_BADGER;
    }

    /**
     * Returns the amount of rows to be processed by a single thread when not forced to handle rows sequentially.
     *
     * @return the row batch size
     */
    public int getRowBatchSize() {
        return m_rowBatchSize;
    }

    /**
     * Returns the maximum number of batches to queue for processing before adding further rows is blocked. Together
     * with the {@link #getRowBatchSize() row batch size}, this determines the maximum number of rows that are held by a
     * container before they are processed.
     *
     * @return the maximum number of queued batches before blocking
     */
    public int getMaxPendingBatches() {
        return m_maxPendingBatches;
    }

    /**
     * Creates a builder for {@link ColumnarRowWriteTableSettings}.
     *
     * @return new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for {@link ColumnarRowWriteTableSettings}.
     */
    public static final class Builder {

        private boolean m_initializeDomains;
        private boolean m_calculateDomains = true;
        private int m_maxPossibleNominalDomainValues;
        private boolean m_checkDuplicateRowKeys;
        private boolean m_forceSynchronousIO;
        private boolean m_useCaching = true;
        private int m_rowBatchSize;
        private int m_maxPendingBatches;

        private Builder() {
        }

        public Builder initializingDomains(final boolean initializeDomains) {
            m_initializeDomains = initializeDomains;
            return this;
        }

        public Builder calculatingDomains(final boolean calculateDomains) {
            m_calculateDomains = calculateDomains;
            return this;
        }

        public Builder withMaxPossibleNominalDomainValues(final int maxPossibleNominalDomainValues) {
            m_maxPossibleNominalDomainValues = maxPossibleNominalDomainValues;
            return this;
        }

        public Builder checkingDuplicateRowKeys(final boolean checkDuplicateRowKeys) {
            m_checkDuplicateRowKeys = checkDuplicateRowKeys;
            return this;
        }

        public Builder forcingSynchronousIO(final boolean forceSynchronousIO) {
            m_forceSynchronousIO = forceSynchronousIO;
            return this;
        }

        public Builder usingCaching(final boolean useCaching) {
            m_useCaching = useCaching;
            return this;
        }

        public Builder withRowBatchSize(final int rowBatchSize) {
            m_rowBatchSize = rowBatchSize;
            return this;
        }

        public Builder withMaxPendingBatches(final int maxPendingBatches) {
            m_maxPendingBatches = maxPendingBatches;
            return this;
        }

        public ColumnarRowWriteTableSettings build() {
            return new ColumnarRowWriteTableSettings(m_initializeDomains, m_calculateDomains,
                m_maxPossibleNominalDomainValues, m_checkDuplicateRowKeys, m_forceSynchronousIO, m_useCaching,
                m_rowBatchSize, m_maxPendingBatches);
        }
    }
}
