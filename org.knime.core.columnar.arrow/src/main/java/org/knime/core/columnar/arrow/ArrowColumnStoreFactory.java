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
package org.knime.core.columnar.arrow;

import java.nio.file.Path;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationOutcome;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.presets.javacpp;
import org.bytedeco.lz4.global.lz4;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils.OffsetProvider;
import org.knime.core.columnar.arrow.compress.ArrowCompression;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.columnar.arrow.extensiontypes.ExtensionTypes;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.memory.ColumnarOffHeapMemoryAlertSystem;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.columnar.store.ColumnStoreFactoryCreator;
import org.knime.core.columnar.store.FileHandle;
import org.knime.core.table.schema.ColumnarSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ColumnStoreFactory} implementation for Arrow.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class ArrowColumnStoreFactory implements ColumnStoreFactory {

    private static final double MEMORY_ALERT_THRESHOLD = 0.9;

    private static final Logger LOGGER = LoggerFactory.getLogger(ArrowColumnStoreFactory.class);

    static {
        // AP-18253: Load LZ4 libs on startup to avoid concurrency issues when different threads try to load it
        // concurrently
        System.setProperty("org.bytedeco.javacpp.maxPhysicalBytes", "0");
        try {
            // NB: Loader.load(Class...) catches exceptions but Loader.load(Class) does not
            Loader.load(javacpp.class);
            Loader.load(lz4.class);
        } catch (final Throwable th) { // NOSONAR
            // AP-19695: If we cannot load the native libraries Loader#load(Class) will throw an UnsatisfiedLinkError.
            // We only catch this error to add a useful error message to the KNIME log. Afterwards we re-throw it
            // because the Arrow backend cannot be used without the lz4 libraries.
            LOGGER.error("Failed to initialize LZ4 libraries. The Columnar Table Backend won't work properly.", th);
            throw th;
        }

        // Register the extension types with arrow
        ExtensionTypes.registerExtensionTypes();
    }

    private final ArrowCompression m_compression;

    private final BufferAllocator m_allocator;

    /**
     * <b>Constructor for unit tests.</b> Create a {@link ColumnStoreFactory} for Arrow with the given compression for
     * writing.
     *
     * @param allocator the allocator to use. Must be closed by the caller after it is done using the store
     * @param compression the compression to use for writing
     * @apiNote only use this constructor in unit tests
     */
    public ArrowColumnStoreFactory(final BufferAllocator allocator, final ArrowCompression compression) {
        m_compression = compression;
        m_allocator = allocator;
    }

    private ArrowColumnStoreFactory(final long memoryLimit) {
        m_allocator = new RootAllocator(new AllocationListener() {

            @Override
            public void onAllocation(final long size) {
                if (m_allocator.getAllocatedMemory() > memoryLimit * MEMORY_ALERT_THRESHOLD) {
                    LOGGER.debug("Memory over {}%. Calling memory alert.", (int)(100 * MEMORY_ALERT_THRESHOLD));
                    memoryAlert();
                }
            }

            @Override
            public boolean onFailedAllocation(final long size, final AllocationOutcome outcome) {
                LOGGER.debug("Allocation of {} bytes failed. Calling memory alert.", size);
                return memoryAlert();
            }

        }, memoryLimit);
        m_compression = ArrowCompressionUtil.getDefaultCompression();
    }

    @Override
    @SuppressWarnings("resource") // Allocator closed by store
    public ArrowBatchStore createStore(final ColumnarSchema schema, final FileHandle fileSupplier) {
        return new ArrowBatchStore(schema, fileSupplier, m_compression, newChildAllocator("ArrowColumnStore"));
    }

    @Override
    @SuppressWarnings("resource") // Allocator closed by store
    public ArrowBatchReadStore createReadStore(final Path path) {
        return new ArrowBatchReadStore(path, newChildAllocator("ArrowColumnReadStore"));
    }

    /**
     * Create a new {@link RandomAccessBatchReadable}, reading data from the provided Arrow IPC file that has not been
     * written completely. The offsets to the record batches and dictionary batches in the file must be provided via a
     * {@link OffsetProvider}.
     *
     * @param path from which data is read
     * @param offsetProvider a provider for the offsets of the record batches and dictionary batches in the Arrow IPC
     *            file
     * @return a newly created readable
     */
    @SuppressWarnings("resource") // Allocator closed by store
    public ArrowPartialFileBatchReadable createPartialFileReadable(final Path path,
        final OffsetProvider offsetProvider) {
        return new ArrowPartialFileBatchReadable(path, offsetProvider,
            newChildAllocator("ArrowPartialFileBatchReadStore"));
    }

    private BufferAllocator newChildAllocator(final String name) {
        return m_allocator.newChildAllocator(name, 0, m_allocator.getLimit());
    }

    private static boolean memoryAlert() {
        try {
            return ColumnarOffHeapMemoryAlertSystem.INSTANCE.sendMemoryAlert();
        } catch (final InterruptedException ex) {
            // Interrupted while waiting on the listeners reacting on the alert
            // We do not re-throw the exception because the AllocationListener should not throw an exception
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** {@link ColumnStoreFactoryCreator} for {@link ArrowColumnStoreFactory}. Used by the columnar framework. */
    public static final class ArrowColumnStoreFactoryCreator implements ColumnStoreFactoryCreator {

        @Override
        public ArrowColumnStoreFactory createFactory(final long memoryLimit) {
            return new ArrowColumnStoreFactory(memoryLimit);
        }
    }
}
