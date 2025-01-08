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
 *   May 6, 2024 (benjamin): created
 */
package org.knime.core.data.columnar.table.virtual;

import java.util.Map;
import java.util.UUID;
import java.util.function.IntSupplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.core.data.RowKeyValue;
import org.knime.core.data.columnar.table.ColumnarRowContainerUtils;
import org.knime.core.data.columnar.table.ColumnarRowWriteTableSettings;
import org.knime.core.data.columnar.table.VirtualTableIncompatibleException;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTable;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTables;
import org.knime.core.data.container.DataContainerSettings;
import org.knime.core.data.v2.RowContainer;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.data.v2.value.DefaultRowKeyValueFactory;
import org.knime.core.data.v2.value.VoidRowKeyFactory;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.table.access.StringAccess;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.virtual.exec.GraphVirtualTableExecutor;

/**
 * Helper for materializing a {@link ColumnarVirtualTable}. Use the staged builder returned by {@link #materializer()}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @since 5.3.0
 * @noreference
 */
public final class ColumnarVirtualTableMaterializer {

    /** A functional interface for listening to materialization progress updates */
    public interface Progress {
        /**
         * Called during materialization to report the progress.
         *
         * @param rowIndex index of the row that was just materialized
         * @param rowKey RowID of the row that was just materialized
         */
        void update(final long rowIndex, final RowKeyValue rowKey);
    }

    private final Map<UUID, RowAccessible> m_sources;

    private final boolean m_materializeRowKey;

    private final Progress m_progress;

    private final ExecutionContext m_exec;

    private final IntSupplier m_tableIdSupplier;

    private ColumnarVirtualTableMaterializer( //
        final Map<UUID, RowAccessible> sources, //
        final boolean materializeRowKey, //
        final Progress progress, //
        final ExecutionContext exec, //
        final IntSupplier tableIdSupplier //
    ) {
        m_sources = sources;
        m_materializeRowKey = materializeRowKey;
        m_progress = progress;
        m_exec = exec;
        m_tableIdSupplier = tableIdSupplier;
    }

    /**
     * Materialize the given virtual table.
     *
     * @param tableToMaterialize
     * @return a {@link ReferenceTable} which contains the materialized table
     * @throws CanceledExecutionException if the execution was canceled
     * @throws VirtualTableIncompatibleException TODO can this happen???
     */
    public ReferenceTable materialize(final ColumnarVirtualTable tableToMaterialize)
        throws CanceledExecutionException, VirtualTableIncompatibleException {

        try (var writable = createContainer(createContainerSchema(tableToMaterialize.getSchema()));
                var writeCursor = ((RowWriteAccessible)writable).getWriteCursor();
                var readable =
                    new GraphVirtualTableExecutor(tableToMaterialize.getProducingTransform()).execute(m_sources).get(0);
                var readCursor = readable.createCursor();) {
            RowKeyValue rowKey = ((StringAccess.StringReadAccess)readCursor.access().getAccess(0))::getStringValue;
            long rowIndex = 1; // use 1 based indexing
            while (readCursor.forward()) {
                writeCursor.access().setFrom(readCursor.access());
                writeCursor.commit();
                m_progress.update(rowIndex++, rowKey);
            }
            var sinkUUID = UUID.randomUUID();
            return ReferenceTables.createClearableReferenceTable(sinkUUID, writable.finish());
        } catch (CanceledExecutionException canceledException) {
            // this stunt is necessary because ColumnarRowContainerUtils.create throws Exception
            throw canceledException;
        } catch (VirtualTableIncompatibleException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new IllegalStateException("Failed to create append table.", ex);
        }
    }

    private ValueSchema createContainerSchema(final ValueSchema virtualTableSchema) {
        var rowIDValueFactory = m_materializeRowKey ? DefaultRowKeyValueFactory.INSTANCE : VoidRowKeyFactory.INSTANCE;

        assert ValueSchemaUtils.hasRowID(virtualTableSchema) : "The ColumnarValueSchema should have a RowID";
        var valueFactories = Stream.concat( //
            Stream.of(rowIDValueFactory), //
            IntStream.range(1, virtualTableSchema.numColumns()).mapToObj(virtualTableSchema::getValueFactory) //
        ).toArray(ValueFactory<?, ?>[]::new);
        return ValueSchemaUtils.create(virtualTableSchema.getSourceSpec(), valueFactories);

    }

    private RowContainer createContainer(final ValueSchema schema) throws Exception {
        assert ValueSchemaUtils.hasRowID(schema) : "The ColumnarValueSchema should have a RowID";
        var dataContainerSettings = DataContainerSettings.getDefault();
        var columnarContainerSettings =
            new ColumnarRowWriteTableSettings(true, dataContainerSettings.getMaxDomainValues(), false, false, 100, 4);
        return ColumnarRowContainerUtils.create(m_exec, m_tableIdSupplier.getAsInt(), schema,
            columnarContainerSettings);
    }

    // ================== STAGED BUILDER

    /** @return a staged builder that creates a {@link ColumnarVirtualTableMaterializer} */
    public static RequiresSources materializer() {
        return sources -> materializeRowKey -> progress -> exec -> tableIdSupplier //
        -> new ColumnarVirtualTableMaterializer(sources, materializeRowKey, progress, exec, tableIdSupplier);
    }

    /** Builder stage for adding sources */
    public interface RequiresSources {
        /**
         * @param sources all sources of the virtual table
         * @return the next stage of the builder
         */
        RequiresMaterializeRowKey sources(Map<UUID, RowAccessible> sources);
    }

    /** Builder stage for adding materializeRowKey */
    public interface RequiresMaterializeRowKey {
        /**
         * @param materializeRowKey if the row key column should be materialized. Use <code>false<code> to use a
         *            {@link VoidRowKeyFactory} for the output table.
         * @return the next stage of the builder
         */
        RequiresProgress materializeRowKey(boolean materializeRowKey);
    }

    /** Builder stage for adding progress */
    public interface RequiresProgress {
        /**
         * @param progress a function that is called during materialization to report progress
         * @return the next stage of the builder
         */
        RequiresExecutionContext progress(Progress progress);
    }

    /** Builder stage for adding the execution context */
    public interface RequiresExecutionContext {
        /**
         * @param exec the execution context
         * @return the next stage of the builder
         */
        RequiresTableIdSupplier executionContext(ExecutionContext exec);
    }

    /** Builder stage for adding the table id supplier */
    public interface RequiresTableIdSupplier {
        /**
         * @param tableIdSupplier a table id supplier
         * @return the next stage of the builder
         */
        ColumnarVirtualTableMaterializer tableIdSupplier(IntSupplier tableIdSupplier);
    }
}
