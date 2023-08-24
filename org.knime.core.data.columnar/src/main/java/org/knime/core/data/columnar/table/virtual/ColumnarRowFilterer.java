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
 *   Aug 9, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual;

import java.util.function.IntSupplier;

import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.TableBackend.RowFilter;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.ColumnarRowContainerUtils;
import org.knime.core.data.columnar.table.ColumnarRowWriteTableSettings;
import org.knime.core.data.columnar.table.VirtualTableExtensionTable;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTable;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTables;
import org.knime.core.data.container.DataContainerSettings;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.v2.RowContainer;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.value.LongValueFactory;
import org.knime.core.data.v2.value.ValueInterfaces.LongWriteValue;
import org.knime.core.data.v2.value.VoidRowKeyFactory;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ColumnarRowFilterer {

    private final ExecutionContext m_exec;

    private final IntSupplier m_tableIdSupplier;

    public ColumnarRowFilterer(final IntSupplier tableIdSupplier, final ExecutionContext exec) {
        m_tableIdSupplier = tableIdSupplier;
        m_exec = exec;
    }

    public VirtualTableExtensionTable filter(final BufferedDataTable table, final RowFilter filter) throws Exception {
        var maskTable = createMaskTable(table, filter);
        var referenceTables = ReferenceTables.createReferenceTables(table, maskTable);
        var virtualTable = toVirtual(referenceTables[0]).mask(toVirtual(referenceTables[1]));
        return new VirtualTableExtensionTable(referenceTables, virtualTable, maskTable.size(),
            m_tableIdSupplier.getAsInt());
    }

    private ColumnarVirtualTable toVirtual(final ReferenceTable table) {
        return new ColumnarVirtualTable(table.getId(), table.getSchema(), true);
    }

    private BufferedDataTable createMaskTable(final BufferedDataTable table, final RowFilter filter) throws Exception {
        var tableFilter = TableFilter.materializeCols(filter.filterColumns());
        try (var maskContainer = createMaskContainer();
                var writeCursor = maskContainer.createCursor();
                var tableCursor = table.cursor(tableFilter)) {
            for (long i = 0; tableCursor.canForward(); i++) {
                if (filter.test(tableCursor.forward())) {
                    writeCursor.forward().<LongWriteValue> getWriteValue(0).setLongValue(i);
                }
            }
            return maskContainer.finish();
        }
    }

    private RowContainer createMaskContainer() throws Exception {
        var schema = ColumnarValueSchemaUtils.create(
            new DataTableSpec(new DataColumnSpecCreator("mask", LongCell.TYPE).createSpec()),
            new ValueFactory<?, ?>[]{VoidRowKeyFactory.INSTANCE, LongValueFactory.INSTANCE});
        var dataContainerSettings = DataContainerSettings.getDefault();
        var columnarContainerSettings =
            new ColumnarRowWriteTableSettings(true, dataContainerSettings.getMaxDomainValues(), false, false, 100, 4);
        return ColumnarRowContainerUtils.create(m_exec, m_tableIdSupplier.getAsInt(), schema,
            columnarContainerSettings);
    }

}
