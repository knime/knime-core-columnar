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
 *   Dec 27, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual.reference;

import java.util.UUID;

import org.knime.core.data.columnar.table.AbstractColumnarContainerTable;
import org.knime.core.data.columnar.table.VirtualTableExtensionTable;
import org.knime.core.data.columnar.table.VirtualTableIncompatibleException;
import org.knime.core.data.container.WrappedTable;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExtensionTable;
import org.knime.core.node.Node;

/**
 * Factory class for {@link ReferenceTable ReferenceTables}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ReferenceTables {

    /**
     * Wraps the given BufferedDataTables into ReferenceTables that can be used as references in a
     * VirtualTableExtensionTable.
     * 
     * @param tables to wrap into ReferenceTables
     * @return ReferenceTables that wrap the provided input tables
     * @throws VirtualTableIncompatibleException if any of the tables is not compatible with virtual columnar tables
     */
    public static ReferenceTable[] createReferenceTables(final BufferedDataTable[] tables)
        throws VirtualTableIncompatibleException {
        var refTables = new ReferenceTable[tables.length];
        for (int i = 0; i < tables.length; i++) {
            refTables[i] = createReferenceTable(tables[i]);
        }
        return refTables;
    }

    /**
     * Wraps a BufferedDataTable into a ReferenceTable that can be used as reference in a VirtualTableExtensionTable.
     * 
     * @param id unique identifier for the table
     * @param table to turn into a ReferenceTable
     * @return a ReferenceTable wrapping the input table
     * @throws VirtualTableIncompatibleException if the input table is not compatible with columnar virtual tables
     */
    @SuppressWarnings("resource") // the caller must handle the life-cycle of the returned table
    public static ReferenceTable createReferenceTable(final UUID id, final BufferedDataTable table)
        throws VirtualTableIncompatibleException {
        final ExtensionTable extensionTable = extractExtensionTable(table);
        if (extensionTable instanceof VirtualTableExtensionTable) {
            final VirtualTableExtensionTable virtualExtensionTable = (VirtualTableExtensionTable)extensionTable;
            return new VirtualReferenceTable(table, id, virtualExtensionTable);
        } else if (extensionTable instanceof AbstractColumnarContainerTable) {
            final AbstractColumnarContainerTable columnarTable = (AbstractColumnarContainerTable)extensionTable;
            return new ColumnarContainerReferenceTable(table, id, columnarTable);
        } else {
            // we end up here if the reference tables are not extension tables (e.g. RearrangeColumnsTable)
            return new BufferedReferenceTable(table, id);
        }
    }

    /**
     * Convenience method for wrapping a BufferedDataTable into a ReferenceTable.
     * Creates a new {@link UUID id} via {@link UUID#randomUUID()}.
     * 
     * @param table to wrap
     * @return a ReferenceTable that wraps the provided BufferedDataTable
     * @throws VirtualTableIncompatibleException if the provided table is not compatible with columnar virtual tables
     */
    public static ReferenceTable createReferenceTable(final BufferedDataTable table)
        throws VirtualTableIncompatibleException {
        return createReferenceTable(UUID.randomUUID(), table);
    }

    private static ExtensionTable extractExtensionTable(final BufferedDataTable table) {
        var delegate = Node.invokeGetDelegate(table);
        if (delegate instanceof ExtensionTable) {
            return (ExtensionTable)delegate;
        } else if (delegate instanceof WrappedTable) {
            return extractExtensionTable(delegate.getReferenceTables()[0]);
        } else {
            return null;
        }
    }

    private ReferenceTables() {

    }
}
