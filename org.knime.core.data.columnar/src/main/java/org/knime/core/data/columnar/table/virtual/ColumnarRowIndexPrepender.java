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
 *   Nov 10, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual;

import java.util.function.IntSupplier;
import java.util.stream.IntStream;

import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.VirtualTableExtensionTable;
import org.knime.core.data.columnar.table.VirtualTableIncompatibleException;
import org.knime.core.data.columnar.table.virtual.ColumnarVirtualTable.ColumnarMapperWithRowIndexFactory;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTable;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTables;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.value.LongValueFactory;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.table.access.LongAccess.LongWriteAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ColumnarRowIndexPrepender {

    private final IntSupplier m_tableIdGenerator;

    public ColumnarRowIndexPrepender(final IntSupplier tableIdGenerator) {
        m_tableIdGenerator = tableIdGenerator;
    }

    public VirtualTableExtensionTable prependRowIndex(final BufferedDataTable table, final String indexColumnName)
        throws VirtualTableIncompatibleException {
        var refTable = ReferenceTables.createReferenceTable(table);
        var virtualTable = new ColumnarVirtualTable(refTable.getId(), refTable.getSchema(), true);
        var numColumns = table.getDataTableSpec().getNumColumns();
        // 0 is necessary because the comp graph can otherwise not determined a predecessor
        var indexTable = virtualTable.map(new RowIndexGenerator(indexColumnName), 0);
        var outputTable = virtualTable.append(indexTable).selectColumns(
            IntStream.concat(IntStream.of(0, numColumns + 1), IntStream.rangeClosed(1, numColumns)).toArray());
        return new VirtualTableExtensionTable(new ReferenceTable[]{refTable}, outputTable, table.size(),
            m_tableIdGenerator.getAsInt());
    }

    private static final class RowIndexGenerator implements ColumnarMapperWithRowIndexFactory {

        private final String m_columnName;

        RowIndexGenerator(final String columnName) {
            m_columnName = columnName;
        }

        @Override
        public Mapper createMapperWithRowIndex(final ReadAccess[] inputs, final WriteAccess[] outputs) {
            var longAccess = (LongWriteAccess) outputs[0];
            return index -> longAccess.setLongValue(index);
        }

        @Override
        public ColumnarValueSchema getOutputSchema() {
            return ColumnarValueSchemaUtils.create(
                new DataTableSpec(new DataColumnSpecCreator(m_columnName, LongCell.TYPE).createSpec()),
                new ValueFactory<?, ?>[]{LongValueFactory.INSTANCE});
        }

    }
}
