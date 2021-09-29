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
 *   15 Feb 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.stream.IntStream;

import org.junit.Test;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.ValueSchema;
import org.knime.core.table.schema.traits.LogicalTypeTrait;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class DefaultColumnarValueSchemaTest {

    static DataTableSpec createSpec(final int nCols) {
        return new DataTableSpec(IntStream.range(0, nCols)
            .mapToObj(i -> new DataColumnSpecCreator(Integer.toString(i), IntCell.TYPE).createSpec())
            .toArray(DataColumnSpec[]::new));
    }

    static DefaultColumnarValueSchema createDefaultColumnarValueSchema(final DataTableSpec spec) {
        final ValueSchema valueSchema =
            ValueSchema.create(spec, RowKeyType.NOKEY, NotInWorkflowWriteFileStoreHandler.create());
        final ColumnarValueSchema schema = ColumnarValueSchemaUtils.create(valueSchema);
        return (DefaultColumnarValueSchema)schema;
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetSpecIndexOutOfBoundsLower() {
        createDefaultColumnarValueSchema(createSpec(0)).getSpec(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetSpecIndexOutOfBoundsUpper() {
        createDefaultColumnarValueSchema(createSpec(0)).getSpec(1);
    }

    @Test
    public void testLogicalDataTypePresent() {
        final var tableSpec = createSpec(1);
        final var schema = createDefaultColumnarValueSchema(tableSpec);
        final var traits = schema.getTraits(0);
        final var logicalType = traits.get(LogicalTypeTrait.class);
        assertNotNull(logicalType);
        assertEquals(schema.getValueFactory(0).getClass().getName(), logicalType.getLogicalType());
    }

}
