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
package org.knime.core.data.columnar.schema;

import java.util.Arrays;
import java.util.UUID;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.v2.RowKeyValueFactory;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchemaUtils;

/**
 * Utility class to work with {@link ColumnarValueSchema}s.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany.
 */
public final class ColumnarValueSchemaUtils {

    private ColumnarValueSchemaUtils() {
    }

    /**
     * Checks if a schema includes a RowID column.
     *
     * @param schema to check
     * @return true if the schema has a RowID column
     */
    // TODP (TP) move to ValueSchemaUtils?
    public static final boolean hasRowID(final ValueSchema schema) {
        return schema.numColumns() > 0 && schema.getValueFactory(0) instanceof RowKeyValueFactory;
    }

    /**
     * Assign new random column names.
     *
     * @param schema input schema
     * @return a new {@code ColumnarValueSchema}, equivalent to input {@code schema} but with new random column names.
     */
    // TODP (TP) move to ValueSchemaUtils?
    public static ValueSchema renameToRandomColumnNames(final ValueSchema schema) {
        var valueFactories = new ValueFactory<?, ?>[schema.numColumns()];
        Arrays.setAll(valueFactories, schema::getValueFactory);

        final DataTableSpec sourceSpec = schema.getSourceSpec();
        var colSpecs = new DataColumnSpec[sourceSpec.getNumColumns()];
        Arrays.setAll(colSpecs, i -> {
            DataColumnSpecCreator creator = new DataColumnSpecCreator(sourceSpec.getColumnSpec(i));
            creator.setName("random-" + UUID.randomUUID().toString());
            return creator.createSpec();
        });
        var spec = new DataTableSpec(colSpecs);

        return ValueSchemaUtils.create(spec, valueFactories);
    }

    // TODP (TP) move to ValueSchemaUtils?
    public static ValueSchema selectColumns(final ValueSchema schema, final int... columnIndices) {

        var valueFactories = new ValueFactory<?, ?>[columnIndices.length];
        Arrays.setAll(valueFactories, i -> schema.getValueFactory(columnIndices[i]));

        final DataTableSpec sourceSpec = schema.getSourceSpec();
        var colSpecs = new DataColumnSpec[columnIndices.length];
        Arrays.setAll(colSpecs, i -> sourceSpec.getColumnSpec(columnIndices[i] - 1));
        var spec = new DataTableSpec(colSpecs);

        return ValueSchemaUtils.create(spec, valueFactories);
    }

}
