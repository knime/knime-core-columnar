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

import java.util.Map;

import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.meta.DataColumnMetaData;
import org.knime.core.data.v2.ValueSchema;

/**
 * Utility class to work with {@link ColumnarValueSchema}s.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany.
 */
public final class ColumnarValueSchemaUtils {

    private ColumnarValueSchemaUtils() {
    }

    /**
     * Create a new {@link ColumnarValueSchema} based on the provided {@link ValueSchema}.
     *
     * @param source the underlying {@link ValueSchema}.
     *
     * @return a new {@link ColumnarValueSchema}.
     */
    public static final ColumnarValueSchema create(final ValueSchema source) {
        return new DefaultColumnarValueSchema(source);
    }

    /**
     * Updates the {@link DataTableSpec} of the passed source scheme with a new {@link DataTableSpec}, including the
     * domains provided in the {@link Map}.
     *
     * @param source the source {@link DataTableSpec}
     * @param domainMap the domains used for update.
     * @param metadataMap the columnar metadata used to update
     *
     * @return the updated {@link ColumnarValueSchema}
     */
    public static final ColumnarValueSchema updateSource(final ColumnarValueSchema source,
        final Map<Integer, DataColumnDomain> domainMap, final Map<Integer, DataColumnMetaData[]> metadataMap) {
        final DataColumnSpec[] result = new DataColumnSpec[source.numColumns() - 1];
        for (int i = 0; i < result.length; i++) {
            final DataColumnSpec colSpec = source.getSourceSpec().getColumnSpec(i);
            final DataColumnDomain domain = domainMap.get(i + 1);
            final DataColumnMetaData[] metadata = metadataMap.get(i + 1);

            if (domain == null && metadata == null) {
                result[i] = colSpec;
            } else {
                final DataColumnSpecCreator creator = new DataColumnSpecCreator(colSpec);
                if (domain != null) {
                    creator.setDomain(domain);
                }

                for (final DataColumnMetaData element : metadata) {
                    creator.addMetaData(element, true);
                }

                result[i] = creator.createSpec();
            }
        }
        return new UpdatedColumnarValueSchema(new DataTableSpec(result), source);
    }

}
