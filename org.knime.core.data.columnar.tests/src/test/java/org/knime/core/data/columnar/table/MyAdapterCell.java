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
 *   Oct 19, 2021 (Carsten Haubold, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import java.io.IOException;

import org.knime.core.data.AdapterCell;
import org.knime.core.data.AdapterValue;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataCellDataInput;
import org.knime.core.data.DataCellSerializer;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;

/**
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class MyAdapterCell extends AdapterCell {
    private static final long serialVersionUID = 1L;

    @SafeVarargs
    public MyAdapterCell(final DataCell valueCell, final AdapterValue predefinedAdapters,
        final Class<? extends DataValue>... valueClasses) {
        super(valueCell, predefinedAdapters, valueClasses);
    }

    @SafeVarargs
    public MyAdapterCell(final DataCell valueCell, final Class<? extends DataValue>... valueClasses) {
        super(valueCell, valueClasses);
    }

    public MyAdapterCell(final DataCellDataInput input) throws IOException {
        super(input);
    }

    @Override
    protected boolean equalsDataCell(final DataCell dc) {
        return getAdapterMap().equals(((MyAdapterCell)dc).getAdapterMap());
    }

    @Override
    public int hashCode() {
        return 0;
    }

    public static class MyAdapterCellSerializer extends AdapterCellSerializer<MyAdapterCell> {
        @Override
        public MyAdapterCell deserialize(final DataCellDataInput input) throws IOException {
            return new MyAdapterCell(input);
        }
    }

    /**
     * We use the legacy way of retrieving the appropriate {@link DataCellSerializer} instead of the extension point, because we
     * cannot register a new {@link DataType} to the extension point from this testing fragment.
     *
     * @return the {@link DataCellSerializer} for {@link MyAdapterCell} instances.
     */
    public static DataCellSerializer<MyAdapterCell> getCellSerializer() {
        return new MyAdapterCellSerializer();
    }
}