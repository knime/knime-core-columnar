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
 *   Oct 12, 2020 (dietzc): created
 */
package org.knime.core.columnar.access;

import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.VoidDataSpec;

/**
 * VoidValueFactory to create VoidData.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
final class ColumnarVoidAccessFactory implements ColumnarAccessFactory {

    /** Singleton instance on Void **/
    static final ColumnarVoidAccessFactory INSTANCE = new ColumnarVoidAccessFactory();

    private ColumnarVoidAccessFactory() {
    }

    @Override
    public VoidWriteAccess createWriteAccess(final ColumnDataIndex index) {
        return VoidWriteAccess.WRITE_ACCESS_INSTANCE;
    }

    @Override
    public VoidReadAccess createReadAccess(final ColumnDataIndex index) {
        return VoidReadAccess.READ_ACCESS_INSTANCE;
    }

    @Override
    public ColumnarReadAccess createFixedReadAccess(final int index) {
        return VoidReadAccess.READ_ACCESS_INSTANCE;
    }

    static final class VoidReadAccess implements ColumnarReadAccess {

        private static final VoidReadAccess READ_ACCESS_INSTANCE = new VoidReadAccess();

        @Override
        public boolean isMissing() {
            return true;
        }

        @Override
        public void setData(final NullableReadData data) {
            // there is no actual data
        }

        @Override
        public DataSpec getDataSpec() {
            return VoidDataSpec.INSTANCE;
        }

    }

    static final class VoidWriteAccess implements ColumnarWriteAccess {

        private static final VoidWriteAccess WRITE_ACCESS_INSTANCE = new VoidWriteAccess();

        @Override
        public void setMissing() {
            // there is no actual data
        }

        @Override
        public void setData(final NullableWriteData data) {
            // there is no actual data
        }

        @Override
        public void setFrom(final ReadAccess access) {
            // there is no actual data
        }

    }
}
