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
package org.knime.core.columnar.data;

import java.time.Duration;

import org.knime.core.columnar.ReadData;
import org.knime.core.columnar.WriteData;
import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.data.ObjectData.ObjectWriteData;

/**
 * Class holding {@link DurationWriteData}, {@link DurationReadData}, and {@link DurationDataSpec} for data holding
 * Duration elements.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class DurationData {

    private DurationData() {
    }

    /**
     * {@link ObjectWriteData} holding Duration elements.
     */
    public static interface DurationWriteData extends ObjectWriteData<Duration> {

        /**
         * Assigns a Duration value to the element at the given index. The contract is that values are only ever set for
         * ascending indices. It is the responsibility of the client calling this method to make sure that the provided
         * index is non-negative and smaller than the capacity of this {@link WriteData}.
         *
         * @param index the index at which to set the Duration value
         * @param val the Duration value to set
         */
        void setDuration(int index, Duration val);

        @Override
        default void setObject(final int index, final Duration obj) {
            setDuration(index, obj);
        }

        @Override
        DurationReadData close(int length);

    }

    /**
     * {@link ObjectReadData} holding Duration elements.
     */
    public static interface DurationReadData extends ObjectReadData<Duration> {

        /**
         * Obtains the Duration value at the given index. It is the responsibility of the client calling this method to
         * make sure that the provided index is non-negative and smaller than the length of this {@link ReadData}.
         *
         * @param index the index at which to obtain the Duration element
         * @return the Duration element at the given index
         */
        Duration getDuration(int index);

        @Override
        default Duration getObject(final int index) {
            return getDuration(index);
        }

    }

    /**
     * {@link DataSpec} for Duration data.
     */
    public static final class DurationDataSpec implements DataSpec {

        static final DurationDataSpec INSTANCE = new DurationDataSpec();

        private DurationDataSpec() {
        }

        @Override
        public <R> R accept(final Mapper<R> v) {
            return v.visit(this);
        }

    }

}
