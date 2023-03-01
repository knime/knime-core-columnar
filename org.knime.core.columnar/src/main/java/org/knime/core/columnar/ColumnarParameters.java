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
 *   Dec 20, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ColumnarParameters {

    // the initial capacity (in number of held elements) of a single chunk
    // arrow has a minimum capacity of 2
    private static final int CAPACITY_INIT_DEF = 2;

    private static final String CAPACITY_INIT_PROPERTY = "knime.columnar.capacity.initial";

    /**
     * The initial capacity (in number of held elements) of a single chunk. Arrow has a minimum of 2.
     */
    public static final int CAPACITY_INIT = Integer.getInteger(CAPACITY_INIT_PROPERTY, CAPACITY_INIT_DEF);

    // the maximum capacity (in number of held elements) of a single chunk
    // subtract 750 since arrow rounds up to the next power of 2 anyways
    static final int CAPACITY_MAX_DEF = (1 << 15) - 750; // 32,018

    private static final String CAPACITY_MAX_PROPERTY = "knime.columnar.capacity.max";

    /**
     * The maximum capacity (in number of held elements) of a single chunk
     * subtract 750 since arrow rounds up to the next power of 2 anyways
     */
    public static final int CAPACITY_MAX = Integer.getInteger(CAPACITY_MAX_PROPERTY, CAPACITY_MAX_DEF);

    // the target size (in bytes) of a full batch
    private static final long BATCH_SIZE_TARGET_DEF = 1L << 26; // 64 MB

    private static final String BATCH_SIZE_TARGET_PROPERTY = "knime.columnar.batch.size.target";

    /**
     * The target size (in bytes) of a full batch. (Default is 64 MB)
     */
    public static final long BATCH_SIZE_TARGET = Long.getLong(BATCH_SIZE_TARGET_PROPERTY, BATCH_SIZE_TARGET_DEF);


    private ColumnarParameters() {

    }
}
