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
 *   Dec 17, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Contains test for {@link DataIndex}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class DataIndexTest {

    @Test
    public void testEqualsAndHashCode() {
        var col0 = DataIndex.createColumnIndex(0);
        var col1 = DataIndex.createColumnIndex(1);
        assertNotEquals(col0, col1);

        var col00 = col0.getChild(0);
        var col01 = col0.getChild(1);
        assertNotEquals(col00, col01);
        assertNotEquals(col0, col00);

        var col0Duplicate = DataIndex.createColumnIndex(0);
        assertEquals(col0, col0Duplicate);
        assertEquals(col0.hashCode(), col0Duplicate.hashCode());

        var col00DuplicateSameParent = col0.getChild(0);
        var col00DuplicateDifferentParent = col0Duplicate.getChild(0);
        assertEquals(col00, col00DuplicateSameParent);
        assertEquals(col00.hashCode(), col00DuplicateSameParent.hashCode());
        assertEquals(col00, col00DuplicateDifferentParent);
        assertEquals(col00.hashCode(), col00DuplicateDifferentParent.hashCode());
    }

    @Test
    public void testIsColumnLevel() {
        var col = DataIndex.createColumnIndex(0);
        assertTrue(col.isColumnLevel());
        var child = col.getChild(0);
        assertFalse(child.isColumnLevel());
    }

}
