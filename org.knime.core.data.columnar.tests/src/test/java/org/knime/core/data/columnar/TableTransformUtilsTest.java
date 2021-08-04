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
 *   Jul 30, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;
import org.knime.core.table.virtual.spec.ColumnFilterTransformSpec;
import org.knime.core.table.virtual.spec.IdentityTransformSpec;
import org.knime.core.table.virtual.spec.PermuteTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

/**
 * Contains unit tests for {@link TableTransformUtils}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class TableTransformUtilsTest {

    @Test
    public void testCreateRearrangeTransformationsOnlyFilter() {
        int[] original = {0, 3, 5, 8};
        final List<TableTransformSpec> transforms = TableTransformUtils.createRearrangeTransformations(original, 10);
        assertEquals(1, transforms.size());
        final ColumnFilterTransformSpec spec = (ColumnFilterTransformSpec)transforms.get(0);
        int[] expected = {0, 1, 4, 6, 9};
        assertArrayEquals(expected, spec.getColumnSelection());
    }

    @Test (expected = IllegalArgumentException.class)
    public void testCreateRearrangeTransformationsWithDuplicateIndices() throws Exception {
        int[] original = {0, 3, 5, 3, 9};
        TableTransformUtils.createRearrangeTransformations(original, 10);
    }

    @Test
    public void testCreateRearrangeTransformationsOnlyPermute() {
        int[] original = {4, 0, 3, 1, 2};
        final List<TableTransformSpec> transforms = TableTransformUtils.createRearrangeTransformations(original, 5);
        assertEquals(1, transforms.size());
        final PermuteTransformSpec spec = (PermuteTransformSpec)transforms.get(0);
        int[] expected = {0, 5, 1, 4, 2, 3};
        assertArrayEquals(expected, spec.getPermutation());
    }

    @Test
    public void testCreateRearrangeTransformationsFilterAndPermuteCombined() {
        int[] original = {8, 0, 5, 3};
        final List<TableTransformSpec> transforms = TableTransformUtils.createRearrangeTransformations(original, 10);
        assertEquals(2, transforms.size());
        final ColumnFilterTransformSpec filterSpec = (ColumnFilterTransformSpec)transforms.get(0);
        int[] expectedFilter = {0, 1, 4, 6, 9};
        assertArrayEquals(expectedFilter, filterSpec.getColumnSelection());
        final PermuteTransformSpec permuteSpec = (PermuteTransformSpec)transforms.get(1);
        int[] expectedPermutation = {0, 4, 1, 3, 2};
        assertArrayEquals(expectedPermutation, permuteSpec.getPermutation());
    }

    @Test
    public void testCreateRearrangeTransformationsNoOp() throws Exception {
        int[] original = {0, 1, 2, 3, 4};
        final List<TableTransformSpec> transforms = TableTransformUtils.createRearrangeTransformations(original, 5);
        assertEquals(1, transforms.size());
        assertEquals(IdentityTransformSpec.INSTANCE, transforms.get(0));
    }

    @Test
    public void testCreateRearrangeTransformationsDetectsFilterForTrailingColumns() {
        int[] original = {0, 1, 2};
        final List<TableTransformSpec> transforms = TableTransformUtils.createRearrangeTransformations(original, 5);
        assertEquals(1, transforms.size());
        final ColumnFilterTransformSpec filterSpec = (ColumnFilterTransformSpec)transforms.get(0);
        int[] expected = {0, 1, 2, 3};
        assertArrayEquals(expected, filterSpec.getColumnSelection());
    }
}
