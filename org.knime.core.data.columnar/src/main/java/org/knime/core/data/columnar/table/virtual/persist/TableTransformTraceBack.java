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
 *   May 2, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual.persist;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.knime.core.table.virtual.TableTransform;

final class TableTransformTraceBack {

    private final Set<TableTransform> m_sourceTransforms = new LinkedHashSet<>();

    private final Map<TableTransform, List<TableTransform>> m_childrenTransforms = new LinkedHashMap<>();

    TableTransformTraceBack(final TableTransform transform) {
        traceBack(transform);
    }

    private void traceBack(final TableTransform transform) {
        final List<TableTransform> parents = transform.getPrecedingTransforms();
        if (parents.isEmpty()) {
            m_sourceTransforms.add(transform);
        } else {
            for (final TableTransform parent : parents) {
                List<TableTransform> children = m_childrenTransforms.get(parent);
                if (children == null) {
                    children = new ArrayList<>();
                    children.add(transform);
                    m_childrenTransforms.put(parent, children);
                    traceBack(parent);
                } else {
                    children.add(transform);
                }
            }
        }
    }

    Collection<TableTransform> getSources() {
        return Collections.unmodifiableCollection(m_sourceTransforms);
    }

    List<TableTransform> getChildren(final TableTransform transform) {
        return m_childrenTransforms.getOrDefault(transform, List.of());
    }

}