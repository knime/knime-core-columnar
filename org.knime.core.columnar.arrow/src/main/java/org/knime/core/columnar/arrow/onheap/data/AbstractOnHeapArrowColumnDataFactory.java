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
 *   Nov 4, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow.onheap.data;

import java.util.Arrays;
import java.util.Objects;

import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.onheap.OnHeapArrowColumnDataFactory;
import org.knime.core.columnar.data.NullableReadData;

/**
 * Abstract implementation of {@link OnHeapArrowColumnDataFactory} for {@link OnHeapArrowReadData} which extend
 * {@link AbstractOnHeapArrowReadData}. Holds the current version.
 *
 * Overwrite {@link #getDictionaries(NullableReadData)} if the data object contains dictionaries.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
abstract class AbstractOnHeapArrowColumnDataFactory implements OnHeapArrowColumnDataFactory {

    /** The current version */
    protected final ArrowColumnDataFactoryVersion m_version;

    protected final OnHeapArrowColumnDataFactory[] m_children;

    /**
     * Create a new abstract {@link OnHeapArrowColumnDataFactory}.
     *
     * @param version the current version
     * @param children nested factories if any. These are part of the version but the tree structure of the factories
     *            does not have to be reflected by the vectors or the data.
     */
    protected AbstractOnHeapArrowColumnDataFactory(final int version, final OnHeapArrowColumnDataFactory... children) {
        m_version = constructVersion(version, children);
        m_children = children;
    }

    @Override
    public ArrowColumnDataFactoryVersion getVersion() {
        return m_version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_version, Arrays.hashCode(m_children));
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        AbstractOnHeapArrowColumnDataFactory other = (AbstractOnHeapArrowColumnDataFactory)obj;
        return m_version.equals(other.m_version) && Arrays.equals(m_children, other.m_children);
    }

    @Override
    public String toString() {
        var childrenString = m_children.length == 0 ? "" : Arrays.toString(m_children);
        return this.getClass().getSimpleName() + ".v" + m_version.getVersion() + childrenString;
    }

    private static ArrowColumnDataFactoryVersion constructVersion(final int version,
        final OnHeapArrowColumnDataFactory... children) {
        var childVersions = new ArrowColumnDataFactoryVersion[children.length];
        for (int i = 0; i < children.length; i++) {
            childVersions[i] = children[i].getVersion();
        }
        return ArrowColumnDataFactoryVersion.version(version, childVersions);
    }
}
