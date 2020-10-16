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
 *   Oct 16, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A class describing the version of a {@link ArrowColumnDataFactory}. The version consists of an integer number and a
 * list of versions for eventual children of the factory.
 * </p>
 * A factory version can be represented as a String and parsed from a String.
 * </p>
 * Use the static method {@link #version(int, ArrowColumnDataFactoryVersion...)} to create a new version and
 * {@link #version(String)} to parse the version from a String.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowColumnDataFactoryVersion {

    private static final char CHILDREN_START = '[';

    private static final char CHILDREN_END = ']';

    private static final char CHILDREN_SEPARATOR = ';';

    /**
     * Create a new version.
     *
     * @param version an integer defining the version of this factory
     * @param childrenVersions the versions of eventual children factories
     * @return the version
     */
    public static ArrowColumnDataFactoryVersion version(final int version,
        final ArrowColumnDataFactoryVersion... childrenVersions) {
        return new ArrowColumnDataFactoryVersion(version, childrenVersions);
    }

    /**
     * Parse the version from the given string.
     *
     * @param encodedVersion a string created with {@link #toString()}.
     * @return the version
     */
    public static ArrowColumnDataFactoryVersion version(final String encodedVersion) {
        final StringBuilder version = new StringBuilder();
        final List<ArrowColumnDataFactoryVersion> children = new ArrayList<>();
        StringBuilder currentChild = new StringBuilder();
        int nestingLevel = 0;

        for (int i = 0; i < encodedVersion.length(); i++) {
            final char c = encodedVersion.charAt(i);
            // Check for the special chars
            if (nestingLevel == 0 && c == ArrowColumnDataFactoryVersion.CHILDREN_START) {
                // Starting with the children
                nestingLevel++;
            } else if (nestingLevel > 0 && c == ArrowColumnDataFactoryVersion.CHILDREN_START) {
                // Starting inner children
                nestingLevel++;
                currentChild.append(c);
            } else if (nestingLevel == 1 && c == ArrowColumnDataFactoryVersion.CHILDREN_END) {
                // Last child
                nestingLevel--;
            } else if (nestingLevel > 1 && c == ArrowColumnDataFactoryVersion.CHILDREN_END) {
                // Ending inner children
                nestingLevel--;
                currentChild.append(c);
            } else if (nestingLevel == 1 && c == ArrowColumnDataFactoryVersion.CHILDREN_SEPARATOR) {
                // Ending the current child
                children.add(version(currentChild.toString()));
                currentChild = new StringBuilder();
            } else {
                if (nestingLevel == 0) {
                    // This version
                    version.append(c);
                } else {
                    // A child version
                    currentChild.append(c);
                }
            }
        }

        return new ArrowColumnDataFactoryVersion(Integer.parseInt(version.toString()),
            children.toArray(new ArrowColumnDataFactoryVersion[0]));
    }

    private final int m_version;

    private final ArrowColumnDataFactoryVersion[] m_childrenVersions;

    private ArrowColumnDataFactoryVersion(final int version, final ArrowColumnDataFactoryVersion... childrenVersions) {
        m_version = version;
        m_childrenVersions = childrenVersions;
    }

    /**
     * @return the integer version of this factory
     */
    public int getVersion() {
        return m_version;
    }

    /**
     * @param index position of the child factory
     * @return the version of the child factory at the given index
     * @throws ArrayIndexOutOfBoundsException if the factory does not have a child with this index
     */
    public ArrowColumnDataFactoryVersion getChildVersion(final int index) {
        return m_childrenVersions[index];
    }

    /*
     * Grammar for the version Strings:
     * childrenStart ::= '['
     * childrenEnd ::= ']'
     * childrenSeparator ::= ';'
     * version ::= <number> | <number><childrenStart>(<version><childrenSeparator>)+<childrenEnd>
     */
    /**
     * Create a String representation of this version which can be parsed with {@link #version(String)}.
     * <ul>
     * <li>In the case of no children, the String representation if just the String value for the
     * {@link #getVersion()}</li>
     * <li>In the case of children, the String representation is the String value for {@link #getVersion()} followed by
     * a ';' separated list of child versions encapsulated in square brackets.</li>
     * </ul>
     * Grammar: <code>
     * version ::= number | number[(version;)+]
     * </code>
     */
    @Override
    public String toString() {
        // TODO(benjamin) is it significantly faster to create no StringBuilder for no children?
        final StringBuilder sb = new StringBuilder();
        sb.append(m_version);
        if (m_childrenVersions.length > 0) {
            sb.append(CHILDREN_START);
            for (final ArrowColumnDataFactoryVersion c : m_childrenVersions) {
                sb.append(c.toString());
                sb.append(CHILDREN_SEPARATOR);
            }
            sb.append(CHILDREN_END);
        }
        return sb.toString();
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof ArrowColumnDataFactoryVersion)) {
            return false;
        }
        final ArrowColumnDataFactoryVersion o = (ArrowColumnDataFactoryVersion)obj;
        return o.m_version == m_version && Arrays.deepEquals(o.m_childrenVersions, m_childrenVersions);
    }

    @Override
    public int hashCode() {
        return 31 * m_version + Arrays.hashCode(m_childrenVersions);
    }
}
