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
 *   Aug 16, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.arrow.data;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Arrow extension type that stores the value factory as meta data.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
// the super class implements equals and delegates to extensionEquals
public final class ValueFactoryExtensionType extends ExtensionType { //NOSONAR

    static {
        ExtensionTypeRegistry.register(new ValueFactoryExtensionType());
    }

    /**
     * Corresponds to DataSpec in fast tables.
     */
    private final ArrowType m_storageType;

    /**
     * E.g. the ValueFactory
     */
    private final String m_valueFactory;

    private ValueFactoryExtensionType() {
        m_storageType = null;
        m_valueFactory = null;
    }

    /**
     * Constructor.
     *
     * @param valueFactory the fully qualified class name of the value factory this type is associated with
     * @param storageType the underlying storage type
     */
    private ValueFactoryExtensionType(final String valueFactory, final ArrowType storageType) {
        m_valueFactory = valueFactory;
        m_storageType = storageType;
    }

    /**
     * Convenience method to wrap an {@link ArrowType} into a {@link ValueFactoryExtensionType} if the corresponding
     * data has a logical type.
     *
     * @param storageType underlying {@link ArrowType}
     * @param logicalType logical type of the data (can be null)
     * @return either a {@link ValueFactoryExtensionType} if logicalType was not null or storageType
     */
    public static ArrowType wrapIfLogical(final ArrowType storageType, final String logicalType) {
        if (logicalType != null) {
            return new ValueFactoryExtensionType(logicalType, storageType);
        } else {
            return storageType;
        }
    }

    /**
     * @return the value factory name
     */
    public String getValueFactory() {
        return m_valueFactory;
    }

    @Override
    public ArrowType storageType() {
        return m_storageType;
    }

    @Override
    public String extensionName() {
        return "knime.value_factory";
    }

    @Override
    public boolean extensionEquals(final ExtensionType other) {
        if (other == this) {
            return true;
        }
        if (other instanceof ValueFactoryExtensionType) {
            final ValueFactoryExtensionType otherType = (ValueFactoryExtensionType)other;
            return m_valueFactory.equals(otherType.m_valueFactory) //
                && m_storageType.equals(otherType.m_storageType);
        }
        return false;
    }

    @Override
    public String serialize() {
        // in Python we don't get access to the ExtensionType id, so we have to store it also as meta data
        return m_valueFactory;
    }

    @Override
    public ArrowType deserialize(final ArrowType storageType, final String serializedData) {
        return new ValueFactoryExtensionType(serializedData, storageType);
    }

    @Override
    public FieldVector getNewVector(final String name, final FieldType fieldType, final BufferAllocator allocator) {
        // TODO implement a custom ExtensionTypeVector to be fully Arrow conform
        var minorStorageType = Types.getMinorTypeForArrowType(m_storageType);
        return minorStorageType.getNewVector(name, fieldType, allocator, null);
    }

}
