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
 *   Oct 6, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.arrow.extensiontypes;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;

/**
 * ExtensionType for ValueFactories whose direct primitive type is struct-dict-encoded. This is necessary
 * because Arrow doesn't support direct nesting of extension types. That's because an ExtensionType is just an
 * annotation (meta-data) on an actual Arrow type and nesting multiple ExtensionTypes results in overwriting this
 * annotation. Note that it is possible to have nested ExtensionTypes when they are attached to different Arrow types
 * (e.g. in a struct or a list).
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
// Equals is implemented in the super class and delegates to #extensionEquals
public final class StructDictEncodedValueFactoryType extends AbstractKnimeExtensionType {//NOSONAR

    private final StructDictEncodedType m_structDictEncodedType;

    private final ValueFactoryType m_valueFactoryType;

    static {
        ExtensionTypeRegistry.register(new StructDictEncodedValueFactoryType());
    }

    /**
     * Private constructor for creating the deserialization instance
     */
    private StructDictEncodedValueFactoryType() {
        super(null);
        m_structDictEncodedType = null;
        m_valueFactoryType = null;
    }

    StructDictEncodedValueFactoryType(final ValueFactoryType valueFactoryType,
        final StructDictEncodedType structDictEncodedType) {
        super(structDictEncodedType.storageType());
        m_structDictEncodedType = structDictEncodedType;
        m_valueFactoryType = valueFactoryType;
    }

    /**
     * @return the {@link ValueFactoryType} part of this type
     */
    public ValueFactoryType getValueFactoryType() {
        return m_valueFactoryType;
    }

    /**
     * @return the {@link StructDictEncodedType} part of this type
     */
    public StructDictEncodedType getStructDictEncodedType() {
        return m_structDictEncodedType;
    }

    @Override
    public String extensionName() {
        return "knime.struct_dict_encoded_value_factory";
    }

    @Override
    public boolean extensionEquals(final ExtensionType other) {
        if (other == this) {
            return true;
        }
        if (other instanceof StructDictEncodedValueFactoryType) {
            var otherType = (StructDictEncodedValueFactoryType)other;
            return m_structDictEncodedType.equals(otherType.m_structDictEncodedType)
                && m_valueFactoryType.equals(otherType.m_valueFactoryType);
        }
        return false;
    }

    @Override
    public String serialize() {
        return m_valueFactoryType.serialize();
    }

    @Override
    public ArrowType deserialize(final ArrowType storageType, final String serializedData) {
        var valueFactoryType = ValueFactoryType.DESERIALIZATION_INSTANCE.deserialize(storageType, serializedData);
        // StructDictEncodedType doesn't have any meta data
        var structDictEncodedType = StructDictEncodedType.DESERIALIZATION_INSTANCE.deserialize(storageType, "");
        return new StructDictEncodedValueFactoryType(valueFactoryType, structDictEncodedType);
    }

}
