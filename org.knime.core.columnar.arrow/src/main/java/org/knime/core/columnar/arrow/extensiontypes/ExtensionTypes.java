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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.LogicalTypeTrait;

/**
 * Static factory class for KNIME {@link ExtensionType ExtensionTypes}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ExtensionTypes {

    private static final AtomicBoolean EXT_TYPES_NOT_REGISTERED = new AtomicBoolean(true);

    /**
     * Registers the extension types with Arrow.
     */
    public static void registerExtensionTypes() {
        if (EXT_TYPES_NOT_REGISTERED.getAndSet(false)) {
            ExtensionTypeRegistry.register(LogicalTypeExtensionType.DESERIALIZATION_INSTANCE);
            ExtensionTypeRegistry.register(StructDictEncodedExtensionType.DESERIALIZATION_INSTANCE);
            ExtensionTypeRegistry.register(StructDictEncodedLogicalTypeExtensionType.DESERIALIZATION_INSTANCE);
        }
    }

    /**
     * Wraps the provided field into a field with an ExtensionTypes if the provided traits indicate it.
     *
     * @param field to potentially wrap
     * @param traits of the field
     * @return the wrapped input field if necessary or just the input field
     */
    public static Field wrapInExtensionTypeIfNecessary(final Field field, final DataTraits traits) {
        final var arrowType = field.getType();
        final var valueFactoryType = DataTraits.getTrait(traits, LogicalTypeTrait.class)//
            .map(LogicalTypeTrait::getLogicalType)//
            .map(t -> new LogicalTypeExtensionType(t, arrowType));
        final var structDictType = DataTraits.getTrait(traits, DictEncodingTrait.class)//
                .map(DictEncodingTrait::getKeyType)//
            .map(k -> new StructDictEncodedExtensionType(field, k));
        final ArrowType type;
        if (valueFactoryType.isPresent() && structDictType.isPresent()) {
            type = new StructDictEncodedLogicalTypeExtensionType(valueFactoryType.get(), structDictType.get());
        } else if (valueFactoryType.isPresent()) {
            type = valueFactoryType.get();
        } else if (structDictType.isPresent()) {
            type = structDictType.get();
        } else {
            // no extension types necessary
            return field;
        }
        var newFieldType = new FieldType(field.isNullable(), type, field.getDictionary(), field.getMetadata());
        return new Field(field.getName(), newFieldType, field.getChildren());
    }

    private ExtensionTypes() {
        // static factory class
    }
}
