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
 *   Sep 28, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.arrow.extensiontypes;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Arrow ExtensionType for struct-dict-encoded data.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class StructDictEncodedExtensionType extends AbstractExtensionType {//NOSONAR

    static final StructDictEncodedExtensionType DESERIALIZATION_INSTANCE = new StructDictEncodedExtensionType(null);

    static {
        ExtensionTypeRegistry.register(DESERIALIZATION_INSTANCE);
    }

    /**
     * Deserialization constructor.
     *
     * @param storageType type of the underlying storage
     */
    private StructDictEncodedExtensionType(final ArrowType storageType) {
        super(storageType);
    }

    /**
     * Constructor. Verifies that the storageField has the correct format and matches keyType.
     * @param storageField a field holding the dictionary struct
     * @param keyType
     */
    StructDictEncodedExtensionType(final Field storageField, final KeyType keyType) {
        super(storageField.getType());
        Preconditions.checkArgument(storageType() instanceof Struct,
            "The storage type for StructDictEncoded data must be a struct.");
        var children = storageField.getChildren();
        Preconditions.checkArgument(children.size() == 2,
            "The storage struct field is expected to have two children, one for the keys and one for the values."
                + " Instead received '%s'.",
            children);
        var storageKeyType = children.get(0).getType();
        var expectedKeyType = getExpectedKeyType(keyType);
        Preconditions.checkArgument(Objects.equal(storageKeyType, expectedKeyType),
            "Expected key type '%s' but got '%s'.", expectedKeyType, storageKeyType);
    }

    private static Int getExpectedKeyType(final KeyType keyType) {
        switch (keyType) {
            case BYTE_KEY:
                return new Int(8, false);
            case INT_KEY:
                return new Int(32, false);
            case LONG_KEY:
                return new Int(64, false);
            default:
                throw new IllegalArgumentException("Unsupported KeyType: " + keyType);
        }
    }

    @Override
    public String extensionName() {
        return "knime.struct_dict_encoded";
    }

    @Override
    public boolean extensionEquals(final ExtensionType other) {
        if (other == this) {
            return true;
        }
        if (other instanceof StructDictEncodedExtensionType) {
            return storageType().equals(other.storageType());
        }
        return false;
    }

    @Override
    public String serialize() {
        return "";
    }

    @Override
    public StructDictEncodedExtensionType deserialize(final ArrowType storageType, final String serializedData) {
        return new StructDictEncodedExtensionType(storageType);
    }

}
