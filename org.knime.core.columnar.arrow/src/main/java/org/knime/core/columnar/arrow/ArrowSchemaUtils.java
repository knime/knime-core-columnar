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
package org.knime.core.columnar.arrow;

import java.nio.file.Path;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.Duration;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeUtf8;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.PrimitiveTypeVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.knime.core.columnar.arrow.extensiontypes.StructDictEncodedExtensionType;
import org.knime.core.columnar.arrow.extensiontypes.StructDictEncodedLogicalTypeExtensionType;
import org.knime.core.columnar.arrow.extensiontypes.LogicalTypeExtensionType;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpecs;
import org.knime.core.table.schema.DataSpecs.DataSpecWithTraits;
import org.knime.core.table.schema.traits.DataTrait;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;
import org.knime.core.table.schema.traits.DataTraitUtils;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.LogicalTypeTrait;

import com.google.common.base.Preconditions;

/**
 * Utility class for dealing with Arrow's {@link Schema}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowSchemaUtils {

	/**
	 * Reads the schema of an Arrow file stored at the provided path.
	 * 
	 * @param path where the Arrow file is located
	 * @return the schema of the file
	 */
    public static ColumnarSchema readSchema(final Path path) {
        var arrowSchema = ArrowReaderWriterUtils.readSchema(path.toFile());
        return convertSchema(arrowSchema);
    }

    /**
     * Converts a {@link Schema} into a {@link ColumnarSchema} including the extraction of {@link DataTraits}.
     *
     * @param schema to convert
     * @return the converted schema
     */
    public static ColumnarSchema convertSchema(final Schema schema) {
        var specsWithTraits = schema.getFields().stream()//
            .map(ArrowSchemaUtils::parseField)//
            .toArray(DataSpecWithTraits[]::new);
        return ColumnarSchema.of(specsWithTraits);
    }

    /**
     * Extracts the column names from a {@link Schema}.
     *
     * @param schema to extract the column names from
     * @return the column names
     */
    public static String[] extractColumnNames(final Schema schema) {
        return schema.getFields().stream()//
            .map(Field::getName)//
            .toArray(String[]::new);
    }

    static DataSpecWithTraits parseField(final Field field) {
        return parseField(field, field.getType());
    }

    private static DataSpecWithTraits parseField(final Field field, final ArrowType type) {//NOSONAR
        if (type instanceof ArrowType.Struct) {
            return parseStructField(field, type);
        } else if (type instanceof StructDictEncodedLogicalTypeExtensionType) {
            return parseStructDictEncodedValueFactoryField(field, (StructDictEncodedLogicalTypeExtensionType)type);
        } else if (type instanceof LogicalTypeExtensionType) {
            return parseValueFactoryField(field, (LogicalTypeExtensionType)type);
        } else if (type instanceof StructDictEncodedExtensionType) {
            return parseStructDictEncodedField(field, (StructDictEncodedExtensionType)type);
        } else if (type instanceof ArrowType.List) {
            return parseListField(field, type);
        } else {
            return type.accept(new PrimitiveFieldVisitor());
        }
    }

    private static DataSpecWithTraits parseStructDictEncodedValueFactoryField(final Field field,
        final StructDictEncodedLogicalTypeExtensionType type) {
        var logicalTypeTrait = new LogicalTypeTrait(type.getValueFactoryType().getValueFactory());
        var dictEncodingTrait = new DataTrait.DictEncodingTrait(parseKeyType(field));
        return parseStorageAndAddTrait(field, type, logicalTypeTrait, dictEncodingTrait);
    }

    private static DataSpecWithTraits parseValueFactoryField(final Field field, final LogicalTypeExtensionType type) {
        var logicalTypeTrait = new LogicalTypeTrait(type.getValueFactory());
        return parseStorageAndAddTrait(field, type, logicalTypeTrait);
    }

    private static DataSpecWithTraits parseStorageAndAddTrait(final Field field, final ExtensionType type,
        final DataTrait... traits) {
        var storageSpecWithTraits = parseField(field, type.storageType());
        return new DataSpecWithTraits(storageSpecWithTraits.spec(),
            DataTraitUtils.withTrait(storageSpecWithTraits.traits(), traits));
    }

    private static DataSpecWithTraits parseStructDictEncodedField(final Field field, final StructDictEncodedExtensionType type) {
        var dictEncodingTrait = new DataTrait.DictEncodingTrait(parseKeyType(field));
        return parseStorageAndAddTrait(field, type, dictEncodingTrait);
    }

    private static KeyType parseKeyType(final Field dictEncodedField) {
        var children = dictEncodedField.getChildren();
        Preconditions.checkArgument(!children.isEmpty(), "The struct-dict-encoded field has no children.");
        var keyField = children.get(0);
        return parseKeyType(keyField.getType());
    }

    private static KeyType parseKeyType(final ArrowType keyType) {
        Preconditions.checkArgument(keyType instanceof Int,
            "The key type must be an integer type (byte, int, long) but was '%s'.", keyType);
        var bitWidth = ((Int)keyType).getBitWidth();
        if (bitWidth == 8) {
            return KeyType.BYTE_KEY;
        } else if (bitWidth == 32) {
            return KeyType.INT_KEY;
        } else if (bitWidth == 64) {
            return KeyType.LONG_KEY;
        } else {
            throw new IllegalArgumentException(String.format(
                "Unsupported bit width '%s' for dictionary key type. Supported are 8, 32 and 64 bits.", bitWidth));
        }
    }

    private static DataSpecWithTraits parseStructField(final Field structField, final ArrowType type) {
        assert type instanceof ArrowType.Struct;
        var inner = structField.getChildren().stream()//
            .map(ArrowSchemaUtils::parseField)//
            .toArray(DataSpecWithTraits[]::new);
        return DataSpecs.STRUCT.of(inner);
    }

    private static DataSpecWithTraits parseListField(final Field listField, final ArrowType type) {
        assert type instanceof ArrowType.List;
        var children = listField.getChildren();
        Preconditions.checkState(children.size() == 1,
            "Expected a single inner vector in field '%s'. This is a coding problem.", listField);
        var inner = parseField(children.get(0));
        return DataSpecs.LIST.of(inner);
    }

    private static final class PrimitiveFieldVisitor extends PrimitiveTypeVisitor<DataSpecWithTraits> {

        @Override
        public DataSpecWithTraits visit(final Null type) {
            return DataSpecs.VOID;
        }

        @Override
        public DataSpecWithTraits visit(final Int type) {
            int bitWidth = type.getBitWidth();
            boolean signed = type.getIsSigned();
            if (bitWidth == 8) {
                return signed ? DataSpecs.BYTE : DataSpecs.INT;
            } else if (bitWidth == 16) {
                return DataSpecs.INT;
            } else if (bitWidth == 32) {
                return signed ? DataSpecs.INT : DataSpecs.LONG;
            } else if (bitWidth == 64) {
                // unsigned longs in Arrow are treated as signed longs and will overflow
                return DataSpecs.LONG;
            } else {
                throw new IllegalArgumentException("Unsupported integer bit width encountered: " + bitWidth);
            }
        }

        @Override
        public DataSpecWithTraits visit(final FloatingPoint type) {
            switch (type.getPrecision()) {
                case DOUBLE:
                    return DataSpecs.DOUBLE;
                case SINGLE:
                    return DataSpecs.FLOAT;
                default:
                    throw new IllegalArgumentException("Unsupported precision: " + type.getPrecision());
            }
        }

        @Override
        public DataSpecWithTraits visit(final Utf8 type) {
            return DataSpecs.STRING;
        }

        @Override
        public DataSpecWithTraits visit(final LargeUtf8 type) {
            return DataSpecs.STRING;
        }

        @Override
        public DataSpecWithTraits visit(final Binary type) {
            return DataSpecs.VARBINARY;
        }

        @Override
        public DataSpecWithTraits visit(final LargeBinary type) {
            return DataSpecs.VARBINARY;
        }

        @Override
        public DataSpecWithTraits visit(final FixedSizeBinary type) {
            return DataSpecs.VARBINARY;
        }

        @Override
        public DataSpecWithTraits visit(final Bool type) {
            return DataSpecs.BOOLEAN;
        }

        @Override
        public DataSpecWithTraits visit(final Decimal type) {
            throw unsupported("Decimal");
        }

        @Override
        public DataSpecWithTraits visit(final Date type) {
            throw unsupported("Date");
        }

        @Override
        public DataSpecWithTraits visit(final Time type) {
            throw unsupported("Time");
        }

        @Override
        public DataSpecWithTraits visit(final Timestamp type) {
            throw unsupported("Timestamp");
        }

        @Override
        public DataSpecWithTraits visit(final Interval type) {
            throw unsupported("Interval");
        }

        @Override
        public DataSpecWithTraits visit(final Duration type) {
            throw unsupported("Duration");
        }

        private static UnsupportedOperationException unsupported(final String type) {
            return new UnsupportedOperationException(type + " type is not supported.");
        }

    }

    private ArrowSchemaUtils() {

    }
}
