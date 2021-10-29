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
 *   Jan 25, 2021 (benjamin): created
 */
package org.knime.core.columnar.arrow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion.version;
import static org.knime.core.table.schema.DataSpecs.INT;
import static org.knime.core.table.schema.DataSpecs.LOGICAL_TYPE;

import org.junit.Test;
import org.knime.core.columnar.arrow.ArrowSchemaMapper.ExtensionArrowColumnDataFactory;
import org.knime.core.columnar.arrow.data.ArrowIntData.ArrowIntDataFactory;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Test the {@link ArrowColumnDataFactoryVersion}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class ArrowColumnDataFactoryVersionTest {

    /** Test encoding and decoding a simple version. */
    @Test
    public void testSimpleVersion() {
        final ArrowColumnDataFactoryVersion simpleVersion = version(15);
        final String simpleVersionString = "15";

        // Test encoding
        final String encoded = simpleVersion.toString();
        assertEquals(simpleVersionString, encoded);

        // Test decoding
        final ArrowColumnDataFactoryVersion decoded = ArrowColumnDataFactoryVersion.version(simpleVersionString);
        assertEquals(simpleVersion, decoded);
        assertNotEquals(version(14), decoded);
        assertEquals(simpleVersion.hashCode(), decoded.hashCode());
    }

    /** Test encoding and decoding a complex version. */
    @Test
    public void testComplexVersion() {
        final ArrowColumnDataFactoryVersion complexVersion = version(1, //
            version(2, //
                version(3), //
                version(4, //
                    version(12), //
                    version(4, version(12), version(3)), //
                    version(1))), //
            version(6), //
            version(17, version(7)));
        final String complexVersionString = "1[2[3;4[12;4[12;3;];1;];];6;17[7;];]";

        // Test encoding
        final String encoded = complexVersion.toString();
        assertEquals(complexVersionString, encoded);

        // Test decoding
        final ArrowColumnDataFactoryVersion decoded = ArrowColumnDataFactoryVersion.version(complexVersionString);
        assertEquals(complexVersion, decoded);
        assertNotEquals(version(2), decoded);
        assertEquals(complexVersion.hashCode(), complexVersion.hashCode());
    }

    /** Test decoding a invalid version strings. */
    @Test
    public void testInvalidVersion() {
        final String noChildrenEnd = "1[2;3;";
        assertThrows(IllegalArgumentException.class, () -> ArrowColumnDataFactoryVersion.version(noChildrenEnd));

        final String noInnerChildrenEnd = "1[2[2;3;";
        assertThrows(IllegalArgumentException.class, () -> ArrowColumnDataFactoryVersion.version(noInnerChildrenEnd));

        final String textAfterChildrenEnd = "1[2;4]2";
        assertThrows(IllegalArgumentException.class, () -> ArrowColumnDataFactoryVersion.version(textAfterChildrenEnd));

    }

    /** Test ExtensionArrowColumnDataFactory wraps version of child factory **/
    @Test
    public void testExtensionArrowColumnDataFactoryVersion() {
        final var factory = ArrowIntDataFactory.INSTANCE;
        final var traits = ColumnarSchema.of(INT(LOGICAL_TYPE("foo"))).getTraits(0);
        final var expectedFactory = new ExtensionArrowColumnDataFactory(factory, traits);
        final var wrappedFactory = ArrowSchemaMapper.wrap(factory, traits);

        assertEquals(expectedFactory.getVersion(), wrappedFactory.getVersion());
        assertEquals(factory.getVersion(), wrappedFactory.getVersion().getChildVersion(0));
    }
}
