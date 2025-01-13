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
 *   Jan 13, 2025 (benjamin): created
 */
package org.knime.core.columnar.arrow;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utility class for checking if an object has non-null references in its instance fields or if it has a reference to an
 * array in its object tree.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class ReferenceAssertions {

    private ReferenceAssertions() {
    }

    /**
     * Asserts that the given object has no non-null references in its instance fields.
     * <p>
     * Skips primitive fields, AtomicInteger reference counters, and enum fields.
     *
     * @param obj the object to inspect
     * @param message the optional message to include in the assertion error
     * @throws AssertionError if any non-primitive field is not null
     */
    public static void assertNoNonNullReferences(final Object obj, final String message) {
        if (obj == null) {
            return; // Nothing to check if the object itself is null
        }

        var clazz = obj.getClass();
        for (var field : getAllFields(clazz)) {
            // Skip static fields
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }

            // Skip primitive fields, AtomicInteger reference counter and enum fields
            if (field.getType().isPrimitive() //
                || AtomicInteger.class.equals(field.getType()) //
                || field.getType().isEnum() //
            ) {
                continue;
            }

            // Make private fields accessible
            field.setAccessible(true);

            try {
                Object value = field.get(obj);
                if (value != null) {
                    String baseErrorMessage = "Field '" + field.getName() + "' in " + clazz.getSimpleName()
                        + " is not null (value: " + value + ").";
                    throw new AssertionError(formatMessage(message, baseErrorMessage));
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Asserts that the given object has no non-null references in its instance fields.
     *
     * @param obj the object to inspect
     */
    public static void assertNoNonNullReferences(final Object obj) {
        assertNoNonNullReferences(obj, null);
    }

    /**
     * Asserts that the given object has a reference to an array somewhere in its object tree.
     *
     * @param obj the object to inspect
     * @param message the optional message to include in the assertion error
     * @throws AssertionError if no array reference is found
     */
    public static void assertHasArrayReference(final Object obj, final String message) {
        Set<Object> visited = new HashSet<>();
        if (hasArrayReference(obj, visited)) {
            return; // Success: array reference found
        }
        String errorMessage = message != null ? message : "No array reference found in object tree.";
        throw new AssertionError(errorMessage);
    }

    /**
     * Asserts that the given object has a reference to an array somewhere in its object tree.
     *
     * @param obj the object to inspect
     */
    public static void assertHasArrayReference(final Object obj) {
        assertHasArrayReference(obj, null);
    }

    /**
     * Recursively checks if the given object has a reference to an array in its object tree.
     *
     * @param obj the object to inspect
     * @param visited a set to track visited objects and prevent infinite loops
     * @return true if an array reference is found, false otherwise
     */
    private static boolean hasArrayReference(final Object obj, final Set<Object> visited) {
        if (obj == null || visited.contains(obj)) {
            return false;
        }

        // Mark the object as visited
        visited.add(obj);

        Class<?> clazz = obj.getClass();

        // If the object itself is an array, return true
        if (clazz.isArray()) {
            return true;
        }

        // Skip primitive wrapper types and common non-traversable types
        if (isNonTraversableType(clazz)) {
            return false;
        }

        for (var field : getAllFields(clazz)) {
            // Skip static fields
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }

            // Make private fields accessible
            field.setAccessible(true);

            try {
                Object value = field.get(obj);
                if (hasArrayReference(value, visited)) {
                    return true;
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    private static List<Field> getAllFields(final Class<?> clazz) {
        List<Field> fields = new ArrayList<>();

        // Base case: Stop when there's no superclass
        if (clazz == null) {
            return fields;
        }

        // Add declared fields of the current class
        for (Field field : clazz.getDeclaredFields()) {
            fields.add(field);
        }

        // Recursive call to add fields from the superclass
        fields.addAll(getAllFields(clazz.getSuperclass()));

        return fields;
    }

    /**
     * Checks if a class is a non-traversable type, such as primitive wrappers, String, or common types from java.lang
     * and java.util packages.
     *
     * @param clazz the class to check
     * @return true if the class is a non-traversable type, false otherwise
     */
    private static boolean isNonTraversableType(final Class<?> clazz) {
        // Skip primitives and types from java.lang and java.util packages
        return clazz.isPrimitive() //
            || clazz.getPackageName().startsWith("java.lang") //
            || clazz.getPackageName().startsWith("java.util") //
            || clazz.getPackageName().startsWith("sun.nio") //
        ;
    }

    /**
     * Formats the message to include the optional user message and the base error message.
     *
     * @param userMessage the user-specified message
     * @param baseMessage the default assertion message
     * @return the combined message
     */
    private static String formatMessage(final String userMessage, final String baseMessage) {
        if (userMessage == null || userMessage.isEmpty()) {
            return baseMessage;
        }
        return userMessage + " ==> " + baseMessage;
    }
}
