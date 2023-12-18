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
 *   5 Feb 2024 (chaubold): created
 */
package org.knime.core.columnar.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

/**
 * Utilities to work with {@link FileHandle}s
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class FileHandleUtils {
    private FileHandleUtils() {

    }

    /**
     * Try to delete the file at the given path and if deletion failed (e.g. because there was still a file lock) try
     * again one second later, while logging all errors to the given message consumers.
     *
     * @param path
     * @param warningsConsumer
     * @param errorConsumer
     */
    public static void deleteAndRetry(final Path path, final BiConsumer<String, Exception> warningsConsumer,
        final BiConsumer<String, Exception> errorConsumer) {
        final long FILE_DELETION_RETRY_DELAY_MS = 1000;

        try {
            Files.deleteIfExists(path);
        } catch (IOException ex) {
            warningsConsumer.accept("Encountered exception while deleting file " + path + ". Retrying in "
                + FILE_DELETION_RETRY_DELAY_MS + "ms.", ex);
            // retry after some time on a separate thread
            Executors.newCachedThreadPool()
                .execute(() -> deleteDelayed(path, FILE_DELETION_RETRY_DELAY_MS, errorConsumer));
        }
    }

    private static void deleteDelayed(final Path path, final long delayMs,
        final BiConsumer<String, Exception> errorConsumer) {
        try {
            Thread.sleep(delayMs);
            Files.deleteIfExists(path);
        } catch (IOException ex2) {
            errorConsumer.accept("Exception while deleting file: " + path, ex2);
        } catch (InterruptedException ex2) {
            errorConsumer.accept("Interrupted while deleting file: " + path, ex2);
            Thread.currentThread().interrupt();
        }
    }
}
