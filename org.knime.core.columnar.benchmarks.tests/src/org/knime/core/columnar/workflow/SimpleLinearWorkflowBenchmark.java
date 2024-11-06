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
 *   Nov 5, 2024 (benjamin): created
 */
package org.knime.core.columnar.workflow;

import java.util.concurrent.TimeUnit;

import org.knime.core.columnar.params.BatchingParam;
import org.knime.core.columnar.params.ReadTableCachesParam;
import org.knime.core.columnar.params.TableBackendParam;
import org.knime.core.columnar.params.WriteTableCachesParam;
import org.knime.core.node.workflow.WorkflowManager;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmark a simple workflow with a linear sequence of nodes. Compares the columnar backend with different caching
 * configurations to the row based backend.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Timeout(time = 1, timeUnit = TimeUnit.MINUTES)
@Warmup(iterations = 4)
@Measurement(iterations = 8)
@State(Scope.Thread)
@SuppressWarnings("javadoc")
public class SimpleLinearWorkflowBenchmark {

    public enum Params {
            /** Row-based backend - other parameters have no effect */
            ROW_BASED(TableBackendParam.BUFFERED_TABLE, null, null, null),

            /** Columnar backend with heap badger batching, read cache disabled, write cache disabled */
            COLUMNAR__HEAP_BATCH__RC_OFF__WC_OFF(TableBackendParam.COLUMNAR_TABLE, BatchingParam.HEAP_BADGER,
                ReadTableCachesParam.DISABLED, WriteTableCachesParam.DISABLED),

            /** Columnar backend with heap badger batching, read cache disabled, write cache enabled */
            COLUMNAR__HEAP_BATCH__RC_OFF__WC_ON(TableBackendParam.COLUMNAR_TABLE, BatchingParam.HEAP_BADGER,
                ReadTableCachesParam.DISABLED, WriteTableCachesParam.ENABLED),

            /** Columnar backend with heap badger batching, read cache enabled, write cache disabled */
            COLUMNAR__HEAP_BATCH__RC_ON__WC_OFF(TableBackendParam.COLUMNAR_TABLE, BatchingParam.HEAP_BADGER,
                ReadTableCachesParam.ENABLED, WriteTableCachesParam.DISABLED),

            /** Columnar backend with heap badger batching, read cache enabled, write cache enabled */
            COLUMNAR__HEAP_BATCH__RC_ON__WC_ON(TableBackendParam.COLUMNAR_TABLE, BatchingParam.HEAP_BADGER,
                ReadTableCachesParam.ENABLED, WriteTableCachesParam.ENABLED),

            /** Columnar backend with legacy batching, read cache disabled, write cache disabled */
            COLUMNAR__LEGACY_BATCH__RC_OFF__WC_OFF(TableBackendParam.COLUMNAR_TABLE, BatchingParam.LEGACY_BATCHING,
                ReadTableCachesParam.DISABLED, WriteTableCachesParam.DISABLED),

            /** Columnar backend with legacy batching, read cache disabled, write cache enabled */
            COLUMNAR__LEGACY_BATCH__RC_OFF__WC_ON(TableBackendParam.COLUMNAR_TABLE, BatchingParam.LEGACY_BATCHING,
                ReadTableCachesParam.DISABLED, WriteTableCachesParam.ENABLED),

            /** Columnar backend with legacy batching, read cache enabled, write cache disabled */
            COLUMNAR__LEGACY_BATCH__RC_ON__WC_OFF(TableBackendParam.COLUMNAR_TABLE, BatchingParam.LEGACY_BATCHING,
                ReadTableCachesParam.ENABLED, WriteTableCachesParam.DISABLED),

            /** Columnar backend with legacy batching, read cache enabled, write cache enabled */
            COLUMNAR__LEGACY_BATCH__RC_ON__WC_ON(TableBackendParam.COLUMNAR_TABLE, BatchingParam.LEGACY_BATCHING,
                ReadTableCachesParam.ENABLED, WriteTableCachesParam.ENABLED),

        ;

        private final TableBackendParam m_backend;

        private final BatchingParam m_batching;

        private final ReadTableCachesParam m_readCache;

        private final WriteTableCachesParam m_writeCache;

        Params(final TableBackendParam backend, final BatchingParam batching, final ReadTableCachesParam readCache,
            final WriteTableCachesParam writeCache) {
            m_backend = backend;
            m_batching = batching;
            m_readCache = readCache;
            m_writeCache = writeCache;
        }

        public void apply(final WorkflowManager wfm) {
            m_backend.setBackend(wfm);
            if (m_batching != null) {
                m_batching.apply();
            }
            if (m_readCache != null) {
                m_readCache.apply();
            }
            if (m_writeCache != null) {
                m_writeCache.apply();
            }
        }
    }

    private static BenchmarkWorkflow loadWorkflow() throws Exception {
        var workflowDir = BenchmarkWorkflow.getWorkflowDirectory("simple_linear", SimpleLinearWorkflowBenchmark.class)
            .getCanonicalFile();
        var benchmarkWorkflow = new BenchmarkWorkflow();
        benchmarkWorkflow.loadAndSetWorkflowInWorkspace(workflowDir, workflowDir.getParentFile());
        return benchmarkWorkflow;
    }

    @Param
    Params backend_batching_readCaches_writeCaches; // NOSONAR: name displayed as JMH parameter

    private BenchmarkWorkflow m_workflow;

    @Setup(Level.Iteration)
    public void setup() throws Exception {
        m_workflow = loadWorkflow();
        backend_batching_readCaches_writeCaches.apply(m_workflow.getManager());
    }

    @TearDown(Level.Iteration)
    public void tearDown() throws Exception {
        m_workflow.tearDown();
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        m_workflow.getManager().resetAndConfigureAll();
    }

    @Benchmark
    public void runWorkflow() throws Exception {
        m_workflow.executeAllAndWait();
    }
}
