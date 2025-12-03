#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2026-06'

library "knime-pipeline@$BN"

def baseBranch = ((BN == KNIMEConstants.NEXT_RELEASE_BRANCH || BN == "releases/STS" || BN == "releases/STS-next") ? "master" : BN.replace("releases/",""))

properties([
    pipelineTriggers([
        upstream(
            'knime-core/' + env.BRANCH_NAME.replaceAll('/', '%2F') +
            ', knime-core-table/' + env.BRANCH_NAME.replaceAll('/', '%2F')
            )
    ]),
    parameters(
        workflowTests.getConfigurationsAsParameters([columnarBackendDefault: true]) +
        [
            booleanParam(
                defaultValue: true,
                description: "Run knime-base workflow tests with the columnar backend",
                name: "KNIME_BASE_WORKFLOW_TESTS",
            ),
            booleanParam(
                defaultValue: true,
                description: "Use legacy Object Cache and batching instead of the HeapBadger",
                name: "USE_LEGACY_BATCHING",
            ),
            booleanParam(
                defaultValue: false,
                description: "Use the HeapBadger instead of the Object Cache",
                name: "USE_HEAP_BADGER",
            ),
            booleanParam(
                defaultValue: true,
                description: "Use off-heap Arrow implementation",
                name: "USE_OFF_HEAP",
            ),
            booleanParam(
                defaultValue: false,
                description: "Use on-heap Arrow implementation",
                name: "USE_ON_HEAP",
            ),
            booleanParam(
                defaultValue: BRANCH_NAME == 'master',
                description: "Run the benchmarks",
                name: "RUN_BENCHMARKS",
            ),
        ],
    ),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

@NonCPS
Map<String, String> getTestflowConfigs() {
    def heapBadgerProperty = 'knime.columnar.heapbadger.enable'
    def onHeapProperty = 'org.knime.core.columnar.store.useOnHeapColumnStore'
    Map<String, String> testflowConfigs = [:]

    if (params["USE_LEGACY_BATCHING"] && params["USE_OFF_HEAP"]) {
        testflowConfigs["LEGACY-BATCH OFF-HEAP"] = "-D${heapBadgerProperty}=false\n-D${onHeapProperty}=false\n-Darrow.memory.debug.allocator=true\n-Darrow.memory.debug.allocator.verbose=true\n-Darrow.memory.debug.stacktrace=true\n-Dbuffer.check.leaks=true"
    }
    if (params["USE_LEGACY_BATCHING"] && params["USE_ON_HEAP"]) {
        testflowConfigs["LEGACY-BATCH ON-HEAP"] = "-D${heapBadgerProperty}=false\n-D${onHeapProperty}=true"
    }
    if (params["USE_HEAP_BADGER"] && params["USE_OFF_HEAP"]) {
        testflowConfigs["HEAP-BADGER OFF-HEAP"] = "-D${heapBadgerProperty}=true\n-D${onHeapProperty}=false"
    }
    if (params["USE_HEAP_BADGER"] && params["USE_ON_HEAP"]) {
        testflowConfigs["HEAP-BADGER ON-HEAP"] = "-D${heapBadgerProperty}=true\n-D${onHeapProperty}=true"
    }

    return testflowConfigs
}

def runCoreColumnarWorkflowTests(String vmArgs) {
    workflowTests.runTests(
        additionalVmArguments: vmArgs,
        dependencies: [
            repositories: ['knime-core-columnar', 'knime-datageneration', 'knime-jep', 'knime-ensembles', 'knime-xml', 'knime-distance']
        ]
    )
}

def runBaseWorkflowTests(String vmArgs, String testflowsDir, String testflowsRegex) {
    withEnv(["MALLOC_ARENA_MAX=1"]) {
        workflowTests.runTests(
            testflowsDir: testflowsDir,
            testflowsRegex: testflowsRegex,
            additionalVmArguments: vmArgs,
            dependencies: [
                repositories:  [
                    "knime-aws",
                    "knime-base",
                    "knime-bigdata",
                    "knime-bigdata-externals",
                    "knime-birt",
                    "knime-cef",
                    "knime-chemistry",
                    "knime-cloud",
                    "knime-conda",
                    "knime-conda-channels",
                    "knime-core",
                    "knime-core-ui",
                    "knime-credentials-base",
                    "knime-database",
                    "knime-database-proprietary",
                    "knime-datageneration",
                    "knime-distance",
                    "knime-ensembles",
                    "knime-excel",
                    "knime-expressions",
                    "knime-exttool",
                    "knime-filehandling",
                    "knime-filehandling-core",
                    "knime-gateway",
                    "knime-h2o",
                    "knime-hubclient-sdk",
                    "knime-jep",
                    "knime-jfreechart",
                    "knime-js-base",
                    "knime-js-core",
                    "knime-js-labs",
                    "knime-kerberos",
                    "knime-office365",
                    "knime-optimization",
                    "knime-parquet",
                    "knime-pmml",
                    "knime-pmml-compilation",
                    "knime-pmml-translation",
                    "knime-python",
                    "knime-python-legacy",
                    "knime-r",
                    "knime-stats",
                    "knime-streaming",
                    "knime-svg",
                    "knime-svm",
                    "knime-testing-internal",
                    "knime-textprocessing",
                    "knime-timeseries",
                    "knime-weka",
                    "knime-wide-data",
                    "knime-xml",
                ],
                ius: ["org.knime.features.chem.types.feature.group"],
            ],
            sidecarContainers: [
                [ image: "${dockerTools.ECR}/knime/sshd:alpine3.11", namePrefix: "SSHD", port: 22 ]
            ]
        )
    }
}

try {
    knimetools.defaultTychoBuild('org.knime.update.core.columnar')

    def testflowConfigs = getTestflowConfigs()

    stage("knime-core-columnar workflow tests") {
        def parallelConfigs = [:]
        testflowConfigs.each { name, vmArgs ->
            parallelConfigs[name] = {
                runCoreColumnarWorkflowTests(vmArgs)
            }
        }
        parallel(parallelConfigs)
    }

    if (params["KNIME_BASE_WORKFLOW_TESTS"]) {
        stage("knime-base workflow tests") {
            def testflowsDir = "Testflows (${baseBranch})/knime-base"
            def excludedTestflows = [
                "\\\\QDate&Time/deprecated/test_AP-6112_DateTimeDifference\\\\E", // uses missing value cause
                "\\\\QDate&Time/deprecated/test_AP-6963_StringToDurationPeriod\\\\E", // uses missing value cause
                "\\\\QDate&Time/test_UIEXT-1670_DateTimeDifference\\\\E", // uses missing value cause
                "\\\\QDate&Time/test_UIEXT-2157_StringToDuration\\\\E", // uses missing value cause
                "\\\\QTransformation/test_CollectionCreator3\\\\E",
                "\\\\QFlow Control/test_endModelCase\\\\E", // TODO AP-20719 - fix this test
            ].join('|')
            def testflowsRegex =
            "/\\\\Q${testflowsDir}\\\\E/(?:(?!OS|\\\\QFile Handling v2\\\\E|Staging|${excludedTestflows})|OS/__KNIME_OS__/|\\\\QStaging/${BRANCH_NAME}\\\\E).+"

            def parallelConfigs = [:]
            testflowConfigs.each { name, vmArgs ->
                parallelConfigs[name] = {
                    runBaseWorkflowTests(vmArgs, testflowsDir, testflowsRegex)
                }
            }
            parallel(parallelConfigs)
        }
    }

    // TODO run on a specific benchmark node
    if (params["RUN_BENCHMARKS"]) {
        node('maven && java21 && ubuntu22.04 && workflow-tests') {
            stage('Run benchmarks') {
                withEnv(["MALLOC_ARENA_MAX=1"]) {
                    env.lastStage = env.STAGE_NAME

                    // Checkout source code
                    checkout scm

                    // Run benchmarks
                    withMaven(mavenOpts: '-Xmx10G') {
                        withCredentials([
                            usernamePassword(credentialsId: 'ARTIFACTORY_CREDENTIALS',
                            passwordVariable: 'ARTIFACTORY_PASSWORD',
                            usernameVariable: 'ARTIFACTORY_LOGIN'),
                        ]) {
                            sh '''
                                mvn -e -Dtycho.localArtifacts=ignore -Dknime.p2.repo=${P2_REPO} clean verify -Pbenchmark
                            '''
                        }
                    }

                    // Archive results
                    resultFile = "target/benchmark-results.json"
                    sh """
                        jq -s add \$(find . -path "*/target/surefire-reports/benchmark-results.json") > ${WORKSPACE}/${resultFile}
                    """
                    archiveArtifacts artifacts: resultFile
                    jmhReport resultFile
                }
            }
        }
    }

    stage('Sonarqube analysis') {
        env.lastStage = env.STAGE_NAME
        workflowTests.runSonar()
    }
} catch (ex) {
    currentBuild.result = 'FAILURE'
    throw ex
} finally {
    notifications.notifyBuild(currentBuild.result);
}
/* vim: set shiftwidth=4 expandtab smarttab: */
