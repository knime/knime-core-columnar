#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2024-12'

library "knime-pipeline@$BN"

def baseBranch = (BN == KNIMEConstants.NEXT_RELEASE_BRANCH ? "master" : BN.replace("releases/",""))

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
                defaultValue: false,
                description: "Use the HeapBadger instead of the Object Cache",
                name: "USE_HEAP_BADGER",
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

try {
    knimetools.defaultTychoBuild('org.knime.update.core.columnar')

    stage("knime-core-columnar workflow tests") {
        workflowTests.runTests(
            dependencies: [
                repositories: ['knime-core-columnar', 'knime-datageneration', 'knime-jep', 'knime-ensembles', 'knime-xml', 'knime-distance']
            ]
        )
    }

    if (params["KNIME_BASE_WORKFLOW_TESTS"]) {
        vmArgs = ''
        if (params["USE_HEAP_BADGER"]) {
            vmArgs = '-Dknime.columnar.heapbadger.enable=true'
        }

        stage("knime-base workflow tests (${vmArgs})") {
            withEnv(["MALLOC_ARENA_MAX=1"]) {
                def testflowsDir = "Testflows (${baseBranch})/knime-base"
                def excludedTestflows = [
                    "\\\\QDate&Time/test_AP-6112_DateTimeDifference\\\\E",
                    "\\\\QDate&Time/test_AP-6963_StringToDurationPeriod\\\\E",
                    "\\\\QTransformation/test_CollectionCreator3\\\\E",
                    "\\\\QFlow Control/test_endModelCase\\\\E", // TODO AP-20719 - fix this test
                ].join('|')
                def testflowsRegex =
                "/\\\\Q${testflowsDir}\\\\E/(?:(?!OS|\\\\QFile Handling v2\\\\E|Staging|${excludedTestflows})|OS/__KNIME_OS__/|\\\\QStaging/${BRANCH_NAME}\\\\E).+"

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
                            "knime-chemistry",
                            "knime-cloud",
                            "knime-conda",
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
                            "knime-gateway",
                            "knime-h2o",
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
                            "knime-python",
                            "knime-python-legacy",
                            "knime-r",
                            "knime-scripting-editor",
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
    }

    // TODO run on a specific benchmark node
    if (params["RUN_BENCHMARKS"]) {
        node('maven && java17 && ubuntu22.04 && workflow-tests') {
            stage('Run benchmarks') {
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
                            mvn -e -Dmaven.test.failure.ignore=true -Dtycho.localArtifacts=ignore -Dknime.p2.repo=${P2_REPO} clean verify -Pbenchmark
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
