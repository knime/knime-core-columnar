#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2023-12'

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
        ],
    ),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

try {
    knimetools.defaultTychoBuild('org.knime.update.core.columnar')

    workflowTests.runTests(
        dependencies: [
            repositories: ['knime-core-columnar', 'knime-datageneration', 'knime-jep', 'knime-ensembles', 'knime-xml', 'knime-distance']
        ]
    )

    if (params["KNIME_BASE_WORKFLOW_TESTS"]) {
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
                dependencies: [
                    repositories:  ["knime-base", "knime-expressions", "knime-core", "knime-core-ui", "knime-pmml", "knime-pmml-compilation",
                    "knime-pmml-translation", "knime-r", "knime-jep","knime-kerberos", "knime-database", "knime-datageneration",
                    "knime-filehandling", "knime-js-base", "knime-ensembles", "knime-distance", "knime-xml", "knime-jfreechart",
                    "knime-timeseries", "knime-python", "knime-python-legacy", "knime-conda", "knime-stats", "knime-h2o", "knime-weka", "knime-birt", "knime-svm",
                    "knime-js-labs", "knime-optimization", "knime-streaming", "knime-textprocessing", "knime-chemistry", "knime-python", "knime-testing-internal",
                    "knime-exttool", "knime-parquet", "knime-bigdata", "knime-bigdata-externals", "knime-cloud", "knime-js-core", "knime-office365",
                    "knime-database-proprietary","knime-svg", "knime-excel", "knime-wide-data", "knime-aws", "knime-credentials-base", "knime-gateway"],
                    ius: ["org.knime.features.chem.types.feature.group"]
                ],
                sidecarContainers: [
                    [ image: "${dockerTools.ECR}/knime/sshd:alpine3.11", namePrefix: "SSHD", port: 22 ]
                ]
            )
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
