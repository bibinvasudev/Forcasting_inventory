pipeline {
    agent any

    options {
        timeout(time: 151, unit: 'MINUTES')
        timestamps()
    }

    stages {
        stage('Determine environment config') {
            steps {
                script {
                    ENVIRONMENTS = [
                            LOCAL_DOCKER     : [
                                    NAME                    : 'ow-forecasting-local-docker',
                                    SSH_PUBLISH_CONFIG      : 'LOCAL',
                                    GITHUB_CREDENTIAL       : null,
                                    DOWNLOAD_GITHUB_FILES   : false,
                                    DROP_INTERNAL_DATABASE  : true,
                                    DROP_DSX_WRITE_DATABASE : true,
                                    SETUP_DSX_WRITE_DATABASE: false,  // included in DROP_DSX_WRITE_DATABASE
                                    RUN_END_TO_END_TEST     : true,
                                    RUN_PRODUCTION_FORECAST : false,
                                    EMAIL_ATTACHMENT_LOG    : true,
                            ],
                            CLIENT_TESTING   : [
                                    NAME                    : 'ow-forecasting-client-testing',
                                    SSH_PUBLISH_CONFIG      : 'txau-mleindev1',
                                    GITHUB_CREDENTIAL       : '0107c464-8342-4b56-9299-65ba8f3f020d',
                                    DOWNLOAD_GITHUB_FILES   : true,
                                    DROP_INTERNAL_DATABASE  : true,
                                    DROP_DSX_WRITE_DATABASE : true,
                                    SETUP_DSX_WRITE_DATABASE: false,  // included in DROP_DSX_WRITE_DATABASE
                                    RUN_END_TO_END_TEST     : true,
                                    RUN_PRODUCTION_FORECAST : false,
                                    EMAIL_ATTACHMENT_LOG    : true,
                            ],
                            CLIENT_STAGING   : [
                                    NAME                    : 'ow-forecasting-client-staging',
                                    SSH_PUBLISH_CONFIG      : 'txau-mleintest1',
                                    GITHUB_CREDENTIAL       : '0107c464-8342-4b56-9299-65ba8f3f020d',
                                    DOWNLOAD_GITHUB_FILES   : true,
                                    DROP_INTERNAL_DATABASE  : false,
                                    DROP_DSX_WRITE_DATABASE : false,
                                    SETUP_DSX_WRITE_DATABASE: true,
                                    RUN_END_TO_END_TEST     : false,
                                    RUN_PRODUCTION_FORECAST : true,
                                    EMAIL_ATTACHMENT_LOG    : false,
                            ],
                            CLIENT_PRODUCTION: [
                                    NAME                    : 'ow-forecasting-client-production',
                                    SSH_PUBLISH_CONFIG      : 'txau-mleinprod1',
                                    GITHUB_CREDENTIAL       : '0107c464-8342-4b56-9299-65ba8f3f020d',
                                    DOWNLOAD_GITHUB_FILES   : true,
                                    DROP_INTERNAL_DATABASE  : false,
                                    DROP_DSX_WRITE_DATABASE : false,
                                    SETUP_DSX_WRITE_DATABASE: false,
                                    RUN_END_TO_END_TEST     : false,
                                    RUN_PRODUCTION_FORECAST : false,
                                    EMAIL_ATTACHMENT_LOG    : false,
                            ],
                    ]

                    def jenkins_domain = env.JENKINS_URL ?: 'localhost'
                    switch (jenkins_domain) {
                        case ~/.*localhost.*/:
                        case ~/.*127.0.0.1.*/:
                            ENVIRONMENT = ENVIRONMENTS.LOCAL_DOCKER
                            break;
                        case ~/.*devprd-devops01.*/:
                            if (env.BRANCH_NAME == 'staging') {
                                ENVIRONMENT = ENVIRONMENTS.CLIENT_STAGING
                            } else if (env.BRANCH_NAME == 'production') {
                                ENVIRONMENT = ENVIRONMENTS.CLIENT_PRODUCTION
                            } else {
                                ENVIRONMENT = ENVIRONMENTS.CLIENT_TESTING
                            }
                            break;
                        default:
                            error("Could not determine environment from ${jenkins_domain}")
                    }
                }

                echo "Determined configuration for ENVIRONMENT=${ENVIRONMENT}"
                sendEmail("Starting pipeline")
            }
        }

        stage('Copy local files') {
            when {
                equals expected: ENVIRONMENTS.LOCAL_DOCKER, actual: ENVIRONMENT
            }
            steps {
                sh 'cp -r /ow-forecasting-platform/owforecasting ./owforecasting'
                sh 'cp -r /ow-forecasting-platform/expected_results ./expected_results'
                sh 'cp -r "/ow-forecasting-platform/01 Raw data" "./01 Raw data"'
                sh 'cp -r "/ow-forecasting-platform/03 Processed data" "./03 Processed data"'
            }
        }

        stage('Download remote files') {
            when {
                equals expected: true, actual: ENVIRONMENT.DOWNLOAD_GITHUB_FILES
            }
            steps {
                withCredentials([usernamePassword(
                        credentialsId: ENVIRONMENT.GITHUB_CREDENTIAL,
                        passwordVariable: 'GITHUB_PASSWORD',
                        usernameVariable: 'GITHUB_USER'
                )]) {
                    sh """
                        set -eu  # Be careful, do not log credentials

                        echo 'Removing downloaded files of previous runs from workspace'
                        rm expected_results.zip owforecasting.zip anonymized_data_dsx.zip identifier.csv DSX_anonymized_input.csv.gz "01 Raw data/DSX_anonymized_input.csv.gz" || true

                        curl https://github.com/github/hub/releases/download/v2.14.2/hub-linux-amd64-2.14.2.tgz -o hub.tgz -L
                        tar -xzf hub.tgz
                        mv hub-linux-*/bin/hub .
                        rm -r hub-linux-* hub.tgz # Cleanup temporary files

                        export HUB_CONFIG=.hub
                        ./hub version
                        ./hub release download 'forecasting-platform-data'

                        mv DSX_anonymized_input.csv.gz "01 Raw data/DSX_anonymized_input.csv.gz"
                    """
                }
            }
        }

        stage('Create Artifacts') {
            steps {
                sh 'echo "Starting Create Artifacts"'
                sh './windows_installation/create_zip.sh'
            }
        }

        stage('Deploy to remote server') {
            steps {

                lock(ENVIRONMENT.NAME) {
                    sshPublisher(
                            failOnError: true,
                            continueOnError: false,
                            publishers: [
                                    sshPublisherDesc(
                                            configName: ENVIRONMENT.SSH_PUBLISH_CONFIG,
                                            transfers: generateSSHCommands(),
                                            verbose: true
                                    )
                            ]
                    )
                }
            }
        }
    }

    post {
        always {
            sendEmail(currentBuild.result)
        }
    }
}

def generateSSHCommands() {
    def commands = [
            sshTransfer(
                    execCommand: "whoami",
                    // This logs the remote user name to ensure that:
                    // - remote server is reachable (no timeouts due to firewalls)
                    // - remote server authentication works (valid SSH key)
                    // - we know the remote user for debugging purposes (e.g. file permission issues)
                    execTimeout: 10 * 1000,  // milliseconds
            ),
            sshTransfer(
                    sourceFiles: [
                            'windows_installation/ow-forecasting-install.zip',
                            'windows_installation/install_forecasting_platform.ps1',
                    ].join(","),
                    removePrefix: 'windows_installation',
                    remoteDirectory: "Desktop",
                    execCommand: "powershell -File Desktop/install_forecasting_platform.ps1",
                    execTimeout: 15 * 60 * 1000,  // milliseconds
            ),
            runForecast('info', 2),
    ]

    if (ENVIRONMENT.DROP_INTERNAL_DATABASE) {
        commands += [
                runForecast('setup-database internal --drop-tables', 2),
        ]
    } else {
        commands += [
                runForecast('setup-database internal', 2),
        ]
    }

    if (ENVIRONMENT.DROP_DSX_WRITE_DATABASE) {
        commands += [
                runForecast('setup-database dsx-write --drop-tables', 2),
        ]
    } else if (ENVIRONMENT.SETUP_DSX_WRITE_DATABASE) {
        commands += [
                runForecast('setup-database dsx-write', 2),
        ]
    }

    if (ENVIRONMENT.RUN_END_TO_END_TEST) {
        commands += [
                // run production command (includes cleaning) and verify against database
                runForecast('production --forecast-periods 1 --prediction-start-month 202001 --output-location test-results/production-run/', 30),
                runForecast('compare-structure-database', 2),

                // run backward forecast and verify against expected result files
                runForecast('backward --prediction-end-month 202003 --output-location test-results/backward-run/ --exclude-model-config ModelConfigAccount13 --forecast-periods 9', 60),
                runForecast('backward --prediction-end-month 202003 --output-location test-results/backward-run/ --only-model-config ModelConfigAccount13 --forecast-periods 6', 10),
                runForecast('compare-results expected_results/ test-results/backward-run/', 10),

                // run development forecast (includes cleaning and update of actuals) and verify against expected structure
                runForecast('development --forecast-periods 1 --prediction-start-month 202001 --output-location test-results/development-run/', 30),
                runForecast('compare-structure test-results/development-run/', 2),

                // verify if results of production command are still valid
                runForecast('compare-structure-database', 2),
        ]
    }

    if (ENVIRONMENT.RUN_PRODUCTION_FORECAST) {
        commands += [
                // TODO FSC-378 use --prediction-start-month default again when DSX database is fixed
                runForecast('production --prediction-start-month 202007', 120),
        ]
    }

    return commands
}

def runForecast(String cmd, int timeoutMinutes) {
    sshTransfer(
            execCommand: 'powershell -File Desktop/ow-forecasting-install/run_forecasting_platform.ps1 ' + cmd,
            execTimeout: timeoutMinutes * 60 * 1000,  // milliseconds
    )
}

def sendEmail(status) {
    emailext to: 'patrick.harbock@oliverwyman.com; pascal.wasser@oliverwyman.com; ivan.hanzlicek@oliverwyman.com',
            subject: "${status} ${currentBuild.fullDisplayName}",
            body: "Branch: ${env.BRANCH_NAME}\n" +
                    "Pipeline: ${env.BUILD_URL}\n" +
                    "Result: ${currentBuild.result}\n" +
                    "Runtime: ${currentBuild.durationString}\n\n" +
                    sh(script: 'git log HEAD~1..HEAD', returnStdout: true),
            attachLog: ENVIRONMENT.EMAIL_ATTACHMENT_LOG
}
