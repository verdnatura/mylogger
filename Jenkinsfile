#!/usr/bin/env groovy

pipeline {
    agent any
    options {
        disableConcurrentBuilds()
    }
    environment {
        PROJECT_NAME = 'mylogger'
        STACK_NAME   = "${env.PROJECT_NAME}-${env.BRANCH_NAME}"
    }
    stages {
        stage('Checkout') {
            steps {
                script {
                    def packageJson = readJSON file: 'package.json'
                    env.VERSION = packageJson.version
                }
                configFileProvider([
                    configFile(fileId: "mylogger.groovy",
                    variable: 'GROOVY_FILE')
                ]) {
                    load env.GROOVY_FILE
                }
                setEnv()
            }
        }
        stage('Build') {
            when {branch 'master'}
            environment {
                CREDENTIALS = credentials('docker-registry')
            }
            steps {
                sh 'docker login --username $CREDS_USR --password $CREDS_PSW $REGISTRY'
                sh 'docker-compose build --build-arg BUILD_ID=$BUILD_ID --parallel'
                sh 'docker-compose push'
            }
        }
        stage('Deploy') {
            when {branch 'master'}
            environment {
                DOCKER_HOST = "${env.SWARM_HOST}"
            }
            steps {
                sh "docker stack deploy --with-registry-auth --compose-file docker-compose.yml ${env.STACK_NAME}"
            }
        }
    }
    post {
        unsuccessful {
            sendEmail()
        }
    }
}
