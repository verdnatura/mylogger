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
                setEnv()
            }
        }
        stage('Build') {
            when {branch 'master'}
            environment {
                IMAGE = 'registry.verdnatura.es/mylogger'
                TAG = env.VERSION
                CREDENTIALS = credentials('docker-registry')
            }
            steps {
                sh 'docker login --username $CREDENTIALS_USR --password $CREDENTIALS_PSW $REGISTRY'
                sh 'docker-compose build --build-arg BUILD_ID=$BUILD_ID --parallel'
                sh 'docker-compose push'
                
                sh 'docker tag $IMAGE:$TAG $IMAGE:latest'
                sh 'docker push $IMAGE:latest'
            }
        }
    }
    post {
        unsuccessful {
            sendEmail()
        }
    }
}
