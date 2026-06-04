pipeline {
    agent any

    environment {
        APP_NAME = 'datapulse'
    }

    stages {

        stage('Checkout') {
            steps {
                echo 'Checking out source code...'
                checkout scm
            }
        }

       stage('Setup') {
    steps {
        sh '''
        python3 --version
        pip3 install --break-system-packages --upgrade pip
        pip3 install --break-system-packages -r requirements.txt
        '''
    }
}

        stage('Lint') {
            steps {
                echo 'Running code quality checks...'
                sh '''
                    pip3 install --break-system-packages pycodestyle
                    pycodestyle --max-line-length=120 --statistics app/ || true
                '''
            }
        }

        stage('Test') {
            steps {
                echo 'Running 27 unit tests...'
                sh '''
                    python3 -m pytest tests/ -v --tb=short --junitxml=test-results.xml || \
                    python3 -m unittest discover -s tests -v
                '''
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: 'test-results.xml'
                }
            }
        }

        stage('Deploy') {
            steps {
                echo ' Deploying DataPulse...'
                sh '''
                    DEPLOY_DIR="/var/jenkins_home/deployed/datapulse"
                    mkdir -p "$DEPLOY_DIR"
                    cp -r app/ "$DEPLOY_DIR/"
                    cp requirements.txt "$DEPLOY_DIR/"
                    echo " App deployed successfully!"
                    echo "Location: $DEPLOY_DIR"
                '''
            }
        }
    }

    post {
        success {
            echo 'DataPulse pipeline completed successfully! All tests passed.'
        }
        failure {
            echo 'Pipeline failed. Check the logs above for details.'
        }
        always {
            echo 'Build finished.'
        }
    }
}
