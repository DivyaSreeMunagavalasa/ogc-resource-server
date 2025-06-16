pipeline {

  environment {
    devRegistry = 'ghcr.io/datakaveri/geoserver-dev'
    testRegistry = 'ghcr.io/datakaveri/geoserver-test:latest'
    registryUri = 'https://ghcr.io'
    registryCredential = 'datakaveri-ghcr'
    GIT_HASH = GIT_COMMIT.take(7)
  }

  agent { 
    node {
      label 'slave1'
    }
  }

  stages {

    stage('Build images') {
      steps{
        script {
          devImage = docker.build( devRegistry, "-f ./docker/dev.dockerfile .")
          testImage = docker.build( testRegistry, "-f ./docker/test.dockerfile .")
        }
      }
    }

    stage('Setup Server for Compliance Tests and Code Coverage Test'){
      steps{
        script{
          sh 'scp src/test/resources/OGC_compliance/compliance.xml jenkins@jenkins-master:/var/lib/jenkins/iudx/ogc/'
          sh 'docker compose -f docker-compose.test.yml up -d test'
          sh 'sleep 20'
        }
      }
      post{
        failure{
          script{
            sh 'docker compose -f docker-compose.test.yml down --remove-orphans'
          }
        }
      }
    }

    stage('Start OGC Feature Compliance Tests'){
      steps{
        node('built-in') {
          script{
            catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
          if (!fileExists('ets-ogcapi-features10')) {
            sh 'git clone https://github.com/opengeospatial/ets-ogcapi-features10'
          }
          dir('ets-ogcapi-features10') {
            if(!fileExists('target')) {
                sh 'mvn clean package -Dmaven.test.skip -Dmaven.javadoc.skip=true -Denforcer.skip=true -Dmaven.site.skip=true -Dspring-javaformat.skip=true'
            }
            sh 'java -jar target/ets-ogcapi-features10-1.10-SNAPSHOT-aio.jar --generateHtmlReport true /var/lib/jenkins/iudx/ogc/compliance.xml'
            }
            }
          }
        }
      }
      post{
        always{
          node('built-in') {
            script{
              catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                  env.NEWEST_TEST_DIR = sh(script: 'ls -t ~/testng | head -n1', returnStdout: true).trim()
                  sh 'cp -r ~/testng/${NEWEST_TEST_DIR} .'
                publishHTML([allowMissing: false, alwaysLinkToLastBuild: true, keepAll: true, reportDir: env.NEWEST_TEST_DIR, reportFiles: 'emailable-report.html,index.html', reportTitles: 'Overview,Detailed Report', reportName: 'OGC Feature Compliance Test Reports'])
              }
            }
          }
        }
      }
    }

    stage('Start STAC Compliance Tests'){
      steps{
        node('built-in') {
          script{
            catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
          if (!fileExists('stac-validator-venv')) {
            sh 'python3.10 -m venv stac-validator-venv'
          }
            sh '''
            . stac-validator-venv/bin/activate

            pip install stac-api-validator

            stac-api-validator \
            --root-url http://jenkins-slave1:8443/stac/ \
            --conformance core \
            --conformance collections \
            --conformance features \
            --conformance item-search \
            --geometry '{"type":"Polygon","coordinates":[[[75.777833,23.447842],[75.777833,30.184513],[87.335451,30.184513],[87.335451,23.447842],[75.777833,23.447842]]]}' \
            --collection 44da9cda-b00c-4481-be78-73b36038a7be > stacOutput.html
            '''
            }
          }
        }
      }
      post{
        always{
          node('built-in') {
            script{
              sh '''
                sed -i '1s/^/<!DOCTYPE html><html>/g' stacOutput.html
                sed -i 's|$|</br></br>|g' stacOutput.html
                echo '</html>' >> stacOutput.html
              '''
              if (!fileExists('stac-compliance-reports')) {
                sh 'mkdir stac-compliance-reports'
              } else {
                sh 'rm -rf stac-compliance-reports/*'
              }
              sh 'mv stacOutput.html stac-compliance-reports/'
              catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                publishHTML([allowMissing: false, alwaysLinkToLastBuild: true, keepAll: true, reportDir: 'stac-compliance-reports', reportFiles: 'stacOutput.html', reportTitles: 'STAC', reportName: 'STAC Compliance Test Reports'])
              }
            }
          }
        }
      }
    }

    stage('Run metering Junit tests and move JaCoCo data to /tmp/test'){
      steps{
        script{
          sh 'sudo rm -rf surefire-reports'
          sh 'docker-compose -f docker-compose.test.yml exec -T test mvn test -Dtest=Metering*'
          sh 'docker-compose -f docker-compose.test.yml exec -T test cp target/jacoco.exec /tmp/test/unit-test-jacoco.exec'
          sh 'docker-compose -f docker-compose.test.yml exec -T test cp -r target/surefire-reports /tmp/test/surefire-reports'
          sh 'docker-compose -f docker-compose.test.yml exec -T test chmod -R a+r /tmp/test/surefire-reports'
        }
      }
      post{
        failure{
          script{
            sh 'docker compose -f docker-compose.test.yml down --remove-orphans'
          }
        }
      }
    }

    stage('Extract class files and dump JaCoCo data from container'){
      steps{
        script{
          sh 'docker-compose -f docker-compose.test.yml exec -T test cp -r ./built-classes /tmp/test'
          sh 'docker-compose -f docker-compose.test.yml exec -T test java -jar /tmp/jacoco/lib/jacococli.jar dump --address 127.0.0.1 --port 57070 --destfile /tmp/test/jar-jacoco.exec'
        }
      }
      post{
        failure{
          script{
            sh 'docker compose -f docker-compose.test.yml down --remove-orphans'
          }
        }
      }
    }

    stage('Move data for Postman Integration Testing and Jmeter Test'){
      steps{
        script{
          sh 'scp Jmeter/OGCResourceServer.jmx jenkins@jenkins-master:/var/lib/jenkins/iudx/ogc/Jmeter/'
          sh 'scp src/test/resources/OGC_Resource_Server_v0.0.3_Release.postman_collection.json jenkins@jenkins-master:/var/lib/jenkins/iudx/ogc/Newman/'
        }
      }
      post{
        failure{
          script{
            sh 'docker compose -f docker-compose.test.yml down --remove-orphans'
          }
        }
      }
    }

    stage('Jmeter Performance Test'){
      steps{
        node('built-in') {
          script{
            sh 'rm -rf /var/lib/jenkins/iudx/ogc/Jmeter/report ; mkdir -p /var/lib/jenkins/iudx/ogc/Jmeter/report'
            sh "set +x;/var/lib/jenkins/apache-jmeter/bin/jmeter.sh -n -t /var/lib/jenkins/iudx/ogc/Jmeter/OGCResourceServer.jmx -l /var/lib/jenkins/iudx/ogc/Jmeter/report/JmeterTest.jtl -e -o /var/lib/jenkins/iudx/ogc/Jmeter/report/ -Jhost=jenkins-slave1"
          }
          perfReport filterRegex: '', showTrendGraphs: true, sourceDataFiles: '/var/lib/jenkins/iudx/ogc/Jmeter/report/*.jtl'     
        }
      }
      post{
        failure{
          script{
            sh 'docker compose -f docker-compose.test.yml  down --remove-orphans'
          }
        }
        cleanup{
          script{
            sh 'docker compose -f docker-compose.test.yml down --remove-orphans'
          }
        }
      }
    }
    
    stage('Start ogc-Resource-Server for RESTAssured Integration Testing'){
      steps{
        script{
          sh 'docker compose -f docker-compose.test.yml up -d integ-test'
          sh 'sleep 20'
        }
      }
      post{
        failure{
          script{
            sh 'docker compose -f docker-compose.test.yml down --remove-orphans'
          }
        }
      }
    }
    
    stage('RESTAssured Integration Tests and Surefire reports'){
      steps{
          script{
            sh 'rm -rf target/failsafe-reports'
            sh 'mkdir -p secrets && cp /home/ubuntu/configs/ogc-integration-test-db-config.properties secrets/integration-test-db-config.properties'
            sh 'mvn verify -DskipUnitTests=true -DskipBuildShadedJar=true -DintTestHost=localhost -DintTestPort=8443'
            }
        }
      post{
        always{
            script{
              catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
              xunit (
                  thresholds: [ skipped(failureThreshold: '0'), failed(failureThreshold: '0') ],
                  tools: [ JUnit(pattern: 'surefire-reports/*.xml') ]
                  )
              xunit (
                  thresholds: [ skipped(failureThreshold: '10'), failed(failureThreshold: '0') ],
                  tools: [ JUnit(pattern: 'target/failsafe-reports/*.xml') ]
                  )
              }
          }
        }
        failure{
          script{
            sh 'docker compose -f docker-compose.test.yml down --remove-orphans'
          }
        }
      }
    }
    
    stage('Extract class files, dump JaCoCo data from container and make JaCoCo report'){
      steps{
        script{
          sh 'docker-compose -f docker-compose.test.yml exec -T integ-test cp -r ./built-classes /tmp/test'
          sh 'docker-compose -f docker-compose.test.yml exec -T integ-test java -jar /tmp/jacoco/lib/jacococli.jar dump --address 127.0.0.1 --port 57070 --destfile /tmp/test/integ-jacoco.exec'
        }
        jacoco classPattern: 'built-classes', execPattern: '*-jacoco.exec'
      }
      post{
        cleanup{
          script{
            sh 'docker compose -f docker-compose.test.yml down --remove-orphans'
          } 
        }          
        failure{
          script{
            sh 'docker compose -f docker-compose.test.yml down --remove-orphans'
          }
        }
      }
    }

    stage('Start ogc-Resource-Server for Postman Integration Testing'){
      steps{
        script{
          sh 'docker compose -f docker-compose.test.yml up -d perfTest'
          sh 'sleep 20'
        }
      }
      post{
        failure{
          script{
            sh 'docker compose -f docker-compose.test.yml down --remove-orphans'
          }
        }
      }
    }

    stage('Postman Integration Tests and OWASP ZAP pen test'){
      steps{
        node('built-in') {
          script{
            startZap ([host: 'localhost', port: 8090, zapHome: '/var/lib/jenkins/tools/com.cloudbees.jenkins.plugins.customtools.CustomTool/OWASP_ZAP/ZAP_2.11.0'])
            sh 'curl http://127.0.0.1:8090/JSON/pscan/action/disableScanners/?ids=10096'
            sh 'curl http://jenkins-slave1:8443'
            catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
              sh 'HTTP_PROXY=\'127.0.0.1:8090\' newman run /var/lib/jenkins/iudx/ogc/Newman/OGC_Resource_Server_v0.0.3_Release.postman_collection.json -e /home/ubuntu/configs/ogc-postman-env.json --insecure -r htmlextra --reporter-htmlextra-export /var/lib/jenkins/iudx/ogc/Newman/report/report.html --reporter-htmlextra-skipSensitiveData'
              runZapAttack()
            }
          }
        }
      }
      post{
        always{
          node('built-in') {
            script{
              catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                publishHTML([allowMissing: false, alwaysLinkToLastBuild: true, keepAll: true, reportDir: '/var/lib/jenkins/iudx/ogc/Newman/report/', reportFiles: 'report.html', reportTitles: '', reportName: 'Integration Test Report'])
                archiveZap failHighAlerts: 1, failMediumAlerts: 3, failLowAlerts: 46
              }
            }
          }
        }
        cleanup{
          script{
            sh 'docker compose -f docker-compose.test.yml down --remove-orphans'
          } 
        }
      }
    }

    stage('Continuous Deployment') {
      when {
        allOf {
          anyOf {
            changeset "docker/**"
            changeset "docs/**"
            changeset "pom.xml"
            changeset "src/main/**"
            triggeredBy cause: 'UserIdCause'
          }
          expression {
            return env.GIT_BRANCH == 'origin/main';
          }
        }
      }
      stages {
        stage('Push Images') {
          steps {
            script {
              docker.withRegistry( registryUri, registryCredential ) {
                devImage.push("1.0.0-alpha-${env.GIT_HASH}")
              }
            }
          }
        }
        stage('Deploy ogc-resource-server') {
          steps{
            script{
              sh "ssh ubuntu@adex-swarm 'docker service update ogc-rs_ogc-rs --image ghcr.io/datakaveri/geoserver-dev:1.0.0-alpha-${env.GIT_HASH}'"
            }
          }
        }
      }
    }
  }
}

