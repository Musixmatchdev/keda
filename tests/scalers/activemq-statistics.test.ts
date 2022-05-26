import * as fs from 'fs'
import * as sh from 'shelljs'
import * as tmp from 'tmp'
import test from 'ava'
import {createNamespace, waitForRollout} from './helpers'

const activeMQStatisticsNamespace = 'activemq-statistics-test'
const activemqConf = '/opt/apache-activemq-5.16.3/conf'
const activemqData = '/opt/apache-activemq-5.16.3/data'
const activemqHome = '/opt/apache-activemq-5.16.3'
const activeMQStatisticsPath = 'bin/activemq'
const activeMQStatisticsUsername = 'admin'
const activeMQStatisticsPassword = 'admin'
const destinationName = 'testQ'
const consumerDeploymentName = 'consumer-activemqs-test-sj'

const produceMessages = 50
const targetQueueSize = 1
const maxReplicaCount = 10
const consumerSleep = 5
const timeoutConsumer = 600

test.before(t => {
	// install ActiveMQStatistics
  createNamespace(activeMQStatisticsNamespace)
	const activeMQStatisticsTmpFile = tmp.fileSync()
	fs.writeFileSync(activeMQStatisticsTmpFile.name, activeMQStatisticsDeployYaml)

	t.is(0, sh.exec(`kubectl apply --namespace ${activeMQStatisticsNamespace} -f ${activeMQStatisticsTmpFile.name}`).code, 'creating ActiveMQStatistics deployment should work.')
	t.is(0, waitForRollout('deployment', "activemq-statistics", activeMQStatisticsNamespace))

	const activeMQStatisticsPod = sh.exec(`kubectl get pods --selector=app=activemq-statistics-app -n ${activeMQStatisticsNamespace} -o jsonpath='{.items[0].metadata.name'}`).stdout

	let activeMQStatisticsReady
	for (let i = 0; i < 30; i++) {
		activeMQStatisticsReady = sh.exec(`kubectl exec -n ${activeMQStatisticsNamespace} ${activeMQStatisticsPod} -- curl -u ${activeMQStatisticsUsername}:${activeMQStatisticsPassword} -s http://0.0.0.0:8161/api/jolokia/exec/org.apache.activemq:type=Broker,brokerName=localhost,service=Health/healthStatus | sed -e 's/[{}]/''/g' | awk -v RS=',"' -F: '/^status/ {print $2}'`)
		if (activeMQStatisticsReady != 200) {
			sh.exec('sleep 5s')
		}
		else {
			break
		}
	}

  // ActiveMQStatistics ready check
	// deploy consumer, scaledjob etc.
	const consumerTmpFile = tmp.fileSync()
	fs.writeFileSync(consumerTmpFile.name, consumerDeployYaml)

	t.is(0, sh.exec(`kubectl apply --namespace ${activeMQStatisticsNamespace} -f ${consumerTmpFile.name}`).code, 'creating consumer ScaledJob should work.')
})

test.serial('ScaledJob should have 0 replicas on start', t => {
	const replicaCount = sh.exec(`kubectl get pods  --namespace ${activeMQStatisticsNamespace} --selector=scaledjob.keda.sh/name=${consumerDeploymentName} 2>/dev/null | grep Running | wc -l | tr -d '\n'`).stdout
	t.is(replicaCount, '0', 'replica count should start out as 0')
})

test.serial(`Jobs should scale up to a maximum of ${maxReplicaCount} and consume ${produceMessages} messages`, t => {
	const activeMQStatisticsPod = sh.exec(`kubectl get pods --selector=app=activemq-statistics-app -n ${activeMQStatisticsNamespace} -o jsonpath='{.items[0].metadata.name'}`).stdout

	// produce messages to ActiveMQ
	t.is(
		0,
		sh.exec(`kubectl exec -n ${activeMQStatisticsNamespace} ${activeMQStatisticsPod} -- ${activeMQStatisticsPath} producer --destination ${destinationName} --messageCount ${produceMessages} | grep INFO`).code,
		`produce ${produceMessages} message to the ActiveMQStatistics queue`
	)

	let replicaCount = '0'

	for (let i = 0; i < 30 && replicaCount === '0'; i++) {
		replicaCount = sh.exec(
      `kubectl get pods  --namespace ${activeMQStatisticsNamespace} --selector=scaledjob.keda.sh/name=${consumerDeploymentName} 2>/dev/null | grep Running | wc -l | tr -d '\n'`,
      { silent: true }
    ).stdout
    if (replicaCount === '0') {
			sh.exec('sleep 1s')
		}
	}
  t.not(replicaCount, '0', `Replica count should start after few seconds`)

  let queueSize = '-1'
	for (let i = 0; i < timeoutConsumer && queueSize !== '0'; i++) {
    queueSize = sh.exec(
      `kubectl exec -n ${activeMQStatisticsNamespace} ${activeMQStatisticsPod} -- ${activeMQStatisticsPath} query -QQueue=testQ --view QueueSize | grep QueueSize | awk '{print $3}' | tr -d '\n'`,
      { silent: true }
    ).stdout
		replicaCount = sh.exec(
			`kubectl get pods  --namespace ${activeMQStatisticsNamespace} --selector=scaledjob.keda.sh/name=${consumerDeploymentName} | grep Running | wc -l | tr -d '\n'`,
      { silent: true }
    ).stdout
    console.log(`Current queue size is ${queueSize} and scaledjob replicas is ${replicaCount}`)
    sh.exec('sleep 1')
	}
	t.is('0', queueSize, `Timeout reached (${timeoutConsumer} seconds). Queue size should be 0`)
  replicaCount = sh.exec(
    `kubectl get pods  --namespace ${activeMQStatisticsNamespace} --selector=scaledjob.keda.sh/name=${consumerDeploymentName} | grep Running | wc -l | tr -d '\n'`).stdout
	t.is('0', replicaCount, 'Replica count should be 0 after queue size is 0 (timeout: 15 minutes)')
})

test.after.always((t) => {
     t.is(0, sh.exec(`kubectl delete namespace ${activeMQStatisticsNamespace}`).code, 'Should delete ActiveMQStatistics namespace')
})

const activeMQStatisticsDeployYaml = `
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    app: activemq-statistics-app
  name: activemq-statistics
spec:
  replicas: 1
  revisionHistoryLimit: 10
  selector:
    matchLabels:
      app: activemq-statistics-app
  template:
    metadata:
      labels:
        app: activemq-statistics-app
    spec:
      containers:
      - image: symptoma/activemq:5.16.3
        imagePullPolicy: IfNotPresent
        name: activemq-statistics
        ports:
        - containerPort: 61616
          name: jmx
          protocol: TCP
        - containerPort: 8161
          name: ui
          protocol: TCP
        - containerPort: 61616
          name: openwire
          protocol: TCP
        - containerPort: 5672
          name: amqp
          protocol: TCP
        - containerPort: 61613
          name: stomp
          protocol: TCP
        - containerPort: 1883
          name: mqtt
          protocol: TCP
        resources:
        volumeMounts:
        - name: activemq-statistics-config
          mountPath: /opt/apache-activemq-5.16.3/conf/activemq.xml
          subPath: activemq.xml
        - name: activemq-statistics-config
          mountPath: /opt/apache-activemq-5.16.3/webapps/api/WEB-INF/classes/jolokia-access.xml
          subPath: jolokia-access.xml
        - name: remote-access-cm
          mountPath: /opt/apache-activemq-5.16.3/conf/jetty.xml
          subPath: jetty.xml
      volumes:
      - name: activemq-statistics-config
        configMap:
          name: activemq-statistics-config
          items:
          - key: activemq.xml
            path: activemq.xml
          - key: jolokia-access.xml
            path: jolokia-access.xml
      - name: remote-access-cm
        configMap:
          name: remote-access-cm
          items:
          - key: jetty.xml
            path: jetty.xml
---
apiVersion: v1
kind: Service
metadata:
  name: activemq-statistics
spec:
  type: ClusterIP
  selector:
    app: activemq-statistics-app
  ports:
  - name: dashboard
    port: 8161
    targetPort: 8161
    protocol: TCP
  - name: openwire
    port: 61616
    targetPort: 61616
    protocol: TCP
  - name: amqp
    port: 5672
    targetPort: 5672
    protocol: TCP
  - name: stomp
    port: 61613
    targetPort: 61613
    protocol: TCP
  - name: mqtt
    port: 1883
    targetPort: 1883
    protocol: TCP
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: activemq-statistics-config
data:
  activemq.xml: |
    <!--
      Licensed to the Apache Software Foundation (ASF) under one or more
      contributor license agreements.  See the NOTICE file distributed with
      this work for additional information regarding copyright ownership.
      The ASF licenses this file to You under the Apache License, Version 2.0
      (the "License"); you may not use this file except in compliance with
      the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

      Unless required by applicable law or agreed to in writing, software
      distributed under the License is distributed on an "AS IS" BASIS,
      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
      See the License for the specific language governing permissions and
      limitations under the License.
    -->
    <!-- START SNIPPET: example -->
    <beans
    xmlns="http://www.springframework.org/schema/beans"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
    http://activemq.apache.org/schema/core http://activemq.apache.org/schema/core/activemq-core.xsd">

      <!-- Allows us to use system properties as variables in this configuration file -->
      <bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer">
          <property name="locations">
              <value>file:${activemqConf}/credentials.properties</value>
          </property>
      </bean>

    <!-- Allows accessing the server log -->
      <bean id="logQuery" class="io.fabric8.insight.log.log4j.Log4jLogQuery"
            lazy-init="false" scope="singleton"
            init-method="start" destroy-method="stop">
      </bean>

      <!--
          The <broker> element is used to configure the ActiveMQ broker.
      -->
      <broker xmlns="http://activemq.apache.org/schema/core" brokerName="localhost" dataDirectory="${activemqData}">
          <plugins>
              <statisticsBrokerPlugin/>
          </plugins>

          <destinationPolicy>
              <policyMap>
                <policyEntries>
                  <policyEntry topic=">" >
                      <!-- The constantPendingMessageLimitStrategy is used to prevent
                          slow topic consumers to block producers and affect other consumers
                          by limiting the number of messages that are retained
                          For more information, see:

                          http://activemq.apache.org/slow-consumer-handling.html

                      -->
                    <pendingMessageLimitStrategy>
                      <constantPendingMessageLimitStrategy limit="1000"/>
                    </pendingMessageLimitStrategy>
                  </policyEntry>
                </policyEntries>
              </policyMap>
          </destinationPolicy>


          <!--
              The managementContext is used to configure how ActiveMQ is exposed in
              JMX. By default, ActiveMQ uses the MBean server that is started by
              the JVM. For more information, see:

              http://activemq.apache.org/jmx.html
          -->
          <managementContext>
              <managementContext createConnector="false"/>
          </managementContext>

          <!--
              Configure message persistence for the broker. The default persistence
              mechanism is the KahaDB store (identified by the kahaDB tag).
              For more information, see:

              http://activemq.apache.org/persistence.html
          -->
          <persistenceAdapter>
              <kahaDB directory="${activemqData}/kahadb"/>
          </persistenceAdapter>


            <!--
              The systemUsage controls the maximum amount of space the broker will
              use before disabling caching and/or slowing down producers. For more information, see:
              http://activemq.apache.org/producer-flow-control.html
            -->
            <systemUsage>
              <systemUsage>
                  <memoryUsage>
                      <memoryUsage percentOfJvmHeap="70" />
                  </memoryUsage>
                  <storeUsage>
                      <storeUsage limit="100 gb"/>
                  </storeUsage>
                  <tempUsage>
                      <tempUsage limit="50 gb"/>
                  </tempUsage>
              </systemUsage>
          </systemUsage>

          <!--
              The transport connectors expose ActiveMQ over a given protocol to
              clients and other brokers. For more information, see:

              http://activemq.apache.org/configuring-transports.html
          -->
          <transportConnectors>
              <!-- DOS protection, limit concurrent connections to 1000 and frame size to 100MB -->
              <transportConnector name="openwire" uri="tcp://0.0.0.0:61616?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
              <transportConnector name="amqp" uri="amqp://0.0.0.0:5672?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
              <transportConnector name="stomp" uri="stomp://0.0.0.0:61613?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
              <transportConnector name="mqtt" uri="mqtt://0.0.0.0:1883?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
              <transportConnector name="ws" uri="ws://0.0.0.0:61614?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
          </transportConnectors>

          <!-- destroy the spring context on shutdown to stop jetty -->
          <shutdownHooks>
              <bean xmlns="http://www.springframework.org/schema/beans" class="org.apache.activemq.hooks.SpringContextHook" />
          </shutdownHooks>

      </broker>

      <!--
          Enable web consoles, REST and Ajax APIs and demos
          The web consoles requires by default login, you can disable this in the jetty.xml file

          Take a look at ${activemqHome}/conf/jetty.xml for more details
      -->
      <import resource="jetty.xml"/>

    </beans>
    <!-- END SNIPPET: example -->

  jolokia-access.xml: |
    <?xml version="1.0" encoding="UTF-8"?>
    <restrict>
      <remote>
        <host>0.0.0.0/0</host>
      </remote>

      <deny>
        <mbean>
          <name>com.sun.management:type=DiagnosticCommand</name>
          <attribute>*</attribute>
          <operation>*</operation>
        </mbean>
        <mbean>
          <name>com.sun.management:type=HotSpotDiagnostic</name>
          <attribute>*</attribute>
          <operation>*</operation>
        </mbean>
      </deny>
    </restrict>
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: remote-access-cm
data:
  jetty.xml: |
    <!--
        Licensed to the Apache Software Foundation (ASF) under one or more contributor
        license agreements. See the NOTICE file distributed with this work for additional
        information regarding copyright ownership. The ASF licenses this file to You under
        the Apache License, Version 2.0 (the "License"); you may not use this file except in
        compliance with the License. You may obtain a copy of the License at
        http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or
        agreed to in writing, software distributed under the License is distributed on an
        "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
        implied. See the License for the specific language governing permissions and
        limitations under the License.
    -->
    <!--
        An embedded servlet engine for serving up the Admin consoles, REST and Ajax APIs and
        some demos Include this file in your configuration to enable ActiveMQStatistics web components
        e.g. <import resource="jetty.xml"/>
    -->
    <beans xmlns="http://www.springframework.org/schema/beans" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd">

    <bean id="httpConfig" class="org.eclipse.jetty.server.HttpConfiguration">
        <property name="sendServerVersion" value="false"/>
    </bean>

    <bean id="securityLoginService" class="org.eclipse.jetty.security.HashLoginService">
        <property name="name" value="ActiveMQStatisticsRealm" />
        <property name="config" value="${activemqConf}/jetty-realm.properties" />
    </bean>

    <bean id="securityConstraint" class="org.eclipse.jetty.util.security.Constraint">
        <property name="name" value="BASIC" />
        <property name="roles" value="user,admin" />
        <!-- set authenticate=false to disable login -->
        <property name="authenticate" value="true" />
    </bean>
    <bean id="adminSecurityConstraint" class="org.eclipse.jetty.util.security.Constraint">
        <property name="name" value="BASIC" />
        <property name="roles" value="admin" />
         <!-- set authenticate=false to disable login -->
        <property name="authenticate" value="true" />
    </bean>
    <bean id="securityConstraintMapping" class="org.eclipse.jetty.security.ConstraintMapping">
        <property name="constraint" ref="securityConstraint" />
        <property name="pathSpec" value="/*,/api/*,/admin/*,*.jsp" />
    </bean>
    <bean id="adminSecurityConstraintMapping" class="org.eclipse.jetty.security.ConstraintMapping">
        <property name="constraint" ref="adminSecurityConstraint" />
        <property name="pathSpec" value="*.action" />
    </bean>

    <bean id="rewriteHandler" class="org.eclipse.jetty.rewrite.handler.RewriteHandler">
        <property name="rules">
            <list>
                <bean id="header" class="org.eclipse.jetty.rewrite.handler.HeaderPatternRule">
                  <property name="pattern" value="*"/>
                  <property name="name" value="X-FRAME-OPTIONS"/>
                  <property name="value" value="SAMEORIGIN"/>
                </bean>
                <bean id="header" class="org.eclipse.jetty.rewrite.handler.HeaderPatternRule">
                  <property name="pattern" value="*"/>
                  <property name="name" value="X-XSS-Protection"/>
                  <property name="value" value="1; mode=block"/>
                </bean>
                <bean id="header" class="org.eclipse.jetty.rewrite.handler.HeaderPatternRule">
                  <property name="pattern" value="*"/>
                  <property name="name" value="X-Content-Type-Options"/>
                  <property name="value" value="nosniff"/>
                </bean>
            </list>
        </property>
    </bean>

    <bean id="secHandlerCollection" class="org.eclipse.jetty.server.handler.HandlerCollection">
        <property name="handlers">
            <list>
            <ref bean="rewriteHandler"/>
                <bean class="org.eclipse.jetty.webapp.WebAppContext">
                    <property name="contextPath" value="/admin" />
                    <property name="resourceBase" value="${activemqHome}/webapps/admin" />
                    <property name="logUrlOnStart" value="true" />
                </bean>
                <bean class="org.eclipse.jetty.webapp.WebAppContext">
                    <property name="contextPath" value="/api" />
                    <property name="resourceBase" value="${activemqHome}/webapps/api" />
                    <property name="logUrlOnStart" value="true" />
                </bean>
                <bean class="org.eclipse.jetty.server.handler.ResourceHandler">
                    <property name="directoriesListed" value="false" />
                    <property name="welcomeFiles">
                        <list>
                            <value>index.html</value>
                        </list>
                    </property>
                    <property name="resourceBase" value="${activemqHome}/webapps/" />
                </bean>
                <bean id="defaultHandler" class="org.eclipse.jetty.server.handler.DefaultHandler">
                    <property name="serveIcon" value="false" />
                </bean>
            </list>
        </property>
    </bean>
    <bean id="securityHandler" class="org.eclipse.jetty.security.ConstraintSecurityHandler">
        <property name="loginService" ref="securityLoginService" />
        <property name="authenticator">
            <bean class="org.eclipse.jetty.security.authentication.BasicAuthenticator" />
        </property>
        <property name="constraintMappings">
            <list>
                <ref bean="adminSecurityConstraintMapping" />
                <ref bean="securityConstraintMapping" />
            </list>
        </property>
        <property name="handler" ref="secHandlerCollection" />
    </bean>

    <bean id="contexts" class="org.eclipse.jetty.server.handler.ContextHandlerCollection">
    </bean>

    <bean id="jettyPort" class="org.apache.activemq.web.WebConsolePort" init-method="start">
             <!-- the default port number for the web console -->
        <property name="host" value="0.0.0.0"/>
        <property name="port" value="8161"/>
    </bean>

    <bean id="Server" depends-on="jettyPort" class="org.eclipse.jetty.server.Server"
        destroy-method="stop">

        <property name="handler">
            <bean id="handlers" class="org.eclipse.jetty.server.handler.HandlerCollection">
                <property name="handlers">
                    <list>
                        <ref bean="contexts" />
                        <ref bean="securityHandler" />
                    </list>
                </property>
            </bean>
        </property>

    </bean>

    <bean id="invokeConnectors" class="org.springframework.beans.factory.config.MethodInvokingFactoryBean">
    <property name="targetObject" ref="Server" />
    <property name="targetMethod" value="setConnectors" />
    <property name="arguments">
    <list>
               <bean id="Connector" class="org.eclipse.jetty.server.ServerConnector">
                   <constructor-arg ref="Server"/>
                   <constructor-arg>
                       <list>
                           <bean id="httpConnectionFactory" class="org.eclipse.jetty.server.HttpConnectionFactory">
                               <constructor-arg ref="httpConfig"/>
                           </bean>
                       </list>
                   </constructor-arg>
                   <!-- see the jettyPort bean -->
                   <property name="host" value="#{systemProperties['jetty.host']}" />
                   <property name="port" value="#{systemProperties['jetty.port']}" />
               </bean>
                <!--
                    Enable this connector if you wish to use https with web console
                -->
                <!-- bean id="SecureConnector" class="org.eclipse.jetty.server.ServerConnector">
                    <constructor-arg ref="Server" />
                    <constructor-arg>
                        <bean id="handlers" class="org.eclipse.jetty.util.ssl.SslContextFactory">

                            <property name="keyStorePath" value="${activemqConf}/broker.ks" />
                            <property name="keyStorePassword" value="password" />
                        </bean>
                    </constructor-arg>
                    <property name="port" value="8162" />
                </bean -->
            </list>
    </property>
    </bean>

    <bean id="configureJetty" class="org.springframework.beans.factory.config.MethodInvokingFactoryBean">
        <property name="staticMethod" value="org.apache.activemq.web.config.JspConfigurer.configureJetty" />
        <property name="arguments">
            <list>
                <ref bean="Server" />
                <ref bean="secHandlerCollection" />
            </list>
        </property>
    </bean>

    <bean id="invokeStart" class="org.springframework.beans.factory.config.MethodInvokingFactoryBean"
    depends-on="configureJetty, invokeConnectors">
    <property name="targetObject" ref="Server" />
    <property name="targetMethod" value="start" />
    </bean>
    </beans>
`
const consumerDeployYaml = `
apiVersion: v1
kind: Secret
metadata:
  name: activemq-statistics-secret
type: Opaque
data:
  activemq-statistics-password: YWRtaW4=
  activemq-statistics-username: YWRtaW4=
---
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: trigger-auth-activemq-statistics
spec:
  secretTargetRef:
    - parameter: username
      name: activemq-statistics-secret
      key: activemq-statistics-username
    - parameter: password
      name: activemq-statistics-secret
      key: activemq-statistics-password
---
apiVersion: keda.sh/v1alpha1
kind: ScaledJob
metadata:
  name: ${consumerDeploymentName}
  labels:
    deploymentName: ${consumerDeploymentName}
spec:
  jobTargetRef:
    template:
      metadata:
        labels:
          app: ${consumerDeploymentName}
      spec:
        containers:
          - name: ${consumerDeploymentName}
            image: symptoma/activemq:5.16.3
            imagePullPolicy: IfNotPresent
            args:
              - |
                  sleep ${consumerSleep} && \
                  ${activeMQStatisticsPath} consumer --brokerUrl tcp://activemq-statistics.${activeMQStatisticsNamespace}.svc.cluster.local:61616 \
                            --user admin --password admin --destination ${destinationName} --ackMode AUTO_ACKNOWLEDGE \
                            --messageCount 1
            command:
              - sh
              - -c
            resources:
              requests:
                cpu: 50m
                memory: 50Mi
        restartPolicy: Never
  rolloutStrategy: gradual
  failedJobsHistoryLimit: 1
  successfulJobsHistoryLimit: 1
  pollingInterval: 1
  maxReplicaCount: ${maxReplicaCount}
  triggers:
    - type: activemq-statistics
      metadata:
        endpoint: "activemq-statistics.${activeMQStatisticsNamespace}.svc.cluster.local:61613"
        destinationName: "testQ"
        stompSSL: "false"
        targetQueueSize: "${targetQueueSize}"
      authenticationRef:
        name: trigger-auth-activemq-statistics
`
