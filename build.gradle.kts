plugins {
  kotlin("jvm") version "1.9.20"
  application
}

group = "org.itmo"
version = "1.0-SNAPSHOT"

repositories {
  mavenCentral()
}

dependencies {
  implementation("com.fasterxml.jackson.core:jackson-databind:2.16.1")
  implementation("edu.stanford.nlp:stanford-corenlp:4.5.5")
  implementation("edu.stanford.nlp:stanford-corenlp:4.5.5:models")
  implementation("edu.stanford.nlp:stanford-corenlp:4.5.5:models-english")
  implementation("edu.stanford.nlp:stanford-corenlp:4.5.5:models-english-kbp")
  implementation("javax.jms:javax.jms-api:2.0.1")
  implementation("org.apache.activemq:activemq-broker:6.1.1")
  implementation("com.rabbitmq:amqp-client:5.27.1")
  testImplementation(kotlin("test"))
}

tasks.test {
  useJUnitPlatform()
}

kotlin {
  jvmToolchain(21)
}

application {
  mainClass.set("MainKt")
}