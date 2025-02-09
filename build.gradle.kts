plugins {
    kotlin("jvm") version "2.1.0"
}

group = "dev.danielsantiago.kafka.streams.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    implementation("org.apache.kafka:kafka-clients:3.9.0")
    implementation("org.apache.kafka:kafka-streams:3.9.0")
    implementation("org.slf4j:slf4j-simple:1.7.36")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.18.2")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.18.2")

}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}