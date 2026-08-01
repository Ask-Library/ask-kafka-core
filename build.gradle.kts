import org.gradle.external.javadoc.StandardJavadocDocletOptions

plugins {
	alias(libs.plugins.java.library)
	alias(libs.plugins.maven.publish)
	alias(libs.plugins.jacoco)
	alias(libs.plugins.freefair.lombok)
}

group = "pe.ask"
version = "1.0.0"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(21)
	}
}

jacoco {
	toolVersion = "0.8.12"
}

repositories {
	mavenCentral()
	maven { url = uri("https://jitpack.io") }
}

dependencies {
	api(platform(libs.spring.boot.dependencies))
	implementation(libs.spring.boot.autoconfigure)
	implementation(libs.spring.boot.starter.validation)

	implementation(libs.project.reactor)
	implementation(libs.reactor.kafka)

	implementation(libs.ask.core)
	implementation(libs.ask.exception.core)

	implementation(libs.jackson.core)
	implementation(libs.jackson.databind)

	annotationProcessor(libs.spring.boot.configuration.processor)
	annotationProcessor(libs.spring.boot.autoconfigure.processor)

	testRuntimeOnly(libs.junit.platform.launcher)
	testImplementation(libs.project.reactor.test)
}

publishing {
	publications {
		create<MavenPublication>("mavenJava") {
			from(components["java"])

			pom {
				name.set("Ask Kafka Core")
				description.set(
					"Reactive Kafka core library for producing, consuming, " +
							"dispatching, and auto-configuring Kafka messages with Spring Boot and Reactor Kafka."
				)
				url.set("https://github.com/Ask-Library/ask-kafka-core")

				licenses {
					license {
						name.set("The Apache License, Version 2.0")
						url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
						distribution.set("repo")
					}
				}

				developers {
					developer {
						id.set("AllanSagastegui")
						name.set("Allan Sagastegui")
						email.set("allxn.sxh@gmail.com")
						url.set("https://github.com/AllanSagastegui")
					}
				}
			}
		}
	}
}

tasks.test {
	useJUnitPlatform()
	finalizedBy(tasks.jacocoTestReport)
}

tasks.jacocoTestReport {
	dependsOn(tasks.test)

	reports {
		xml.required.set(true)
		html.required.set(true)
	}
}

tasks.withType<Javadoc>().configureEach {
	isFailOnError = false

	(options as StandardJavadocDocletOptions).apply {
		addStringOption("Xdoclint:none", "-quiet")
		encoding = "UTF-8"
		charSet = "UTF-8"
		docEncoding = "UTF-8"
	}
}