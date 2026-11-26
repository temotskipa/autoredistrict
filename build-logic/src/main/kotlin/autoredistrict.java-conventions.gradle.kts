plugins {
	`java-library`
	id("org.gradlex.extra-java-module-info")
}

val libs = the<VersionCatalogsExtension>().named("libs")

// Matches the Java module and package names (com.temotskipa.autoredistrict.<module>).
group = "com.temotskipa.autoredistrict"

repositories {
	mavenCentral()
}

// JTS is published without a module-info, which makes it an automatic module, and jlink only links
// explicit modules. This adds one to its jar on every class path; every other dependency is a module.
extraJavaModuleInfo {
	module("org.locationtech.jts:jts-core", "org.locationtech.jts") {
		exportAllPackages()
		// Its awt and util packages draw with java.awt.
		requires("java.desktop")
	}
}

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(28)
	}
}

tasks.withType<JavaCompile>().configureEach {
	options.encoding = "UTF-8"
	options.release = 28
	options.compilerArgs.addAll(JvmFlags.preview + listOf("-Xlint:all,-preview,-serial,-processing,-this-escape"))
}

// Tests have no module-info, so they run on the class path (white-box, the main module-info is ignored).
tasks.withType<Test>().configureEach {
	useJUnitPlatform()
	jvmArgs(JvmFlags.launch)
	testLogging {
		events("failed", "skipped")
		exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
	}
}

tasks.withType<Jar>().configureEach {
	// Reproducible jars keep the AOT cache valid across rebuilds with unchanged code.
	isPreserveFileTimestamps = false
	isReproducibleFileOrder = true
}

dependencies {
	"testImplementation"(platform(libs.findLibrary("junit-bom").get()))
	"testImplementation"(libs.findLibrary("junit-jupiter").get())
	"testRuntimeOnly"(libs.findLibrary("junit-launcher").get())
}
