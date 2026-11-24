plugins {
	`java-library`
}

val libs = the<VersionCatalogsExtension>().named("libs")

repositories {
	mavenCentral()
	maven {
		name = "osgeo"
		url = uri("https://repo.osgeo.org/repository/release/")
		// GeoTools and the few of its dependencies that aren't on Maven Central.
		content {
			includeGroupByRegex("org[.]geotools.*")
			includeGroup("it.geosolutions.jgridshift")
			includeGroup("org.apache.xml")
		}
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

tasks.withType<Test>().configureEach {
	useJUnitPlatform()
	jvmArgs(JvmFlags.launch)
	testLogging {
		events("failed", "skipped")
		exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
	}
}

tasks.withType<JavaExec>().configureEach {
	jvmArgs(JvmFlags.launch)
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
