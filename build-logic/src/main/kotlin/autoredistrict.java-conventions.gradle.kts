plugins {
	`java-library`
}

val libs = the<VersionCatalogsExtension>().named("libs")

// Matches the Java module and package names (com.temotskipa.autoredistrict.<module>).
group = "com.temotskipa.autoredistrict"

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
	// GeoTools and JTS are automatic modules; requiring them is deliberate, so those lints stay off.
	options.compilerArgs.addAll(
		JvmFlags.preview +
				listOf("-Xlint:all,-preview,-serial,-processing,-this-escape,-requires-automatic,-requires-transitive-automatic")
	)
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
