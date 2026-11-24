plugins {
	id("autoredistrict.java-conventions")
	application
}

// Each application sets mainModule and mainClass; the run task and the start scripts then use
// --module-path and --module. Jars that are not modules (a few GeoTools dependencies) stay on the
// class path, where the automatic GeoTools modules can read them.
application {
	applicationDefaultJvmArgs = JvmFlags.app
}

// With mainModule set, Gradle also writes the main class into module-info.class, using its bundled
// ASM, which can't read Java 28 class files ("Unsupported class file major version 72"). Not needed:
// the run task and the start scripts pass --module <module>/<main class>.
tasks.named<JavaCompile>("compileJava") {
	options.javaModuleMainClass.unsetConvention()
}

// `gradlew :cli:run` and `:gui:run` start in the repository root, so .cache and out/ resolve there.
tasks.named<JavaExec>("run") {
	workingDir = rootDir
}

// The start scripts run on the JDK 28 EA toolchain, not whatever JAVA_HOME points at
// (here Temurin 25, which can't load preview class files). AUTOREDISTRICT_JAVA_HOME overrides it.
val toolchainHome = javaToolchains.launcherFor(java.toolchain).map {
	it.metadata.installationPath.asFile.absolutePath
}

tasks.named<CreateStartScripts>("startScripts") {
	inputs.property("toolchainHome", toolchainHome)
	doLast {
		val home = toolchainHome.get()
		val bat = windowsScript.readText()
		windowsScript.writeText(
			bat.replace(
				"@rem Find java.exe",
				"if defined AUTOREDISTRICT_JAVA_HOME (set \"JAVA_HOME=%AUTOREDISTRICT_JAVA_HOME%\") " +
						"else (set \"JAVA_HOME=$home\")\r\n\r\n@rem Find java.exe",
			).replace("__APP_HOME__", "%APP_HOME%")
		)
		val unixHome = home.replace('\\', '/')
		val sh = unixScript.readText()
		unixScript.writeText(
			sh.replaceFirst(
				"# Determine the Java command to use",
				"JAVA_HOME=\"\${AUTOREDISTRICT_JAVA_HOME:-$unixHome}\"\n\n# Determine the Java command to use",
			).replace("__APP_HOME__", "'\"\$APP_HOME\"'")
		)
	}
}
