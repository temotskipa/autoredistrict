plugins {
	id("autoredistrict.java-conventions")
	application
}

// Each application sets mainModule and mainClass; the run task and the start scripts then use
// --module-path and --module. Every dependency is an explicit module (see java-conventions for JTS).
application {
	applicationDefaultJvmArgs = JvmFlags.launch
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
				$$"JAVA_HOME=\"${AUTOREDISTRICT_JAVA_HOME:-$$unixHome}\"\n\n# Determine the Java command to use",
			).replace("__APP_HOME__", $$"'\"$APP_HOME\"'")
		)
	}
}

// jlink links the app's modules and the JDK modules they need into one runtime image with the launch
// flags built in (build/jlink, started with bin/<name>). jpackage turns that image into a native app
// folder (jpackageImage, build/jpackage/<name>) or an installer (jpackageInstaller, build/installer):
// msi on Windows, which needs the WiX toolset (v3, or v4+ with the UI and Util extensions), dmg on
// macOS, deb on Linux. -PinstallerType picks another type (exe, pkg, rpm) and -PappVersion sets the
// version (default 1.0). Each app adds its own options, such as its Start menu entry.
val jlinkImage = tasks.register<Exec>("jlinkImage") {
	group = "distribution"
	description = "Links a runtime image with the app's modules and the JDK modules they need."
	val jar = tasks.named<Jar>("jar").flatMap { it.archiveFile }
	val runtime = configurations.named("runtimeClasspath")
	val image = layout.buildDirectory.dir("jlink")
	inputs.file(jar)
	inputs.files(runtime)
	inputs.property("toolchainHome", toolchainHome)
	inputs.property("jvmArgs", provider { application.applicationDefaultJvmArgs.toList() })
	outputs.dir(image)
	doFirst {
		image.get().asFile.deleteRecursively()
		executable = File(toolchainHome.get(), "bin/jlink").absolutePath
	}
	argumentProviders.add(CommandLineArgumentProvider {
		val module = application.mainModule.get()
		listOf(
			"--module-path", (listOf(jar.get().asFile) + runtime.get().files).joinToString(File.pathSeparator),
			"--add-modules", module,
			"--launcher", "${application.applicationName}=$module/${application.mainClass.get()}",
			// One argument: jlink takes a separate value that starts with "--" for an option.
			"--add-options=" + application.applicationDefaultJvmArgs.joinToString(" "),
			"--strip-debug", "--no-header-files", "--no-man-pages", "--compress", "zip-6",
			"--output", image.get().asFile.absolutePath,
		)
	})
}

val installerType: Provider<String> = providers.gradleProperty("installerType").orElse(provider {
	val os = System.getProperty("os.name").lowercase()
	when {
		os.startsWith("windows") -> "msi"
		os.startsWith("mac") -> "dmg"
		else -> "deb"
	}
})
val appVersion: Provider<String> = providers.gradleProperty("appVersion").orElse("1.0")

fun jpackage(name: String, about: String, type: Provider<String>, folder: String) = tasks.register<Exec>(name) {
	group = "distribution"
	description = about
	val image = jlinkImage.map { it.outputs.files.singleFile }
	val dest = layout.buildDirectory.dir(folder)
	inputs.dir(image)
	inputs.property("type", type)
	inputs.property("version", appVersion)
	outputs.dir(dest)
	doFirst {
		dest.get().asFile.deleteRecursively()
		executable = File(toolchainHome.get(), "bin/jpackage").absolutePath
	}
	argumentProviders.add(CommandLineArgumentProvider {
		listOf(
			"--type", type.get(),
			"--name", application.applicationName,
			"--app-version", appVersion.get(),
			"--vendor", "autoredistrict",
			"--dest", dest.get().asFile.absolutePath,
			"--runtime-image", image.get().absolutePath,
			"--module", "${application.mainModule.get()}/${application.mainClass.get()}",
		)
	})
}

jpackage("jpackageImage", "Wraps the jlink runtime image as a native app folder.", provider { "app-image" }, "jpackage")
jpackage("jpackageInstaller", "Builds a native installer (msi, dmg or deb) from the jlink runtime image.", installerType, "installer")
