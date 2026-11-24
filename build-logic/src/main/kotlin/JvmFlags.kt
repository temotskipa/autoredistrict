/** JVM flags shared by every compile, test, run and start script, so no module misses one. */
object JvmFlags {
	/** JDK 28 preview features: value classes (JEP 401) and lazy constants. */
	val preview = listOf("--enable-preview")

	/** ZGC with adaptive heap sizing (JEP 546) and no -Xmx; the flag is off by default in the EA build. Tests use these. */
	val launch = preview + listOf(
		"-XX:+UseZGC", "-XX:+ZAdaptiveHeapSizing",
		// GeoTools 35 calls sun.misc.Unsafe::invokeCleaner when it releases shapefile buffers.
		"--sun-misc-unsafe-memory-access=allow",
	)

	/**
	 * The applications run as named modules on the module path. GeoTools' jars are automatic
	 * modules that declare no requires, so the explicit modules they need (units of measure,
	 * Apache Commons) are resolved by adding every module on the path.
	 */
	val app = launch + listOf("--add-modules=ALL-MODULE-PATH")

	/** JavaFX loads its native libraries (a restricted method since JDK 24). */
	val gui = app + listOf("--enable-native-access=javafx.graphics")
}
