/** JVM flags shared by every compile, test, run and start script, so no module misses one. */
object JvmFlags {
	/** JDK 28 preview features: value classes (JEP 401) and lazy constants. */
	val preview = listOf("--enable-preview")

	/** ZGC with adaptive heap sizing (JEP 546) and no -Xmx; the flag is off by default in the EA build. */
	val launch = preview + listOf(
		"-XX:+UseZGC", "-XX:+ZAdaptiveHeapSizing",
		// GeoTools 35 calls sun.misc.Unsafe::invokeCleaner when it releases shapefile buffers.
		"--sun-misc-unsafe-memory-access=allow",
	)

	/** JavaFX loads its native libraries from the class path. */
	val gui = launch + listOf("--enable-native-access=ALL-UNNAMED")
}
