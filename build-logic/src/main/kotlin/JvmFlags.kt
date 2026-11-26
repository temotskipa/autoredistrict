/** JVM flags shared by every compile, test, run and start script, so no module misses one. */
object JvmFlags {
	/** JDK 28 preview features: value classes (JEP 401) and lazy constants. */
	val preview = listOf("--enable-preview")

	/**
	 * ZGC with adaptive heap sizing (JEP 546) and no -Xmx; the flag is off by default in the EA build.
	 * Tests and both applications use these.
	 */
	val launch = preview + listOf("-XX:+UseZGC", "-XX:+ZAdaptiveHeapSizing")

	/** JavaFX loads its native libraries (a restricted method since JDK 24). */
	val gui = launch + listOf("--enable-native-access=javafx.graphics")
}
