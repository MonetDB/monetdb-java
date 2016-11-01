/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded;

/**
 * The MonetDB's JNI library loader for Java.
 * <br/>
 * <strong>Note</strong>: The MonetDB's JNI library must be successfully loaded in order to the other methods work.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class MonetDBEmbeddedInstance {

    private static boolean isEmbeddedInstanceInitialized = false;

    private static final String NATIVE_LIB_NAME = "monetdb5";

    /**
     * Tries to load the JNI library with MonetDBLite from the current Java Classpath.
     *
     * @param libraryName The library name, if null will load the default name "monetdb5"
     * @return A boolean indicating if the load was successful
     */
    public static boolean TryLoadEmbeddedInstanceFromName(String libraryName) {
        if(!isEmbeddedInstanceInitialized) {
            if(libraryName == null) {
                libraryName = NATIVE_LIB_NAME;
            }
            System.loadLibrary(libraryName);
            isEmbeddedInstanceInitialized = true;
        }
        return true;
    }

    /**
     * Tries to load the JNI library with MonetDBLite from the given path.
     *
     * @param libraryPath The full library path name
     * @return A boolean indicating if the load was successful
     */
    public static boolean TryLoadEmbeddedInstanceFromPath(String libraryPath) {
        if(!isEmbeddedInstanceInitialized) {
            if(libraryPath == null) {
                System.load(NATIVE_LIB_NAME);
            }
            System.load(libraryPath);
            isEmbeddedInstanceInitialized = true;
        }
        return true;
    }

    /**
     * Check if the JNI library with MonetDBLite has been loaded yet or not.
     *
     * @return A boolean indicating if it is loaded
     */
    public static boolean IsEmbeddedInstanceInitialized() {
        return isEmbeddedInstanceInitialized;
    }
}
