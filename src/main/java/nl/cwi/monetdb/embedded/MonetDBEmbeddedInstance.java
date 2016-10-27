package nl.cwi.monetdb.embedded;

/**
 * Created by ferreira on 10/27/16.
 */
public class MonetDBEmbeddedInstance {

    private static boolean isEmbeddedInstanceInitialized = false;

    private static final String NATIVE_LIB_NAME = "monetdb5";

    public static boolean TryLoadEmbeddedInstanceFromName(String libraryName) {
        if(isEmbeddedInstanceInitialized == false) {
            if(libraryName == null) {
                libraryName = NATIVE_LIB_NAME;
            }
            System.loadLibrary(libraryName);
            isEmbeddedInstanceInitialized = true;
        }
        return true;
    }

    public static boolean TryLoadEmbeddedInstanceFromPath(String libraryPath) {
        if(isEmbeddedInstanceInitialized == false) {
            if(libraryPath == null) {
                return false;
            }
            System.load(libraryPath);
            isEmbeddedInstanceInitialized = true;
        }
        return true;
    }

    public static boolean IsEmbeddedInstanceInitialized() {
        return isEmbeddedInstanceInitialized;
    }
}
