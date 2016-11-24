package nl.cwi.monetdb.embedded.env;

/**
 * Created by ferreira on 11/24/16.
 */
public interface IEmbeddedConnection {

    long getConnectionPointer();

    void closeConnectionImplementation();
}
