package nl.cwi.monetdb.mcl.connection;

/**
 * Created by ferreira on 12/9/16.
 */
public interface IMonetDBLanguage {

    String getQueryTemplateIndex(int index);

    String getCommandTemplateIndex(int index);

    String[] getQueryTemplates();

    String[] getCommandTemplates();

    String getRepresentation();
}
