package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers;

/**
 * The type Sia parser factory.
 */
public class SiaParserFactory {

    /**
     * Gets sia parser.
     *
     * @param datasetName the dataset name
     * @return the sia parser
     */
    public static SiaParser getSiaParser(String datasetName) {
        if(datasetName.equalsIgnoreCase("MERRA")) {
            return new MerraParser();
        }
        else if(datasetName.equalsIgnoreCase("MERRA2")) {
            return new Merra2Parser();
        }

        return null;
    }
}
