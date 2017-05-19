package gov.nasa.gsfc.cisto.cds.sia.core.collection;

/**
 * The type Sia collection factory.
 */
public class SiaCollectionFactory {

    /**
     * Gets sia collection.
     *
     * @param datasetName the dataset name
     * @return the sia collection
     */
    public static SiaCollection getSIACollection(String datasetName) {
        if(datasetName.equalsIgnoreCase("MERRA")) {
            return new MerraCollection();
        }
        else if(datasetName.equalsIgnoreCase("MERRA2")) {
            return new Merra2Collection();
        }

        return null;
    }
}
