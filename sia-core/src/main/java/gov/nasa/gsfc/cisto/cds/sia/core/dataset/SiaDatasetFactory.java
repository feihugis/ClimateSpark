package gov.nasa.gsfc.cisto.cds.sia.core.dataset;

/**
 * The type Sia dataset factory.
 */
public class SiaDatasetFactory {

    /**
     * Gets sia dataset.
     *
     * @param datasetName the dataset name
     * @return the sia dataset
     */
    public static SiaDataset getSIADataset(String datasetName) {
        if(datasetName.equalsIgnoreCase("MERRA")) {
            return new MerraDataset();
        }
        else if(datasetName.equalsIgnoreCase("MERRA2")) {
            return new Merra2Dataset();
        }

        return null;
    }

    //TODO make enums for datasets
/**
    private enum SIADatasetName {
        merra("MERRA"), merra2("MERRA2");

        private String datasetName;

        SIADatasetName(String datasetName) {
            this.datasetName = datasetName;
        }

        private String getDatasetName() {
            return datasetName;
        }
    }
 */
}
