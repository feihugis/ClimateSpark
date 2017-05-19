package gov.nasa.gsfc.cisto.cds.sia.core.variableentities;

/**
 * The type Sia variable entity factory.
 */
public class SiaVariableEntityFactory {

    /**
     * Gets sia variable entity.
     *
     * @param datasetName the dataset name
     * @return the sia variable entity
     */
    public static SiaVariableEntity getSIAVariableEntity(String datasetName) {

        if(datasetName.equalsIgnoreCase("MERRA")) {
            return new MerraVariableEntity();
        }

        if(datasetName.equalsIgnoreCase("MERRA2")) {
            return new Merra2VariableEntity();
        }

        return null;
    }
}
