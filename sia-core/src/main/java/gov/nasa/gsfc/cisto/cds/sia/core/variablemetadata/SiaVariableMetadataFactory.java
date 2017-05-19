package gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata;

/**
 * The type Sia variable metadata factory.
 */
public class SiaVariableMetadataFactory {

    /**
     * Gets variable metadata.
     *
     * @param variableMetadataName the variable metadata name
     * @return the variable metadata
     */
    public static SiaVariableAttribute getVariableMetadata(String variableMetadataName) {
        if(variableMetadataName.equalsIgnoreCase("MERRA")) {
            return new MerraVariableAttribute();
        }
        else if(variableMetadataName.equalsIgnoreCase("MERRA2")) {
            return new Merra2VariableAttribute();
        }

        return null;
    }
}
