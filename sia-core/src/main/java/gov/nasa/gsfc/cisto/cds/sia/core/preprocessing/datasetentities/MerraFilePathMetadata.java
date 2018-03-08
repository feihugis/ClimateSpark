package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities;

import javax.persistence.*;

import gov.nasa.gsfc.cisto.cds.sia.core.config.ConfigParameterKeywords;

/**
 * The type Merra file path metadata.
 */
@Entity
@Table(name = ConfigParameterKeywords.MERRA_FILE_PATH_METADATA_TABLE_NAME)
@AssociationOverrides({
        @AssociationOverride(name = "siaMetadata", joinColumns = @JoinColumn(name = "collection_name", insertable = false, updatable = false))
})
public class MerraFilePathMetadata extends SiaFilePathMetadata<MerraMetadata> {

    private MerraMetadata siaMetadata;

    /**
     * Instantiates a new Merra file path metadata.
     */
    public MerraFilePathMetadata() {

    }
}
