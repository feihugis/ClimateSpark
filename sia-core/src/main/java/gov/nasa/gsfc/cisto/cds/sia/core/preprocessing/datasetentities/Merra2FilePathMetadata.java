package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities;

import javax.persistence.*;

import gov.nasa.gsfc.cisto.cds.sia.core.config.ConfigParameterKeywords;

/**
 * The type Merra 2 file path metadata.
 */
@Entity
@Table(name = ConfigParameterKeywords.MERRA2_FILE_PATH_METADATA_TABLE_NAME)
@AssociationOverrides({
        @AssociationOverride(name = "siaMetadata", joinColumns = @JoinColumn(name = "collection_name", insertable = false, updatable = false))
})
public class Merra2FilePathMetadata extends SiaFilePathMetadata<Merra2Metadata> {

    private Merra2Metadata siaMetadata;

    /**
     * Instantiates a new Merra 2 file path metadata.
     */
    public Merra2FilePathMetadata() {

    }
}