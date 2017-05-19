package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities;

import javax.persistence.*;

/**
 * The type Merra 2 file path metadata.
 */
@Entity
@Table(name = "merra2_file_path_metadata")
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