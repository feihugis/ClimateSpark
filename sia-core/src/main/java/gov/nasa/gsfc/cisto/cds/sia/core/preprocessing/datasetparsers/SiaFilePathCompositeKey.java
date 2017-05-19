package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

/**
 * The type Sia file path composite key.
 */
@Embeddable
public class SiaFilePathCompositeKey implements Serializable {
    /**
     * The Collection name.
     */
    @Column(name = "collection_name", nullable = false, updatable = false)
    protected String collectionName;
    /**
     * The Temporal key.
     */
    @Column(name = "temporal_key", nullable = false, updatable = false)
    protected String temporalKey;

    /**
     * Instantiates a new Sia file path composite key.
     */
    public SiaFilePathCompositeKey() {}

    /**
     * Gets collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets collection name.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Gets temporal key.
     *
     * @return the temporal key
     */
    public String getTemporalKey() {
        return temporalKey;
    }

    /**
     * Sets temporal key.
     *
     * @param temporalKey the temporal key
     */
    public void setTemporalKey(String temporalKey) {
        this.temporalKey = temporalKey;
    }

    @Override
    public String toString() {
        return "SiaFilePathCompositeKey{" +
                "collectionName='" + collectionName + '\'' +
                ", temporalKey='" + temporalKey + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SiaFilePathCompositeKey that = (SiaFilePathCompositeKey) o;

        if (!collectionName.equals(that.collectionName)) return false;
        return temporalKey.equals(that.temporalKey);

    }

    @Override
    public int hashCode() {
        int result = collectionName.hashCode();
        result = 31 * result + temporalKey.hashCode();
        return result;
    }
}
