package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

/**
 * The type Sia variable composite key.
 */
@Embeddable
public class SiaVariableCompositeKey implements Serializable {
    /**
     * The Collection name.
     */
    @Column(name = "collection_name", nullable = false, updatable = false)
    protected String collectionName;
    /**
     * The Variable name.
     */
    @Column(name = "variable_name", nullable = false, updatable = false)
    protected String variableName;

    /**
     * Instantiates a new Sia variable composite key.
     */
    public SiaVariableCompositeKey() {}

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
     * Gets variable name.
     *
     * @return the variable name
     */
    public String getVariableName() {
        return variableName;
    }

    /**
     * Sets variable name.
     *
     * @param variableName the variable name
     */
    public void setVariableName(String variableName) {
        this.variableName = variableName;
    }

    @Override
    public String toString() {
        return "SiaVariableCompositeKey{" +
                "collectionName='" + collectionName + '\'' +
                ", variableName='" + variableName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SiaVariableCompositeKey that = (SiaVariableCompositeKey) o;

        if (!collectionName.equals(that.collectionName)) return false;
        return variableName.equals(that.variableName);

    }

    @Override
    public int hashCode() {
        int result = collectionName.hashCode();
        result = 31 * result + variableName.hashCode();
        return result;
    }
}
