package gov.nasa.gsfc.cisto.cds.sia.core.collection;

import org.hibernate.boot.model.naming.PhysicalNamingStrategy;

/**
 * The interface Sia collection.
 */
public interface SiaCollection extends PhysicalNamingStrategy {

    /**
     * Gets table name.
     *
     * @return the table name
     */
    public String getTableName();

    /**
     * Sets table name.
     *
     * @param tableName the table name
     */
    public void setTableName(String tableName);

    /**
     * Gets xml hibernate table mapping file path.
     *
     * @return the xml hibernate table mapping file path
     */
    public String getXmlHibernateTableMappingFilePath();

    /**
     * Sets xml hibernate table mapping file path.
     *
     * @param filePath the file path
     */
    public void setXmlHibernateTableMappingFilePath(String filePath);

    /**
     * Gets spatiotemporal layout.
     *
     * @return the spatiotemporal layout
     */
    public String getSpatiotemporalLayout();

    /**
     * Sets spatiotemporal layout.
     *
     * @param spatiotemporalLayout the spatiotemporal layout
     */
    public void setSpatiotemporalLayout(String spatiotemporalLayout);

    /**
     * Gets xml collection setup file.
     *
     * @return the xml collection setup file
     */
    public String getXmlCollectionSetupFile();

    /**
     * Gets number pressure levels.
     *
     * @return the number pressure levels
     */
    public int getNumberPressureLevels();

    /**
     * Sets number pressure levels.
     *
     * @param numberPressureLevels the number pressure levels
     */
    public void setNumberPressureLevels(int numberPressureLevels);

    /**
     * Gets number latitude points.
     *
     * @return the number latitude points
     */
    public int getNumberLatitudePoints();

    /**
     * Sets number latitude points.
     *
     * @param numberLatitudePoints the number latitude points
     */
    public void setNumberLatitudePoints(int numberLatitudePoints);

    /**
     * Gets number longitude points.
     *
     * @return the number longitude points
     */
    public int getNumberLongitudePoints();

    /**
     * Sets number longitude points.
     *
     * @param numberLongitudePoints the number longitude points
     */
    public void setNumberLongitudePoints(int numberLongitudePoints);

    /**
     * Gets number spatial dimensions.
     *
     * @return the number spatial dimensions
     */
    public int getNumberSpatialDimensions();

    /**
     * Sets number spatial dimensions.
     *
     * @param numberSpatialDimensions the number spatial dimensions
     */
    public void setNumberSpatialDimensions(int numberSpatialDimensions);

    /**
     * Gets collection name.
     *
     * @return the collection name
     */
    public String getCollectionName();

    /**
     * Sets collection name.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName);

    /**
     * Gets temporal type.
     *
     * @return the temporal type
     */
    public String getTemporalType();

    /**
     * Sets temporal type.
     *
     * @param temporalType the temporal type
     */
    public void setTemporalType(String temporalType);

    /**
     * Gets valid min.
     *
     * @return the valid min
     */
    public float getValidMin();

    /**
     * Sets valid min.
     *
     * @param validMin the valid min
     */
    public void setValidMin(float validMin);

    /**
     * Gets valid max.
     *
     * @return the valid max
     */
    public float getValidMax();

    /**
     * Sets valid max.
     *
     * @param validMax the valid max
     */
    public void setValidMax(float validMax);

    /**
     * Gets missing value.
     *
     * @return the missing value
     */
    public float getMissingValue();

    /**
     * Sets missing value.
     *
     * @param missingValue the missing value
     */
    public void setMissingValue(float missingValue);

    /**
     * Gets current variable.
     *
     * @return the current variable
     */
    public String getCurrentVariable();

    /**
     * Sets current variable.
     *
     * @param currentVariable the current variable
     */
    public void setCurrentVariable(String currentVariable);

}
