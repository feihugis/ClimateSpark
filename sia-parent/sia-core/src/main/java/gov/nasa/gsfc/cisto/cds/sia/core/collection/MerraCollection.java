package gov.nasa.gsfc.cisto.cds.sia.core.collection;

import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

/**
 * The type Merra collection.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class MerraCollection implements SiaCollection {

    private static final String xmlCollectionSetupFile = "/merra_mapreduce_collections.xml";
    private String xmlHibernateTableMappingFile;
    private String collectionName;
    private String temporalType;
    private int numberPressureLevels;
    private int numberLatitudePoints;
    private int numberLongitudePoints;
    private int numberSpatialDimensions;
    private String spatiotemporalLayout;
    private float missingValue;
    private float validMin;
    private float validMax;
    private String currentVariable;
    private String tableName;

    /**
     * Instantiates a new Merra collection.
     */
    public MerraCollection() {
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getXmlHibernateTableMappingFilePath() {
        return xmlHibernateTableMappingFile;
    }

    public void setXmlHibernateTableMappingFilePath(String filePath) {
        this.xmlHibernateTableMappingFile = filePath;
    }

    public String getSpatiotemporalLayout() {
        return spatiotemporalLayout;
    }

    public void setSpatiotemporalLayout(String spatiotemporalLayout) {
        this.spatiotemporalLayout = spatiotemporalLayout;
    }

    public String getCurrentVariable() {
        return currentVariable;
    }

    public void setCurrentVariable(String currentVariable) {
        this.currentVariable = currentVariable;
    }

    public String getXmlCollectionSetupFile() {
        return xmlCollectionSetupFile;
    }

    public int getNumberSpatialDimensions() {
        return numberSpatialDimensions;
    }

    public void setNumberSpatialDimensions(int numberSpatialDimensions) {
        this.numberSpatialDimensions = numberSpatialDimensions;
    }

    public int getNumberPressureLevels() {
        return numberPressureLevels;
    }

    public void setNumberPressureLevels(int numberPressureLevels) {
        this.numberPressureLevels = numberPressureLevels;
    }

    public int getNumberLatitudePoints() {
        return numberLatitudePoints;
    }

    public void setNumberLatitudePoints(int numberLatitudePoints) {
        this.numberLatitudePoints = numberLatitudePoints;
    }

    public int getNumberLongitudePoints() {
        return numberLongitudePoints;
    }

    public void setNumberLongitudePoints(int numberLongitudePoints) {
        this.numberLongitudePoints = numberLongitudePoints;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getTemporalType() {
        return temporalType;
    }

    public void setTemporalType(String temporalType) {
        this.temporalType = temporalType;
    }

    public float getValidMin() {
        return validMin;
    }

    public void setValidMin(float validMin) {
        this.validMin = validMin;
    }

    public float getValidMax() {
        return validMax;
    }

    public void setValidMax(float validMax) {
        this.validMax = validMax;
    }

    public float getMissingValue() {
        return missingValue;
    }

    public void setMissingValue(float missingValue) {
        this.missingValue = missingValue;
    }

    public Identifier toPhysicalCatalogName(Identifier identifier, JdbcEnvironment jdbcEnvironment) {
        return identifier;
    }

    public Identifier toPhysicalSchemaName(Identifier identifier, JdbcEnvironment jdbcEnvironment) {
        return identifier;
    }

    public Identifier toPhysicalTableName(Identifier name, JdbcEnvironment context) {
        Identifier identifier = new Identifier(this.tableName, false);
        return identifier;
    }

    public Identifier toPhysicalSequenceName(Identifier identifier, JdbcEnvironment jdbcEnvironment) {
        return identifier;
    }

    public Identifier toPhysicalColumnName(Identifier identifier, JdbcEnvironment jdbcEnvironment) {
        return identifier;
    }
}
