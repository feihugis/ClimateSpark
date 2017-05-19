package gov.nasa.gsfc.cisto.cds.sia.core.dataset;

/**
 * The interface Sia dataset.
 */
public interface SiaDataset {

    /**
     * Gets xml dataset setup file.
     *
     * @return the xml dataset setup file
     */
    public String getXmlDatasetSetupFile();

    /**
     * Sets dataset name.
     *
     * @param datasetName the dataset name
     */
    public void setDatasetName(String datasetName);

    /**
     * Gets dataset name.
     *
     * @return the dataset name
     */
    public String getDatasetName();

    /**
     * Sets file extension.
     *
     * @param fileExtension the file extension
     */
    public void setFileExtension(String fileExtension);

    /**
     * Gets file extension.
     *
     * @return the file extension
     */
    public String getFileExtension();

    /**
     * Sets hibernate config xml file.
     *
     * @param hibernateConfigXmlFile the hibernate config xml file
     */
    public void setHibernateConfigXmlFile(String hibernateConfigXmlFile);

    /**
     * Gets hibernate config xml file.
     *
     * @return the hibernate config xml file
     */
    public String getHibernateConfigXmlFile();
}
