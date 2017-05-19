package gov.nasa.gsfc.cisto.cds.sia.core.dataset;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * The type Merra dataset.
 */
@XmlRootElement
public class MerraDataset implements SiaDataset {

    private static final String xmlDatasetSetupFile = "/merra_dataset.xml";
    private String datasetName;
    private String fileExtension;
    private String hibernateConfigXmlFile = "/merra_indexer_db.cfg.xml";

    /**
     * Instantiates a new Merra dataset.
     */
    public MerraDataset() {
    }

    public String getXmlDatasetSetupFile() {
        return xmlDatasetSetupFile;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public String getDatasetName() {
        return this.datasetName;
    }

    public void setFileExtension(String fileExtension) {
        this.fileExtension = fileExtension;
    }

    public String getFileExtension() {
        return this.fileExtension;
    }

    public void setHibernateConfigXmlFile(String hibernateConfigXmlFile) {
        this.hibernateConfigXmlFile = hibernateConfigXmlFile;
    }

    public String getHibernateConfigXmlFile() {
        return this.hibernateConfigXmlFile;
    }
}
