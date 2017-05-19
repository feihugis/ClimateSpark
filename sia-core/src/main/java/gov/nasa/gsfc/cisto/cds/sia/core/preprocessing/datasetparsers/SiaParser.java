package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * The interface Sia parser.
 */
public interface SiaParser {

    /**
     * Recursive file list list.
     *
     * @param directoryPath the directory path
     * @return the list
     */
    List<File> recursiveFileList(String directoryPath);

    /**
     * Add all collections to db.
     *
     * @param fileList the file list
     * @throws IOException        the io exception
     * @throws JAXBException      the jaxb exception
     * @throws XMLStreamException the xml stream exception
     */
    void addAllCollectionsToDb(List<File> fileList) throws IOException, JAXBException, XMLStreamException;
}
