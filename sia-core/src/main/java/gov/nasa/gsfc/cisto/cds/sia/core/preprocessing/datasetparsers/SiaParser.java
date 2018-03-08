package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import ucar.ma2.InvalidRangeException;

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
    List<FileStatus> recursiveFileList(String directoryPath) throws Exception;

    /**
     * Add all collections to db.
     *
     * @param fileList the file list
     * @throws IOException        the io exception
     * @throws JAXBException      the jaxb exception
     * @throws XMLStreamException the xml stream exception
     */
    void addAllCollectionsToDb(List<FileStatus> fileList) throws IOException, JAXBException, XMLStreamException;

    void addAllCollectionToDB(List<FileStatus> fileList, gov.nasa.gsfc.cisto.cds.sia.hibernate.DAOImpl dao, Configuration hadoopConfig) throws IOException,
                                                                                                                                               ParseException,
                                                                                                                                               InvalidRangeException;
}
