package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers;

import gov.nasa.gsfc.cisto.cds.sia.core.HibernateUtils;
import gov.nasa.gsfc.cisto.cds.sia.core.common.DAOImpl;
import gov.nasa.gsfc.cisto.cds.sia.core.config.ConfigParameterKeywords;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.Merra2FilePathMetadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.Merra2Metadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.Merra2VariableMetadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.MerraFilePathMetadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.MerraMetadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.MerraVariableMetadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.utils.DateAdapter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

import gov.nasa.gsfc.cisto.cds.sia.core.randomaccessfile.MerraRandomAccessFile;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * The type Merra parser.
 */
public class MerraParser implements SiaParser {

  private static final Log LOG = LogFactory.getLog(MerraParser.class);

    private String xmlFile = "/merra_collections.xml";
    private static final int MIN_NUMBER_DIMENSIONS = 1;
    private static final String VALID_MIN = "vmin";
    private static final String VALID_MAX = "vmax";
    private static final String MISSING_VALUE = "missing_value";
    private static final String FILL_VALUE = "_FillValue";
    private static final String ADD_OFFSET = "add_offset";
    private static final String SCALE_FACTOR = "scale_factor";
    private static final Integer NOT_APPLICABLE_INTEGER = -1;
    private static final Float NOT_APPLICABLE_FLOAT = -1.0f;
    private static final String TEMPORAL_DIMENSION = "Time";
    private static final String X_AXIS_SPATIAL_DIMENSION = "XDim";
    private static final String Y_AXIS_SPATIAL_DIMENSION = "YDim";
    private static final String Z_AXIS_SPATIAL_DIMENSION = "Height";
    private static final Float FLOAT_DIVISOR_CONVERSION = 1.0f;
    private static final Float DEFAULT_VALID_MIN = -1.0E30f;
    private static final Float DEFAULT_VALID_MAX = 1.0E30f;
    private static final Float DEFAULT_MISSING_VALUE = 9.9999999E14f;
    private static final Float DEFAULT_FILL_VALUE = 9.9999999E14f;
    private static final Float DEFAULT_HEIGHT = 1.0f;


    // Attributes for Collection
    // Collection Name - String - From User
    // Temporal Resolution - String - From User
    // Temporal Range Start - Date - From User
    // Temporal Range End - Date - From User
    // Raw Data Format - String - From User
    // Statistical Interval Type - String - From User
    // *** Attributes for Variables
    // done *** Variable Name - String - From File
    // done *** Spatial Resolution - List Float - From File
    // done *** Temporal Resolution - List Integer - ???
    // done *** Dimension Order - List String - From User
    // done *** Chunk Sizes - List Integer - From File
    // done *** Valid Min - Float - From File
    // done *** Valid Max - Float - From File
    // done *** Missing Value - Float - From File
    // done *** Fill Value - Float - From File
    // done *** Add Offset - Float - From File
    // done *** Scale Factor - Float - From File
    // done *** Units - String - From File

    public List<FileStatus> recursiveFileList(String directoryPath) throws Exception {
      PathFilter merraPathFilter = new PathFilter() {
        public boolean accept(Path path) {
          try {
            FileSystem fs = gov.nasa.gsfc.cisto.cds.sia.core.common.FileUtils.getFileSystem(path.toString());
            return fs.isDirectory(path) || path.toString().endsWith("hdf");
          } catch (IOException e) {
            e.printStackTrace();
          } catch (Exception e) {
            e.printStackTrace();
          }
          return false;
        }
      };

      FileSystem fs = gov.nasa.gsfc.cisto.cds.sia.core.common.FileUtils.getFileSystem(directoryPath);
      ArrayList<FileStatus> fileList = new ArrayList<FileStatus>();
      gov.nasa.gsfc.cisto.cds.sia.core.common.FileUtils
          .getFileList(fs, new Path(directoryPath), merraPathFilter, fileList, true);

      return fileList;
    }

  public void addAllCollectionToDB(List<FileStatus> fileList, gov.nasa.gsfc.cisto.cds.sia.hibernate.DAOImpl dao, Configuration hadoopConfig)
      throws IOException, ParseException, InvalidRangeException {
    FileSystem fileSystem = FileSystem.get(hadoopConfig);
    FileStatus sampleFile = fileList.get(0);
    String collectionName = collectionNameFromFileName(sampleFile.getPath().getName());
    LOG.info("collection name: " + collectionName);

    //insert merra2metadata
    MerraMetadata merraMetadata = new MerraMetadata();
    merraMetadata.setCollectionName(collectionName);
    merraMetadata.setTemporalResolution(hadoopConfig.get(ConfigParameterKeywords.TEMPORAL_RESOLUTION));

    Date temporalRangeStart = new SimpleDateFormat("yyyy-MM-dd").parse(hadoopConfig.get(ConfigParameterKeywords.TEMPORAL_RANGE_START));
    Date temporalRangeEnd = new SimpleDateFormat("yyyy-MM-dd").parse(hadoopConfig.get(ConfigParameterKeywords.TEMPORAL_RANGE_END));
    merraMetadata.setTemporalRangeStart(temporalRangeStart);
    merraMetadata.setTemporalRangeEnd(temporalRangeEnd);
    merraMetadata.setRawDataFormat(hadoopConfig.get(ConfigParameterKeywords.RAW_DATA_FORMAT));
    merraMetadata.setStatisticalIntervalType(hadoopConfig.get(ConfigParameterKeywords.STATISTICAL_INTERVAL_TYPE));

    dao.insert(merraMetadata);

    //insert file path
    List<MerraFilePathMetadata> merraFilePathMetadataList = new ArrayList<MerraFilePathMetadata>();
    for(FileStatus file: fileList) {
      if(merraMetadata != null) {
        String temporalKey = temporalKeyFromFileName(file.getPath().getName());
        MerraFilePathMetadata
            merraFilePathMetadata = buildMerraFilePathMetadata(merraMetadata, temporalKey, file.getPath().toString());
        merraFilePathMetadataList.add(merraFilePathMetadata);
      }
    }

    dao.insertList(merraFilePathMetadataList);

    MerraRandomAccessFile randomAccessFile = new MerraRandomAccessFile(sampleFile, new Configuration());
    NetcdfFile netcdfFile = NetcdfFile.open(randomAccessFile, sampleFile.getPath().toString());
    //NetcdfFile netcdfFile = NetcdfFile.open(sampleFile.getPath().toString());
    List<Variable> variableList = netcdfFile.getVariables();

    for(Variable variable: variableList) {
      List<Dimension> dimensionList = variable.getDimensions();

      List<MerraParser.DimensionAndShape> dimensionAndShapeList = buildDimensionAndShapeList(variable.getShape(), dimensionList);
      List<MerraVariableMetadata> merraVariableMetaDataList = new ArrayList<MerraVariableMetadata>();

      if(dimensionList.size() > MIN_NUMBER_DIMENSIONS) {
        MerraVariableMetadata merraVariableMetadata = new MerraVariableMetadata();
        ArrayList<Attribute> attributeList = (ArrayList) variable.getAttributes();
        SiaVariableCompositeKey siaVariableCompositeKey = new SiaVariableCompositeKey();
        siaVariableCompositeKey.setVariableName(variable.getShortName());
        siaVariableCompositeKey.setCollectionName(merraMetadata.getCollectionName());
        merraVariableMetadata.setSiaVariableCompositeKey(siaVariableCompositeKey);
        merraVariableMetadata.setSiaMetadata(merraMetadata);
        merraVariableMetadata.setSpatialResolution(buildSpatialResolutionList(dimensionAndShapeList));
        merraVariableMetadata.setTemporalResolution(buildTemporalResolutionList(dimensionAndShapeList));
        merraVariableMetadata.setDimensionOrder(buildDimensionOrderList(dimensionList));
        merraVariableMetadata.setChunkSizes(buildChunkSizeList(dimensionAndShapeList));
        merraVariableMetadata.setValidMin(findValidMin(attributeList));
        merraVariableMetadata.setValidMax(findValidMax(attributeList));
        merraVariableMetadata.setMissingValue(findMissingValue(attributeList));
        merraVariableMetadata.setFillValue(findFillValue(attributeList));
        merraVariableMetadata.setAddOffset(findAddOffset(attributeList));
        merraVariableMetadata.setScaleFactor(findScaleFactor(attributeList));
        merraVariableMetadata.setUnits(variable.getUnitsString());
        merraVariableMetadata.setDataType(variable.getDataType().toString());

        merraMetadata.getSiaVariableMetadataSet().add(merraVariableMetadata);
        merraVariableMetaDataList.add(merraVariableMetadata);
      }

      dao.insertList(merraVariableMetaDataList);
    }
  }

    public void addAllCollectionsToDb(List<FileStatus> fileList) throws IOException, JAXBException, XMLStreamException {
        JaxbMerraCollections jaxbMerraCollections = parseXmlFile();
        HashMap<String, JaxbMerraCollection> jaxbMerraCollectionHashMap = arrayListToHashMap(jaxbMerraCollections);
        FileSystem fileSystem = FileSystem.get(new Configuration());
        SessionFactory sessionFactory = HibernateUtils.createSessionFactory("/sia_metadata_db.cfg.xml");
        DAOImpl dao = new DAOImpl();
        Session session = sessionFactory.openSession();
        dao.setSession(session);

        MerraMetadata merraMetadata = null;
        FileStatus sampleFile = fileList.get(0);
        String collectionName = collectionNameFromFileName(sampleFile.getPath().getName());
        JaxbMerraCollection jaxbMerraCollection = jaxbMerraCollectionHashMap.get(collectionName);
        merraMetadata = addCollectionToDb(sampleFile.getPath(), jaxbMerraCollection, fileSystem, dao);

        for(FileStatus file: fileList) {
            if(merraMetadata != null) {
                String temporalKey = temporalKeyFromFileName(file.getPath().getName());
                MerraFilePathMetadata merraFilePathMetadata = buildMerraFilePathMetadata(merraMetadata, temporalKey, file.getPath().toString());
                dao.insert(merraFilePathMetadata);
            }
        }

        session.close();
    }

    /**
     * Add collection to db merra metadata.
     *
     * @param file                the file
     * @param jaxbMerraCollection the jaxb merra collection
     * @param fileSystem          the file system
     * @param dao                 the dao
     * @return the merra metadata
     * @throws IOException        the io exception
     * @throws JAXBException      the jaxb exception
     * @throws XMLStreamException the xml stream exception
     */
    protected MerraMetadata addCollectionToDb(Path file, JaxbMerraCollection jaxbMerraCollection, FileSystem fileSystem, DAOImpl dao) throws IOException, JAXBException, XMLStreamException {
        FileStatus fileStatus = fileSystem.getFileStatus(file);
        MerraRandomAccessFile randomAccessFile = new MerraRandomAccessFile(fileStatus, new Configuration());
        NetcdfFile netcdfFile = NetcdfFile.open(randomAccessFile, fileStatus.getPath().toString());

        List<Variable> variableList = netcdfFile.getVariables();
        MerraMetadata merraMetadata = buildMerraMetadata(jaxbMerraCollection);
        dao.insert(merraMetadata);

        /*String temporalKey = temporalKeyFromFileName(file.getName());
        MerraFilePathMetadata merraFilePathMetadata = buildMerraFilePathMetadata(merraMetadata, temporalKey, file.toString());
        merraMetadata.getSiaFilePathMetadataSet().add(merraFilePathMetadata);
        dao.insert(merraFilePathMetadata);*/

        for(Variable variable: variableList) {
            List<Dimension> dimensionList = variable.getDimensions();

            List<DimensionAndShape> dimensionAndShapeList = buildDimensionAndShapeList(variable.getShape(), dimensionList);


            if(dimensionList.size() > MIN_NUMBER_DIMENSIONS) {
                MerraVariableMetadata merraVariableMetadata = new MerraVariableMetadata();
                ArrayList<Attribute> attributeList = (ArrayList) variable.getAttributes();
                SiaVariableCompositeKey siaVariableCompositeKey = new SiaVariableCompositeKey();
                siaVariableCompositeKey.setVariableName(variable.getShortName());
                siaVariableCompositeKey.setCollectionName(merraMetadata.getCollectionName());
                merraVariableMetadata.setSiaVariableCompositeKey(siaVariableCompositeKey);
                merraVariableMetadata.setSiaMetadata(merraMetadata);
                merraVariableMetadata.setSpatialResolution(buildSpatialResolutionList(dimensionAndShapeList));
                merraVariableMetadata.setTemporalResolution(buildTemporalResolutionList(dimensionAndShapeList));
                merraVariableMetadata.setDimensionOrder(buildDimensionOrderList(dimensionList));
                merraVariableMetadata.setChunkSizes(buildChunkSizeList(dimensionAndShapeList));
                merraVariableMetadata.setValidMin(findValidMin(attributeList));
                merraVariableMetadata.setValidMax(findValidMax(attributeList));
                merraVariableMetadata.setMissingValue(findMissingValue(attributeList));
                merraVariableMetadata.setFillValue(findFillValue(attributeList));
                merraVariableMetadata.setAddOffset(findAddOffset(attributeList));
                merraVariableMetadata.setScaleFactor(findScaleFactor(attributeList));
                merraVariableMetadata.setUnits(variable.getUnitsString());
                merraVariableMetadata.setDataType(variable.getDataType().toString());

                merraMetadata.getSiaVariableMetadataSet().add(merraVariableMetadata);
                dao.insert(merraVariableMetadata);
            }
        }

        return merraMetadata;
    }

    /**
     * Gets xml file.
     *
     * @return the xml file
     */
    protected String getXmlFile() { return this.xmlFile; }

    /**
     * Sets xml file.
     *
     * @param xmlFile the xml file
     */
    protected void setXmlFile(String xmlFile) { this.xmlFile = xmlFile; }

    /**
     * Collection name from file name string.
     *
     * @param fileName the file name
     * @return the string
     */
    protected String collectionNameFromFileName(String fileName) {
        String[] splitName = fileName.split("\\.");
        return splitName[3].toLowerCase();
    }

    /**
     * Temporal key from file name string.
     *
     * @param fileName the file name
     * @return the string
     */
    protected String temporalKeyFromFileName(String fileName) {
        String[] splitName = fileName.split("\\.");
        return splitName[splitName.length - 2].toLowerCase();
    }

    /**
     * Parse xml file jaxb merra collections.
     *
     * @return the jaxb merra collections
     * @throws JAXBException      the jaxb exception
     * @throws XMLStreamException the xml stream exception
     */
    protected JaxbMerraCollections parseXmlFile() throws JAXBException, XMLStreamException {
        XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();
        StreamSource streamSource = new StreamSource(getClass().getResourceAsStream(this.getXmlFile()));
        XMLStreamReader xmlStreamReader = xmlInputFactory.createXMLStreamReader(streamSource);
        JaxbMerraCollections jaxbMerraCollections;
        JAXBContext jaxbContext = JAXBContext.newInstance(JaxbMerraCollections.class);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        JAXBElement<JaxbMerraCollections> jaxbElement = unmarshaller.unmarshal(xmlStreamReader, JaxbMerraCollections.class);
        jaxbMerraCollections = jaxbElement.getValue();

        return jaxbMerraCollections;
    }

    /**
     * Array list to hash map hash map.
     *
     * @param jaxbMerraCollections the jaxb merra collections
     * @return the hash map
     */
    protected HashMap<String, JaxbMerraCollection> arrayListToHashMap(JaxbMerraCollections jaxbMerraCollections) {
        ArrayList<JaxbMerraCollection> jaxbMerraCollectionArrayList = jaxbMerraCollections.getJaxbMerraCollections();
        HashMap<String, JaxbMerraCollection> jaxbMerraCollectionHashMap = new HashMap<String, JaxbMerraCollection>(jaxbMerraCollectionArrayList.size());

        for(JaxbMerraCollection jaxbMerraCollection: jaxbMerraCollectionArrayList) {
            jaxbMerraCollectionHashMap.put(jaxbMerraCollection.getName().toLowerCase(), jaxbMerraCollection);
        }

        return jaxbMerraCollectionHashMap;
    }

    /**
     * Build merra metadata merra metadata.
     *
     * @param jaxbMerraCollection the jaxb merra collection
     * @return the merra metadata
     */
    protected MerraMetadata buildMerraMetadata(JaxbMerraCollection jaxbMerraCollection) {
        MerraMetadata merraMetadata = new MerraMetadata();
        merraMetadata.setCollectionName(jaxbMerraCollection.getName());
        merraMetadata.setTemporalResolution(jaxbMerraCollection.getTemporalResolution());
        merraMetadata.setTemporalRangeStart(jaxbMerraCollection.getTemporalRangeStart());
        merraMetadata.setTemporalRangeEnd(jaxbMerraCollection.getTemporalRangeEnd());
        merraMetadata.setRawDataFormat(".hdf");
        merraMetadata.setStatisticalIntervalType(jaxbMerraCollection.getStatisticalIntervalType());

        return merraMetadata;
    }

    /**
     * Build merra file path metadata merra file path metadata.
     *
     * @param merraMetadata the merra metadata
     * @param temporalKey   the temporal key
     * @param filePath      the file path
     * @return the merra file path metadata
     */
    protected MerraFilePathMetadata buildMerraFilePathMetadata(MerraMetadata merraMetadata, String temporalKey, String filePath) {
        MerraFilePathMetadata merraFilePathMetadata = new MerraFilePathMetadata();
        SiaFilePathCompositeKey siaFilePathCompositeKey = new SiaFilePathCompositeKey();
        siaFilePathCompositeKey.setCollectionName(merraMetadata.getCollectionName());
        siaFilePathCompositeKey.setTemporalKey(Integer.parseInt(temporalKey));
        merraFilePathMetadata.setSiaFilePathCompositeKey(siaFilePathCompositeKey);
        merraFilePathMetadata.setSiaMetadata(merraMetadata);
        merraFilePathMetadata.setFilePath(filePath);

        return merraFilePathMetadata;
    }

    /**
     * Build dimension and shape list list.
     *
     * @param shapeArray    the shape array
     * @param dimensionList the dimension list
     * @return the list
     */
    protected List<DimensionAndShape> buildDimensionAndShapeList(int[] shapeArray, List<Dimension> dimensionList) {
        List<DimensionAndShape> dimensionAndShapeList = new ArrayList<DimensionAndShape>();

        for(int i = 0; i < shapeArray.length; i++) {
            dimensionAndShapeList.add(new DimensionAndShape(shapeArray[i], dimensionList.get(i)));
        }

        return dimensionAndShapeList;
    }

    /**
     * Build spatial resolution list list.
     *
     * @param dimensionAndShapeList the dimension and shape list
     * @return the list
     */
    protected List<Float> buildSpatialResolutionList(List<DimensionAndShape> dimensionAndShapeList) {
        List<Float> spatialResolutionList = new ArrayList<Float>();

        spatialResolutionList.add(findLongitude(dimensionAndShapeList));
        spatialResolutionList.add(findLatitude(dimensionAndShapeList));
        spatialResolutionList.add(findHeight(dimensionAndShapeList));
        return spatialResolutionList;
    }

    /**
     * Find longitude float.
     *
     * @param dimensionAndShapeList the dimension and shape list
     * @return the float
     */
    protected Float findLongitude(List<DimensionAndShape> dimensionAndShapeList) {
        Float longitudeResolution = NOT_APPLICABLE_FLOAT;

        for(DimensionAndShape dimensionAndShape: dimensionAndShapeList) {
            if(dimensionAndShape.getDimension().getShortName().equalsIgnoreCase(X_AXIS_SPATIAL_DIMENSION)) {
                return (dimensionAndShape.getShape() / FLOAT_DIVISOR_CONVERSION);
            }
        }

        return longitudeResolution;
    }

    /**
     * Find latitude float.
     *
     * @param dimensionAndShapeList the dimension and shape list
     * @return the float
     */
    protected Float findLatitude(List<DimensionAndShape> dimensionAndShapeList) {
        Float latitudeResolution = NOT_APPLICABLE_FLOAT;

        for(DimensionAndShape dimensionAndShape: dimensionAndShapeList) {
            if(dimensionAndShape.getDimension().getShortName().equalsIgnoreCase(Y_AXIS_SPATIAL_DIMENSION)) {
                return (dimensionAndShape.getShape() / FLOAT_DIVISOR_CONVERSION);
            }
        }

        return latitudeResolution;
    }

    /**
     * Find height float.
     *
     * @param dimensionAndShapeList the dimension and shape list
     * @return the float
     */
    protected Float findHeight(List<DimensionAndShape> dimensionAndShapeList) {
        Float heightResolution = DEFAULT_HEIGHT;

        for(DimensionAndShape dimensionAndShape: dimensionAndShapeList) {
            if(dimensionAndShape.getDimension().getShortName().equalsIgnoreCase(Z_AXIS_SPATIAL_DIMENSION)) {
                return (dimensionAndShape.getShape() / FLOAT_DIVISOR_CONVERSION);
            }
        }

        return heightResolution;
    }

    /**
     * Build temporal resolution list list.
     *
     * @param dimensionAndShapeList the dimension and shape list
     * @return the list
     */
    protected List<Integer> buildTemporalResolutionList(List<DimensionAndShape> dimensionAndShapeList) {
        List<Integer> temporalResolutionList = new ArrayList<Integer>();

        temporalResolutionList.add(findTime(dimensionAndShapeList));

        return temporalResolutionList;
    }

    /**
     * Find time integer.
     *
     * @param dimensionAndShapeList the dimension and shape list
     * @return the integer
     */
    protected Integer findTime(List<DimensionAndShape> dimensionAndShapeList) {
        Integer timeResolution = NOT_APPLICABLE_INTEGER;

        for(DimensionAndShape dimensionAndShape: dimensionAndShapeList) {
            if(dimensionAndShape.getDimension().getShortName().equalsIgnoreCase(TEMPORAL_DIMENSION)) {
                return dimensionAndShape.getShape();
            }
        }

        return timeResolution;
    }

    /**
     * Build dimension order list list.
     *
     * @param dimensionList the dimension list
     * @return the list
     */
    protected List<String> buildDimensionOrderList(List<Dimension> dimensionList) {
        List<String> dimensionOrderList = new ArrayList<String>();

        for(Dimension dimension: dimensionList) {
            dimensionOrderList.add(dimension.getShortName());
        }

        return dimensionOrderList;
    }

    /**
     * Build chunk size list list.
     *
     * @param dimensionAndShapeList the dimension and shape list
     * @return the list
     */
    protected List<Integer> buildChunkSizeList(List<DimensionAndShape> dimensionAndShapeList) {
        List<Integer> chunkSizeList = new ArrayList<Integer>();
        for (DimensionAndShape dimensionAndShape : dimensionAndShapeList) {
          chunkSizeList.add(dimensionAndShape.getShape());
        }

        if (chunkSizeList.size() > 2) {
          for (int i = 0; i < chunkSizeList.size() - 2; i++) {
            chunkSizeList.set(i, 1);
          }
        }

       /* chunkSizeList.add(findLongitude(dimensionAndShapeList).intValue());
        chunkSizeList.add(findLatitude(dimensionAndShapeList).intValue());
        chunkSizeList.add(findHeight(dimensionAndShapeList).intValue());*/

        return chunkSizeList;
    }

    /**
     * Find valid min float.
     *
     * @param attributeList the attribute list
     * @return the float
     */
    protected Float findValidMin(List<Attribute> attributeList) {
        Float validMin = DEFAULT_VALID_MIN;

        for(Attribute attribute: attributeList) {
            if(attribute.getFullName().equalsIgnoreCase(VALID_MIN)) {
                return (Float) attribute.getNumericValue();
            }
        }
        return validMin;
    }

    /**
     * Find valid max float.
     *
     * @param attributeList the attribute list
     * @return the float
     */
    protected Float findValidMax(List<Attribute> attributeList) {
        Float validMax = DEFAULT_VALID_MAX;

        for(Attribute attribute: attributeList) {
            if(attribute.getFullName().equalsIgnoreCase(VALID_MAX)) {
                return (Float) attribute.getNumericValue();
            }
        }
        return validMax;
    }

    /**
     * Find missing value float.
     *
     * @param attributeList the attribute list
     * @return the float
     */
    protected Float findMissingValue(List<Attribute> attributeList) {
        Float missingValue = DEFAULT_MISSING_VALUE;

        for(Attribute attribute: attributeList) {
            if(attribute.getFullName().equalsIgnoreCase(MISSING_VALUE)) {
                return (Float) attribute.getNumericValue();
            }
        }
        return missingValue;
    }

    /**
     * Find fill value float.
     *
     * @param attributeList the attribute list
     * @return the float
     */
    protected Float findFillValue(List<Attribute> attributeList) {
        Float fillValue = DEFAULT_FILL_VALUE;

        for(Attribute attribute: attributeList) {
            if(attribute.getFullName().equalsIgnoreCase(FILL_VALUE)) {
                return (Float) attribute.getNumericValue();
            }
        }
        return fillValue;
    }

    /**
     * Find add offset float.
     *
     * @param attributeList the attribute list
     * @return the float
     */
    protected Float findAddOffset(List<Attribute> attributeList) {
        Float addOffset = NOT_APPLICABLE_FLOAT;

        for(Attribute attribute: attributeList) {
            if(attribute.getFullName().equalsIgnoreCase(ADD_OFFSET)) {
                return (Float) attribute.getNumericValue();
            }
        }
        return addOffset;
    }

    /**
     * Find scale factor float.
     *
     * @param attributeList the attribute list
     * @return the float
     */
    protected Float findScaleFactor(List<Attribute> attributeList) {
        Float scaleFactor = NOT_APPLICABLE_FLOAT;

        for(Attribute attribute: attributeList) {
            if(attribute.getFullName().equalsIgnoreCase(SCALE_FACTOR)) {
                return (Float) attribute.getNumericValue();
            }
        }
        return scaleFactor;
    }

    /**
     * The type Dimension and shape.
     */
    protected class DimensionAndShape {

        private Dimension dimension;
        private int shape;

        /**
         * Instantiates a new Dimension and shape.
         *
         * @param shape     the shape
         * @param dimension the dimension
         */
        public DimensionAndShape(int shape, Dimension dimension) {
            this.shape = shape;
            this.dimension = dimension;
        }

        /**
         * Gets shape.
         *
         * @return the shape
         */
        public int getShape() {
            return shape;
        }

        /**
         * Gets dimension.
         *
         * @return the dimension
         */
        public Dimension getDimension() {
            return dimension;
        }
    }

    /**
     * The type Jaxb merra collections.
     */
    @XmlRootElement(name = "merraCollections")
    protected static class JaxbMerraCollections {
        private ArrayList<JaxbMerraCollection> jaxbMerraCollections;

        /**
         * Gets jaxb merra collections.
         *
         * @return the jaxb merra collections
         */
        public ArrayList<JaxbMerraCollection> getJaxbMerraCollections() {
            return jaxbMerraCollections;
        }

        /**
         * Sets jaxb merra collections.
         *
         * @param jaxbMerraCollections the jaxb merra collections
         */
        @XmlElement(name = "collection")
        public void setJaxbMerraCollections(ArrayList<JaxbMerraCollection> jaxbMerraCollections) {
            this.jaxbMerraCollections = jaxbMerraCollections;
        }
    }

    /**
     * The type Jaxb merra collection.
     */
    @XmlRootElement(name = "collection")
    protected static class JaxbMerraCollection {
        private String name;
        private String temporalResolution;
        private Date temporalRangeStart;
        private Date temporalRangeEnd;
        private String statisticalIntervalType;

        /**
         * Gets name.
         *
         * @return the name
         */
        public String getName() {
            return name;
        }

        /**
         * Sets name.
         *
         * @param name the name
         */
        @XmlAttribute
        public void setName(String name) {
            this.name = name;
        }

        /**
         * Gets temporal resolution.
         *
         * @return the temporal resolution
         */
        public String getTemporalResolution() {
            return temporalResolution;
        }

        /**
         * Sets temporal resolution.
         *
         * @param temporalResolution the temporal resolution
         */
        @XmlElement
        public void setTemporalResolution(String temporalResolution) {
            this.temporalResolution = temporalResolution;
        }

        /**
         * Gets temporal range start.
         *
         * @return the temporal range start
         */
        public Date getTemporalRangeStart() {
            return temporalRangeStart;
        }

        /**
         * Sets temporal range start.
         *
         * @param temporalRangeStart the temporal range start
         */
        @XmlElement
        @XmlJavaTypeAdapter(DateAdapter.class)
        public void setTemporalRangeStart(Date temporalRangeStart) {
            this.temporalRangeStart = temporalRangeStart;
        }

        /**
         * Gets temporal range end.
         *
         * @return the temporal range end
         */
        public Date getTemporalRangeEnd() {
            return temporalRangeEnd;
        }

        /**
         * Sets temporal range end.
         *
         * @param temporalRangeEnd the temporal range end
         */
        @XmlElement
        @XmlJavaTypeAdapter(DateAdapter.class)
        public void setTemporalRangeEnd(Date temporalRangeEnd) {
            this.temporalRangeEnd = temporalRangeEnd;
        }

        /**
         * Gets statistical interval type.
         *
         * @return the statistical interval type
         */
        public String getStatisticalIntervalType() {
            return statisticalIntervalType;
        }

        /**
         * Sets statistical interval type.
         *
         * @param statisticalIntervalType the statistical interval type
         */
        @XmlElement
        public void setStatisticalIntervalType(String statisticalIntervalType) {
            this.statisticalIntervalType = statisticalIntervalType;
        }
    }
}