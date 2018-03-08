package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers;

import gov.nasa.gsfc.cisto.cds.sia.core.HibernateUtils;
import gov.nasa.gsfc.cisto.cds.sia.core.common.DAOImpl;
import gov.nasa.gsfc.cisto.cds.sia.core.config.ConfigParameterKeywords;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.PreprocessorDriver;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.Merra2FilePathMetadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.Merra2Metadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.Merra2VariableMetadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.MerraFilePathMetadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.MerraMetadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.utils.DateAdapter;
import gov.nasa.gsfc.cisto.cds.sia.core.randomaccessfile.MerraRandomAccessFile;

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
 * The type Merra 2 parser.
 */
public class Merra2Parser implements SiaParser {

  private static final Log LOG = LogFactory.getLog(Merra2Parser.class);

  private String xmlFile = "/merra2_collections.xml";
  private static final int MIN_NUMBER_DIMENSIONS = 1;
  private static final String VALID_MIN = "vmin";
  private static final String VALID_MAX = "vmax";
  private static final String MISSING_VALUE = "missing_value";
  private static final String FILL_VALUE = "_FillValue";
  private static final String ADD_OFFSET = "add_offset";
  private static final String SCALE_FACTOR = "scale_factor";
  private static final Integer NOT_APPLICABLE_INTEGER = -1;
  private static final Float NOT_APPLICABLE_FLOAT = -1.0f;
  private static final String TEMPORAL_DIMENSION = "time";
  private static final String X_AXIS_SPATIAL_DIMENSION = "lon";
  private static final String Y_AXIS_SPATIAL_DIMENSION = "lat";
  private static final String Z_AXIS_SPATIAL_DIMENSION = "Height";
  private static final Float FLOAT_DIVISOR_CONVERSION = 1.0f;
  private static final Float DEFAULT_VALID_MIN = -9.9999999E14f;
  private static final Float DEFAULT_VALID_MAX = 9.9999999E14f;
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
        PathFilter merra2PathFilter = new PathFilter() {
          public boolean accept(Path path) {
            try {
              FileSystem fs = gov.nasa.gsfc.cisto.cds.sia.core.common.FileUtils.getFileSystem(path.toString());
              return fs.isDirectory(path) || path.toString().endsWith("nc4");
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
          .getFileList(fs, new Path(directoryPath), merra2PathFilter, fileList, true);

      return fileList;
    }

    public void addAllCollectionToDB(List<FileStatus> fileList, gov.nasa.gsfc.cisto.cds.sia.hibernate.DAOImpl dao, Configuration hadoopConfig)
        throws IOException, ParseException, InvalidRangeException {
      FileStatus sampleFile = fileList.get(0);
      String collectionName = collectionNameFromFileName(sampleFile.getPath().getName());
      LOG.info("collection name: " + collectionName);

      //insert merra2metadata
      Merra2Metadata merra2Metadata = new Merra2Metadata();
      merra2Metadata.setCollectionName(collectionName);
      merra2Metadata.setTemporalResolution(hadoopConfig.get(ConfigParameterKeywords.TEMPORAL_RESOLUTION));

      Date temporalRangeStart = new SimpleDateFormat("yyyy-MM-dd").parse(hadoopConfig.get(ConfigParameterKeywords.TEMPORAL_RANGE_START));
      Date temporalRangeEnd = new SimpleDateFormat("yyyy-MM-dd").parse(hadoopConfig.get(ConfigParameterKeywords.TEMPORAL_RANGE_END));
      merra2Metadata.setTemporalRangeStart(temporalRangeStart);
      merra2Metadata.setTemporalRangeEnd(temporalRangeEnd);
      merra2Metadata.setRawDataFormat(hadoopConfig.get(ConfigParameterKeywords.RAW_DATA_FORMAT));
      merra2Metadata.setStatisticalIntervalType(hadoopConfig.get(ConfigParameterKeywords.STATISTICAL_INTERVAL_TYPE));

      dao.insert(merra2Metadata);

      //insert file path
      List<Merra2FilePathMetadata> merra2FilePathMetadataList = new ArrayList<Merra2FilePathMetadata>();
      for(FileStatus file: fileList) {
        if(merra2Metadata != null) {
          String temporalKey = temporalKeyFromFileName(file.getPath().getName());
          Merra2FilePathMetadata merraFilePathMetadata = buildMerra2FilePathMetadata(merra2Metadata, temporalKey, file.getPath().toString());
          merra2FilePathMetadataList.add(merraFilePathMetadata);
          //dao.insert(merraFilePathMetadata);
        }
      }

      dao.insertList(merra2FilePathMetadataList);

      MerraRandomAccessFile randomAccessFile = new MerraRandomAccessFile(sampleFile, new Configuration());
      NetcdfFile netcdfFile = NetcdfFile.open(randomAccessFile, sampleFile.getPath().toString());
      List<Variable> variableList = netcdfFile.getVariables();

      List<Merra2VariableMetadata> merra2VariableMetadataList = new ArrayList<Merra2VariableMetadata>();
      for(Variable variable: variableList) {
        List<Dimension> dimensionList = variable.getDimensions();
        List<DimensionAndShape> dimensionAndShapeList = buildDimensionAndShapeList(variable.getShape(), dimensionList);

        if(dimensionList.size() > MIN_NUMBER_DIMENSIONS) {
          Merra2VariableMetadata merra2VariableMetadata = new Merra2VariableMetadata();
          ArrayList<Attribute> attributeList = (ArrayList) variable.getAttributes();
          SiaVariableCompositeKey siaVariableCompositeKey = new SiaVariableCompositeKey();
          siaVariableCompositeKey.setVariableName(variable.getShortName());
          siaVariableCompositeKey.setCollectionName(merra2Metadata.getCollectionName());
          merra2VariableMetadata.setSiaVariableCompositeKey(siaVariableCompositeKey);
          merra2VariableMetadata.setSiaMetadata(merra2Metadata);
          merra2VariableMetadata.setSpatialResolution(buildSpatialResolutionList(dimensionAndShapeList));
          merra2VariableMetadata.setTemporalResolution(buildTemporalResolutionList(dimensionAndShapeList));
          merra2VariableMetadata.setDimensionOrder(buildDimensionOrderList(dimensionList));
          merra2VariableMetadata.setChunkSizes(getChunkSize(variable));
          merra2VariableMetadata.setValidMin(findValidMin(attributeList));
          merra2VariableMetadata.setValidMax(findValidMax(attributeList));
          merra2VariableMetadata.setMissingValue(findMissingValue(attributeList));
          merra2VariableMetadata.setFillValue(findFillValue(attributeList));
          merra2VariableMetadata.setAddOffset(findAddOffset(attributeList));
          merra2VariableMetadata.setScaleFactor(findScaleFactor(attributeList));
          merra2VariableMetadata.setUnits(variable.getUnitsString());
          merra2VariableMetadata.setDataType(variable.getDataType().toString());

          merra2Metadata.getSiaVariableMetadataSet().add(merra2VariableMetadata);

          //dao.insert(merra2VariableMetadata);
          merra2VariableMetadataList.add(merra2VariableMetadata);
        }
      }

      dao.insertList(merra2VariableMetadataList);
    }

    public void addAllCollectionsToDb(List<FileStatus> fileList) throws IOException, JAXBException, XMLStreamException {
        JaxbMerra2Collections jaxbMerra2Collections = parseXmlFile();
        HashMap<String, JaxbMerra2Collection> jaxbMerra2CollectionHashMap = arrayListToHashMap(jaxbMerra2Collections);
        FileSystem fileSystem = FileSystem.get(new Configuration());
        SessionFactory sessionFactory = HibernateUtils.createSessionFactory("sia_metadata_db.cfg.xml");
        DAOImpl dao = new DAOImpl();
        Session session = sessionFactory.openSession();
        dao.setSession(session);

        Merra2Metadata merra2Metadata = null;
        FileStatus sampleFile = fileList.get(0);
        String collectionName = collectionNameFromFileName(sampleFile.getPath().getName());
        System.out.println("collection name: " + collectionName);
        JaxbMerra2Collection jaxbMerra2Collection = jaxbMerra2CollectionHashMap.get(collectionName);
        try {
          merra2Metadata = addCollectionToDb(sampleFile.getPath(), jaxbMerra2Collection, fileSystem, dao);
        } catch (InvalidRangeException e) {
          e.printStackTrace();
        }

        for(FileStatus file: fileList) {
            if(merra2Metadata != null) {
                String temporalKey = temporalKeyFromFileName(file.getPath().getName());
                Merra2FilePathMetadata merraFilePathMetadata = buildMerra2FilePathMetadata(merra2Metadata, temporalKey, file.getPath().toString());
                dao.insert(merraFilePathMetadata);
              }
        }

        session.close();
    }

    /**
     * Add collection to db.
     *
     * @param file                 the file
     * @param jaxbMerra2Collection the jaxb merra 2 collection
     * @param fileSystem           the file system
     * @param dao                  the dao
     * @throws IOException        the io exception
     * @throws JAXBException      the jaxb exception
     * @throws XMLStreamException the xml stream exception
     */
    protected Merra2Metadata addCollectionToDb(Path file, JaxbMerra2Collection jaxbMerra2Collection, FileSystem fileSystem, DAOImpl dao)
        throws IOException, JAXBException, XMLStreamException, InvalidRangeException {
        FileStatus fileStatus = fileSystem.getFileStatus(file);
        MerraRandomAccessFile randomAccessFile = new MerraRandomAccessFile(fileStatus, new Configuration());
        NetcdfFile netcdfFile = NetcdfFile.open(randomAccessFile, fileStatus.getPath().toString());

        List<Variable> variableList = netcdfFile.getVariables();
        Merra2Metadata merra2Metadata = buildMerra2Metadata(jaxbMerra2Collection);
        dao.insert(merra2Metadata);

        for(Variable variable: variableList) {
            List<Dimension> dimensionList = variable.getDimensions();

            List<DimensionAndShape> dimensionAndShapeList = buildDimensionAndShapeList(variable.getShape(), dimensionList);

            if(dimensionList.size() > MIN_NUMBER_DIMENSIONS) {
                Merra2VariableMetadata merra2VariableMetadata = new Merra2VariableMetadata();
                ArrayList<Attribute> attributeList = (ArrayList) variable.getAttributes();
                SiaVariableCompositeKey siaVariableCompositeKey = new SiaVariableCompositeKey();
                siaVariableCompositeKey.setVariableName(variable.getShortName());
                siaVariableCompositeKey.setCollectionName(merra2Metadata.getCollectionName());
                merra2VariableMetadata.setSiaVariableCompositeKey(siaVariableCompositeKey);
                merra2VariableMetadata.setSiaMetadata(merra2Metadata);
                merra2VariableMetadata.setSpatialResolution(buildSpatialResolutionList(dimensionAndShapeList));
                merra2VariableMetadata.setTemporalResolution(buildTemporalResolutionList(dimensionAndShapeList));
                merra2VariableMetadata.setDimensionOrder(buildDimensionOrderList(dimensionList));
                merra2VariableMetadata.setChunkSizes(getChunkSize(variable));
                merra2VariableMetadata.setValidMin(findValidMin(attributeList));
                merra2VariableMetadata.setValidMax(findValidMax(attributeList));
                merra2VariableMetadata.setMissingValue(findMissingValue(attributeList));
                merra2VariableMetadata.setFillValue(findFillValue(attributeList));
                merra2VariableMetadata.setAddOffset(findAddOffset(attributeList));
                merra2VariableMetadata.setScaleFactor(findScaleFactor(attributeList));
                merra2VariableMetadata.setUnits(variable.getUnitsString());
                merra2VariableMetadata.setDataType(variable.getDataType().toString());

                merra2Metadata.getSiaVariableMetadataSet().add(merra2VariableMetadata);
                dao.insert(merra2VariableMetadata);
            }
        }
        return merra2Metadata;
    }


    public List<Integer> getChunkSize(Variable variable) throws IOException, InvalidRangeException {
      String metaInfo = variable.getVarLocationInformation();
      String cnr = metaInfo.substring(0, metaInfo.indexOf("  ChunkedDataNode"));
      String[] cnrs = cnr.split(",");
      int corners[] = new int[cnrs.length];
      int shape[] = new int[cnrs.length];
      List<Integer> chunkSize = new ArrayList<Integer>();
      for (int i=0; i<cnrs.length; i++) {
        String[] r = cnrs[i].split(":");
        corners[i] = Integer.parseInt(r[0]);
        shape[i] = Integer.parseInt(r[1]) - Integer.parseInt(r[0]) + 1;
        chunkSize.add(shape[i]);
      }
      return chunkSize;
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
        return splitName[1].toLowerCase();
    }

    /**
     * Parse xml file jaxb merra 2 collections.
     *
     * @return the jaxb merra 2 collections
     * @throws JAXBException      the jaxb exception
     * @throws XMLStreamException the xml stream exception
     */
    protected JaxbMerra2Collections parseXmlFile() throws JAXBException, XMLStreamException {
        XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();
        StreamSource streamSource = new StreamSource(getClass().getResourceAsStream(this.getXmlFile()));
        getClass().getResourceAsStream(this.getXmlFile());
        XMLStreamReader xmlStreamReader = xmlInputFactory.createXMLStreamReader(streamSource);
        JaxbMerra2Collections jaxbMerra2Collections;
        JAXBContext jaxbContext = JAXBContext.newInstance(JaxbMerra2Collections.class);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        JAXBElement<JaxbMerra2Collections> jaxbElement = unmarshaller.unmarshal(xmlStreamReader, JaxbMerra2Collections.class);
        jaxbMerra2Collections = jaxbElement.getValue();

        return jaxbMerra2Collections;
    }

    /**
     * Array list to hash map hash map.
     *
     * @param jaxbMerra2Collections the jaxb merra 2 collections
     * @return the hash map
     */
    protected HashMap<String, JaxbMerra2Collection> arrayListToHashMap(JaxbMerra2Collections jaxbMerra2Collections) {
        ArrayList<JaxbMerra2Collection> jaxbMerra2CollectionArrayList = jaxbMerra2Collections.getJaxbMerra2Collections();
        HashMap<String, JaxbMerra2Collection> jaxbMerra2CollectionHashMap = new HashMap<String, JaxbMerra2Collection>(jaxbMerra2CollectionArrayList.size());

        for(JaxbMerra2Collection jaxbMerra2Collection : jaxbMerra2CollectionArrayList) {
            jaxbMerra2CollectionHashMap.put(jaxbMerra2Collection.getName().toLowerCase(), jaxbMerra2Collection);
        }

        return jaxbMerra2CollectionHashMap;
    }

    /**
     * Build merra 2 metadata merra 2 metadata.
     *
     * @param jaxbMerra2Collection the jaxb merra 2 collection
     * @return the merra 2 metadata
     */
    protected Merra2Metadata buildMerra2Metadata(JaxbMerra2Collection jaxbMerra2Collection) {
        Merra2Metadata merra2Metadata = new Merra2Metadata();
        merra2Metadata.setCollectionName(jaxbMerra2Collection.getName());
        merra2Metadata.setTemporalResolution(jaxbMerra2Collection.getTemporalResolution());
        merra2Metadata.setTemporalRangeStart(jaxbMerra2Collection.getTemporalRangeStart());
        merra2Metadata.setTemporalRangeEnd(jaxbMerra2Collection.getTemporalRangeEnd());
        merra2Metadata.setRawDataFormat(".nc4");
        merra2Metadata.setStatisticalIntervalType(jaxbMerra2Collection.getStatisticalIntervalType());

        return merra2Metadata;
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

        /*chunkSizeList.add(findLongitude(dimensionAndShapeList).intValue());
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
     * Build merra file path metadata merra file path metadata.
     *
     * @param merra2Metadata the merra metadata
     * @param temporalKey   the temporal key
     * @param filePath      the file path
     * @return the merra file path metadata
     */
    protected Merra2FilePathMetadata buildMerra2FilePathMetadata(Merra2Metadata merra2Metadata, String temporalKey, String filePath) {
      Merra2FilePathMetadata merra2FilePathMetadata = new Merra2FilePathMetadata();
      SiaFilePathCompositeKey siaFilePathCompositeKey = new SiaFilePathCompositeKey();
      siaFilePathCompositeKey.setCollectionName(merra2Metadata.getCollectionName());
      siaFilePathCompositeKey.setTemporalKey(Integer.parseInt(temporalKey));
      merra2FilePathMetadata.setSiaFilePathCompositeKey(siaFilePathCompositeKey);
      merra2FilePathMetadata.setSiaMetadata(merra2Metadata);
      merra2FilePathMetadata.setFilePath(filePath);

      return merra2FilePathMetadata;
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
     * The type Jaxb merra 2 collections.
     */
    @XmlRootElement(name = "merra2Collections")
    protected static class JaxbMerra2Collections {
        private ArrayList<JaxbMerra2Collection> jaxbMerra2Collections;

        /**
         * Gets jaxb merra 2 collections.
         *
         * @return the jaxb merra 2 collections
         */
        public ArrayList<JaxbMerra2Collection> getJaxbMerra2Collections() {
            return jaxbMerra2Collections;
        }

        /**
         * Sets jaxb merra 2 collections.
         *
         * @param jaxbMerra2Collections the jaxb merra 2 collections
         */
        @XmlElement(name = "collection")
        public void setJaxbMerra2Collections(ArrayList<JaxbMerra2Collection> jaxbMerra2Collections) {
            this.jaxbMerra2Collections = jaxbMerra2Collections;
        }
    }

    /**
     * The type Jaxb merra 2 collection.
     */
    @XmlRootElement(name = "collection")
    protected static class JaxbMerra2Collection {
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
