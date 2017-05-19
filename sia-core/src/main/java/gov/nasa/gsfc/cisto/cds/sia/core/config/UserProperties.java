package gov.nasa.gsfc.cisto.cds.sia.core.config;

/**
 * The type User properties.
 */
public class UserProperties {

    private final String datasetName;
    private final String collectionName;
    private final String jobName;
    private final String inputPath;
    private final String outputPath;
    private final String fileExtension;
    private final String[] variableNames;
    private final String mapreduceFrameworkName;
    private final String xmlHibernateTableMappingFilePath;
    private final int filesPerMapTask;
    private final int numberReducers;
    private final int threadsPerNode;
    private final String analyticsOperation;

    /**
     * Instantiates a new User properties.
     *
     * @param builder the builder
     */
    public UserProperties(UserPropertiesBuilder builder) {
        this.datasetName = builder.datasetName;
        this.collectionName = builder.collectionName;
        this.jobName = builder.jobName;
        this.inputPath = builder.inputPath;
        this.outputPath = builder.outputPath;
        this.fileExtension = builder.fileExtension;
        this.variableNames = builder.variableNames;
        this.mapreduceFrameworkName = builder.mapreduceFrameworkName;
        this.xmlHibernateTableMappingFilePath = builder.xmlHibernateTableMappingFilePath;
        this.filesPerMapTask = builder.filesPerMapTask;
        this.numberReducers = builder.numberReducers;
        this.threadsPerNode = builder.threadsPerNode;
        this.analyticsOperation = builder.analyticsOperation;
    }

    /**
     * Gets dataset name.
     *
     * @return the dataset name
     */
    public String getDatasetName() {
        return datasetName;
    }

    /**
     * Gets collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Gets job name.
     *
     * @return the job name
     */
    public String getJobName() {
        return jobName;
    }

    /**
     * Gets input path.
     *
     * @return the input path
     */
    public String getInputPath() {
        return inputPath;
    }

    /**
     * Gets output path.
     *
     * @return the output path
     */
    public String getOutputPath() {
        return outputPath;
    }

    /**
     * Gets file extension.
     *
     * @return the file extension
     */
    public String getFileExtension() { return fileExtension; }

    /**
     * Get variable names string [ ].
     *
     * @return the string [ ]
     */
    public String[] getVariableNames() {
        return variableNames;
    }

    /**
     * Gets mapreduce framework name.
     *
     * @return the mapreduce framework name
     */
    public String getMapreduceFrameworkName() {
        return mapreduceFrameworkName;
    }

    /**
     * Gets xml hibernate table mapping file path.
     *
     * @return the xml hibernate table mapping file path
     */
    public String getXmlHibernateTableMappingFilePath() {
        return xmlHibernateTableMappingFilePath;
    }

    /**
     * Gets files per map task.
     *
     * @return the files per map task
     */
    public int getFilesPerMapTask() {
        return filesPerMapTask;
    }

    /**
     * Gets number reducers.
     *
     * @return the number reducers
     */
    public int getNumberReducers() {
        return numberReducers;
    }

    /**
     * Gets threads per node.
     *
     * @return the threads per node
     */
    public int getThreadsPerNode() {
        return threadsPerNode;
    }

    /**
     * Gets analytics operation.
     *
     * @return the analytics operation
     */
    public String getAnalyticsOperation() {
        return analyticsOperation;
    }

    @Override
    public String toString() {
        return "UserProperties{" +
                "datasetName='" + datasetName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", jobName='" + jobName + '\'' +
                ", inputPath='" + inputPath + '\'' +
                ", outputPath='" + outputPath + '\'' +
                ", fileExtension='" + fileExtension + '\'' +
                ", variableNames=" + variableNames +
                ", mapreduceFrameworkName='" + mapreduceFrameworkName + '\'' +
                ", xmlHibernateTableMappingFilePath='" + xmlHibernateTableMappingFilePath + '\'' +
                ", filesPerMapTask=" + filesPerMapTask +
                ", numberReducers=" + numberReducers +
                ", threadsPerNode=" + threadsPerNode +
                ", analyticsOperation='" + analyticsOperation + '\'' +
                '}';
    }

    /**
     * The type User properties builder.
     */
    public static class UserPropertiesBuilder {
        private final String datasetName;
        private final String jobName;
        private String collectionName;
        private String inputPath;
        private String outputPath;
        private String fileExtension;
        private String[] variableNames;
        private String mapreduceFrameworkName;
        private String xmlHibernateTableMappingFilePath;
        private int filesPerMapTask;
        private int numberReducers;
        private int threadsPerNode;
        private String analyticsOperation;

        /**
         * Instantiates a new User properties builder.
         *
         * @param datasetName the dataset name
         * @param jobName     the job name
         */
        public UserPropertiesBuilder(String datasetName, String jobName) {
            this.datasetName = datasetName;
            this.jobName = jobName;
        }

        /**
         * Collection name user properties builder.
         *
         * @param collectionName the collection name
         * @return the user properties builder
         */
        public UserPropertiesBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Input path user properties builder.
         *
         * @param inputPath the input path
         * @return the user properties builder
         */
        public UserPropertiesBuilder inputPath(String inputPath) {
            this.inputPath = inputPath;
            return this;
        }

        /**
         * Output path user properties builder.
         *
         * @param outputPath the output path
         * @return the user properties builder
         */
        public UserPropertiesBuilder outputPath(String outputPath) {
            this.outputPath = outputPath;
            return this;
        }

        /**
         * File extension user properties builder.
         *
         * @param fileExtension the file extension
         * @return the user properties builder
         */
        public UserPropertiesBuilder fileExtension(String fileExtension) {
            this.fileExtension = fileExtension;
            return this;
        }

        /**
         * Variable names user properties builder.
         *
         * @param variableNames the variable names
         * @return the user properties builder
         */
        public UserPropertiesBuilder variableNames(String[] variableNames) {
            this.variableNames = variableNames;
            return this;
        }

        /**
         * Mapreduce framework name user properties builder.
         *
         * @param mapreduceFrameworkName the mapreduce framework name
         * @return the user properties builder
         */
        public UserPropertiesBuilder mapreduceFrameworkName(String mapreduceFrameworkName) {
            this.mapreduceFrameworkName = mapreduceFrameworkName;
            return this;
        }

        /**
         * Xml hibernate table mapping file path user properties builder.
         *
         * @param xmlHibernateTableMappingFilePath the xml hibernate table mapping file path
         * @return the user properties builder
         */
        public UserPropertiesBuilder xmlHibernateTableMappingFilePath(String xmlHibernateTableMappingFilePath) {
            this.xmlHibernateTableMappingFilePath = xmlHibernateTableMappingFilePath;
            return this;
        }

        /**
         * Files per map task user properties builder.
         *
         * @param filesPerMapTask the files per map task
         * @return the user properties builder
         */
        public UserPropertiesBuilder filesPerMapTask(int filesPerMapTask) {
            this.filesPerMapTask = filesPerMapTask;
            return this;
        }

        /**
         * Number reducers user properties builder.
         *
         * @param numberReducers the number reducers
         * @return the user properties builder
         */
        public UserPropertiesBuilder numberReducers(int numberReducers) {
            this.numberReducers = numberReducers;
            return this;
        }

        /**
         * Threads per node user properties builder.
         *
         * @param threadsPerNode the threads per node
         * @return the user properties builder
         */
        public UserPropertiesBuilder threadsPerNode(int threadsPerNode) {
            this.threadsPerNode = threadsPerNode;
            return this;
        }

        /**
         * Analytics operation user properties builder.
         *
         * @param analyticsOperation the analytics operation
         * @return the user properties builder
         */
        public UserPropertiesBuilder analyticsOperation(String analyticsOperation) {
            this.analyticsOperation = analyticsOperation;
            return this;
        }

        /**
         * Build user properties.
         *
         * @return the user properties
         */
        public UserProperties build() {
            UserProperties userProperties = new UserProperties(this);
            //validateUserProps(userProperties);
            return userProperties;
        }

        private void validateUserProps(UserProperties userProperties) {
            throw new UnsupportedOperationException("Validation of user input properties not implemented yet.");
        }
    }
}