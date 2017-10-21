package gov.nasa.gsfc.cisto.cds.sia.core.config;

/**
 * Created by Fei Hu on 5/25/17.
 */
public class ClimateSparkConfig {

  private String dataset_name;
  private String collection_name;
  private String job_name;
  private String input_path;
  private String output_path;
  private String xml_hibernate_table_mapping_file_path;
  private String variable_names;
  private String file_extension;
  private String mapreduce_framework_name;


  private String analytics_operation;
  private String year_start;
  private String year_end;
  private String month_start;
  private String month_end;
  private String day_start;
  private String day_end;
  private String hour_start;
  private String hour_end;

  private String lat_start;
  private String lat_end;
  private String lon_start;
  private String lon_end;
  private String height_start = "-1";
  private String height_end = "-1";

  private String files_per_map_task = "7";
  private String threads_per_node = "1";
  private String number_reducers = "1";

  public ClimateSparkConfig(String dataset_name, String collection_name, String job_name,
                            String input_path, String output_path, String xml_hibernate_table_mapping_file_path,
                            String variable_names, String file_extension, String mapreduce_framework_name,
                            String analytics_operation, String year_start, String year_end,
                            String month_start, String month_end, String day_start, String day_end,
                            String hour_start, String hour_end,
                            String lat_start, String lat_end,
                            String lon_start, String lon_end) {
    this.dataset_name = dataset_name;
    this.collection_name = collection_name;
    this.job_name = job_name;
    this.input_path = input_path;
    this.output_path = output_path;
    this.xml_hibernate_table_mapping_file_path = xml_hibernate_table_mapping_file_path;
    this.variable_names = variable_names;
    this.file_extension = file_extension;
    this.mapreduce_framework_name = mapreduce_framework_name;
    this.analytics_operation = analytics_operation;
    this.year_start = year_start;
    this.year_end = year_end;
    this.month_start = month_start;
    this.month_end = month_end;
    this.day_start = day_start;
    this.day_end = day_end;
    this.hour_start = hour_start;
    this.hour_end = hour_end;
    this.lat_start = lat_start;
    this.lat_end = lat_end;
    this.lon_start = lon_start;
    this.lon_end = lon_end;
  }

  public String getDataset_name() {
    return dataset_name;
  }

  public void setDataset_name(String dataset_name) {
    this.dataset_name = dataset_name;
  }

  public String getCollection_name() {
    return collection_name;
  }

  public void setCollection_name(String collection_name) {
    this.collection_name = collection_name;
  }

  public String getJob_name() {
    return job_name;
  }

  public void setJob_name(String job_name) {
    this.job_name = job_name;
  }

  public String getInput_path() {
    return input_path;
  }

  public void setInput_path(String input_path) {
    this.input_path = input_path;
  }

  public String getOutput_path() {
    return output_path;
  }

  public void setOutput_path(String output_path) {
    this.output_path = output_path;
  }

  public String getXml_hibernate_table_mapping_file_path() {
    return xml_hibernate_table_mapping_file_path;
  }

  public void setXml_hibernate_table_mapping_file_path(
      String xml_hibernate_table_mapping_file_path) {
    this.xml_hibernate_table_mapping_file_path = xml_hibernate_table_mapping_file_path;
  }

  public String getVariable_names() {
    return variable_names;
  }

  public void setVariable_names(String variable_names) {
    this.variable_names = variable_names;
  }

  public String getFile_extension() {
    return file_extension;
  }

  public void setFile_extension(String file_extension) {
    this.file_extension = file_extension;
  }

  public String getMapreduce_framework_name() {
    return mapreduce_framework_name;
  }

  public void setMapreduce_framework_name(String mapreduce_framework_name) {
    this.mapreduce_framework_name = mapreduce_framework_name;
  }

  public String getAnalytics_operation() {
    return analytics_operation;
  }

  public void setAnalytics_operation(String analytics_operation) {
    this.analytics_operation = analytics_operation;
  }

  public String getYear_start() {
    return year_start;
  }

  public void setYear_start(String year_start) {
    this.year_start = year_start;
  }

  public String getYear_end() {
    return year_end;
  }

  public void setYear_end(String year_end) {
    this.year_end = year_end;
  }

  public String getMonth_start() {
    return month_start;
  }

  public void setMonth_start(String month_start) {
    this.month_start = month_start;
  }

  public String getMonth_end() {
    return month_end;
  }

  public void setMonth_end(String month_end) {
    this.month_end = month_end;
  }

  public String getDay_start() {
    return day_start;
  }

  public void setDay_start(String day_start) {
    this.day_start = day_start;
  }

  public String getDay_end() {
    return day_end;
  }

  public void setDay_end(String day_end) {
    this.day_end = day_end;
  }

  public String getHour_start() {
    return hour_start;
  }

  public void setHour_start(String hour_start) {
    this.hour_start = hour_start;
  }

  public String getHour_end() {
    return hour_end;
  }

  public void setHour_end(String hour_end) {
    this.hour_end = hour_end;
  }

  public String getLat_start() {
    return lat_start;
  }

  public void setLat_start(String lat_start) {
    this.lat_start = lat_start;
  }

  public String getLat_end() {
    return lat_end;
  }

  public void setLat_end(String lat_end) {
    this.lat_end = lat_end;
  }

  public String getLon_start() {
    return lon_start;
  }

  public void setLon_start(String lon_start) {
    this.lon_start = lon_start;
  }

  public String getLon_end() {
    return lon_end;
  }

  public void setLon_end(String lon_end) {
    this.lon_end = lon_end;
  }

  public String getHeight_start() {
    return height_start;
  }

  public void setHeight_start(String height_start) {
    this.height_start = height_start;
  }

  public String getHeight_end() {
    return height_end;
  }

  public void setHeight_end(String height_end) {
    this.height_end = height_end;
  }

  public String getFiles_per_map_task() {
    return files_per_map_task;
  }

  public void setFiles_per_map_task(String files_per_map_task) {
    this.files_per_map_task = files_per_map_task;
  }

  public String getThreads_per_node() {
    return threads_per_node;
  }

  public void setThreads_per_node(String threads_per_node) {
    this.threads_per_node = threads_per_node;
  }

  public String getNumber_reducers() {
    return number_reducers;
  }

  public void setNumber_reducers(String number_reducers) {
    this.number_reducers = number_reducers;
  }
}
