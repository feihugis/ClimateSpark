package gov.nasa.gsfc.cisto.cds.sia.core.config;

import java.util.ArrayList;

/**
 * The type Spatiotemporal filters.
 */
public class SpatiotemporalFilters {

    private final int yearStart;
    private final int yearEnd;
    private final int monthStart;
    private final int monthEnd;
    private final int dayStart;
    private final int dayEnd;
    private final int hourStart;
    private final int hourEnd;
    private final int heightStart;
    private final int heightEnd;
    private final int latStart;
    private final int latEnd;
    private final int lonStart;
    private final int lonEnd;

    private SpatiotemporalFilters(SpatiotemporalFiltersBuilder builder) {
        this.yearStart = builder.yearStart;
        this.yearEnd = builder.yearEnd;
        this.monthStart = builder.monthStart;
        this.monthEnd = builder.monthEnd;
        this.dayStart = builder.dayStart;
        this.dayEnd = builder.dayEnd;
        this.hourStart = builder.hourStart;
        this.hourEnd = builder.hourEnd;
        this.heightStart = builder.heightStart;
        this.heightEnd = builder.heightEnd;
        this.latStart = builder.latStart;
        this.latEnd = builder.latEnd;
        this.lonStart = builder.lonStart;
        this.lonEnd = builder.lonEnd;
    }

    /**
     * Gets start date.
     *
     * @return the start date
     */
    public int getStartDate() {
       String yearStartStr = (yearStart == -1) ? "" : String.format("%04d", yearStart);
       String monthStartStr = (monthStart == -1) ? "" : String.format("%02d", monthStart);
       String dayStartStr = (dayStart == -1) ? "" : String.format("%02d", dayStart);

       return Integer.parseInt(yearStartStr + monthStartStr + dayStartStr);
    }

    /**
     * Gets end date.
     *
     * @return the end date
     */
    public int getEndDate() {
       String yearEndStr = (yearEnd == -1) ? "" : String.format("%04d", yearEnd);
       String monthEndStr = (monthEnd == -1) ? "" : String.format("%02d", monthEnd);
       String dayEndStr = (dayEnd == -1) ? "" : String.format("%02d", dayEnd);

       return Integer.parseInt(yearEndStr + monthEndStr + dayEndStr);
    }

    public int[] getStartSpatialBounding() {
      ArrayList<Integer> bounding = new ArrayList<Integer>();
      if (hourStart != -1) bounding.add(hourStart);
      if (heightStart != -1) bounding.add(heightStart);
      if (latStart != -1) bounding.add(latStart);
      if (lonStart != -1) bounding.add(lonStart);
      int[] startBounding = new int[bounding.size()];
      for (int i = 0; i < bounding.size(); i++) {
        startBounding[i] = bounding.get(i);
      }

      return startBounding;
    }

  public int[] getEndSpatialBounding() {
    ArrayList<Integer> bounding = new ArrayList<Integer>();
    if (hourEnd != -1) bounding.add(hourEnd);
    if (heightEnd != -1) bounding.add(heightEnd);
    if (latEnd != -1) bounding.add(latEnd);
    if (lonEnd != -1) bounding.add(lonEnd);

    int[] endBounding = new int[bounding.size()];
    for (int i = 0; i < bounding.size(); i++) {
      endBounding[i] = bounding.get(i);
    }

    return endBounding;
  }

    /**
     * Gets year start.
     *
     * @return the year start
     */
    public int getYearStart() {
        return yearStart;
    }

    /**
     * Gets year end.
     *
     * @return the year end
     */
    public int getYearEnd() {
        return yearEnd;
    }

    /**
     * Gets month start.
     *
     * @return the month start
     */
    public int getMonthStart() {
        return monthStart;
    }

    /**
     * Gets month end.
     *
     * @return the month end
     */
    public int getMonthEnd() {
        return monthEnd;
    }

    /**
     * Gets day start.
     *
     * @return the day start
     */
    public int getDayStart() {
        return dayStart;
    }

    /**
     * Gets day end.
     *
     * @return the day end
     */
    public int getDayEnd() {
        return dayEnd;
    }

    /**
     * Gets hour start.
     *
     * @return the hour start
     */
    public int getHourStart() {
        return hourStart;
    }

    /**
     * Gets hour end.
     *
     * @return the hour end
     */
    public int getHourEnd() {
        return hourEnd;
    }

    /**
     * Gets height start.
     *
     * @return the height start
     */
    public int getHeightStart() {
        return heightStart;
    }

    /**
     * Gets height end.
     *
     * @return the height end
     */
    public int getHeightEnd() {
        return heightEnd;
    }

    /**
     * Gets lat start.
     *
     * @return the lat start
     */
    public int getLatStart() {
        return latStart;
    }

    /**
     * Gets lat end.
     *
     * @return the lat end
     */
    public int getLatEnd() {
        return latEnd;
    }

    /**
     * Gets lon start.
     *
     * @return the lon start
     */
    public int getLonStart() {
        return lonStart;
    }

    /**
     * Gets lon end.
     *
     * @return the lon end
     */
    public int getLonEnd() {
        return lonEnd;
    }

    @Override
    public String toString() {
        return "SpatiotemporalFilters{" +
                "yearStart=" + yearStart +
                ", yearEnd=" + yearEnd +
                ", monthStart=" + monthStart +
                ", monthEnd=" + monthEnd +
                ", dayStart=" + dayStart +
                ", dayEnd=" + dayEnd +
                ", hourStart=" + hourStart +
                ", hourEnd=" + hourEnd +
                ", heightStart=" + heightStart +
                ", heightEnd=" + heightEnd +
                ", latStart=" + latStart +
                ", latEnd=" + latEnd +
                ", lonStart=" + lonStart +
                ", lonEnd=" + lonEnd +
                '}';
    }

    /**
     * The type Spatiotemporal filters builder.
     */
    public static class SpatiotemporalFiltersBuilder {
        private int yearStart;
        private int yearEnd;
        private int monthStart;
        private int monthEnd;
        private int dayStart;
        private int dayEnd;
        private int hourStart;
        private int hourEnd;
        private int heightStart;
        private int heightEnd;
        private int latStart;
        private int latEnd;
        private int lonStart;
        private int lonEnd;

        /**
         * Instantiates a new Spatiotemporal filters builder.
         */
        public SpatiotemporalFiltersBuilder() {

        }

        /**
         * Build monthly filter spatiotemporal filters builder.
         *
         * @param yearStart  the year start
         * @param monthStart the month start
         * @param yearEnd    the year end
         * @param monthEnd   the month end
         * @return the spatiotemporal filters builder
         */
        public SpatiotemporalFiltersBuilder buildMonthlyFilter(String yearStart, String monthStart, String yearEnd, String monthEnd) {
          this.yearStart = Integer.parseInt(yearStart);
          this.monthStart = Integer.parseInt(monthStart);
          this.dayStart = -1;
          this.hourStart = -1;

          this.yearEnd = Integer.parseInt(yearEnd);
          this.monthEnd = Integer.parseInt(monthEnd);
          this.dayEnd = -1;
          this.hourEnd = -1;
          return this;
        }

        /**
         * Build daily filter spatiotemporal filters builder.
         *
         * @param yearStart  the year start
         * @param monthStart the month start
         * @param dayStart   the day start
         * @param yearEnd    the year end
         * @param monthEnd   the month end
         * @param dayEnd     the day end
         * @return the spatiotemporal filters builder
         */
        public SpatiotemporalFiltersBuilder buildDailyFilter(String yearStart, String monthStart, String dayStart, String yearEnd, String monthEnd, String dayEnd) {
        this.yearStart = Integer.parseInt(yearStart);
        this.monthStart = Integer.parseInt(monthStart);
        this.dayStart = Integer.parseInt(dayStart);
        this.hourStart = -1;

        this.yearEnd = Integer.parseInt(yearEnd);
        this.monthEnd = Integer.parseInt(monthEnd);
        this.dayEnd = Integer.parseInt(dayEnd);
        this.hourEnd = -1;
        return this;
      }

      /**
       * Build daily filter spatiotemporal filters builder.
       *
       * @param yearStart  the year start
       * @param monthStart the month start
       * @param dayStart   the day start
       * @param yearEnd    the year end
       * @param monthEnd   the month end
       * @param dayEnd     the day end
       * @return the spatiotemporal filters builder
       */
      public SpatiotemporalFiltersBuilder buildDailyFilter(String yearStart, String monthStart,
                                                           String dayStart, String hourStart,
                                                           String heightStart, String latStart,
                                                           String lonStart,
                                                           String yearEnd, String monthEnd,
                                                           String dayEnd, String hourEnd,
                                                           String heightEnd, String latEnd,
                                                           String lonEnd) {
        this.yearStart = Integer.parseInt(yearStart);
        this.monthStart = Integer.parseInt(monthStart);
        this.dayStart = Integer.parseInt(dayStart);
        this.hourStart = Integer.parseInt(hourStart);
        this.heightStart = Integer.parseInt(heightStart);
        this.latStart = Integer.parseInt(latStart);
        this.lonStart = Integer.parseInt(lonStart);

        this.yearEnd = Integer.parseInt(yearEnd);
        this.monthEnd = Integer.parseInt(monthEnd);
        this.dayEnd = Integer.parseInt(dayEnd);
        this.hourEnd = Integer.parseInt(hourEnd);
        this.heightEnd = Integer.parseInt(heightEnd);
        this.latEnd = Integer.parseInt(latEnd);
        this.lonEnd = Integer.parseInt(lonEnd);

        return this;
      }

        /**
         * Year start spatiotemporal filters builder.
         *
         * @param yearStart the year start
         * @return the spatiotemporal filters builder
         */
        public SpatiotemporalFiltersBuilder yearStart(String yearStart) {
            this.yearStart = Integer.parseInt(yearStart);
            return this;
        }

        /**
         * Year end spatiotemporal filters builder.
         *
         * @param yearEnd the year end
         * @return the spatiotemporal filters builder
         */
        public SpatiotemporalFiltersBuilder yearEnd(String yearEnd) {
            this.yearEnd = Integer.parseInt(yearEnd);
            return this;
        }

        /**
         * Month start spatiotemporal filters builder.
         *
         * @param monthStart the month start
         * @return the spatiotemporal filters builder
         */
        public SpatiotemporalFiltersBuilder monthStart(String monthStart) {
            this.monthStart = Integer.parseInt(monthStart);
            return this;
        }

        /**
         * Month end spatiotemporal filters builder.
         *
         * @param monthEnd the month end
         * @return the spatiotemporal filters builder
         */
        public SpatiotemporalFiltersBuilder monthEnd(String monthEnd) {
            this.monthEnd = Integer.parseInt(monthEnd);
            return this;
        }

        /**
         * Day start spatiotemporal filters builder.
         *
         * @param dayStart the day start
         * @return the spatiotemporal filters builder
         */
        public SpatiotemporalFiltersBuilder dayStart(String dayStart) {
            this.dayStart = Integer.parseInt(dayStart);
            return this;
        }

        /**
         * Day end spatiotemporal filters builder.
         *
         * @param dayEnd the day end
         * @return the spatiotemporal filters builder
         */
        public SpatiotemporalFiltersBuilder dayEnd(String dayEnd) {
            this.dayEnd = Integer.parseInt(dayEnd);
            return this;
        }

        /**
         * Hour start spatiotemporal filters builder.
         *
         * @param hourStart the hour start
         * @return the spatiotemporal filters builder
         */
        public SpatiotemporalFiltersBuilder hourStart(String hourStart) {
            this.hourStart = Integer.parseInt(hourStart);
            return this;
        }

        /**
         * Hour end spatiotemporal filters builder.
         *
         * @param hourEnd the hour end
         * @return the spatiotemporal filters builder
         */
        public SpatiotemporalFiltersBuilder hourEnd(String hourEnd) {
            this.hourEnd = Integer.parseInt(hourEnd);
            return this;
        }

        /**
         * Height start spatiotemporal filters builder.
         *
         * @param heightStart the height start
         * @return the spatiotemporal filters builder
         */
        public SpatiotemporalFiltersBuilder heightStart(String heightStart) {
            this.heightStart = Integer.parseInt(heightStart);
            return this;
        }

        /**
         * Height end spatiotemporal filters builder.
         *
         * @param heightEnd the height end
         * @return the spatiotemporal filters builder
         */
        public SpatiotemporalFiltersBuilder heightEnd(String heightEnd) {
            this.heightEnd = Integer.parseInt(heightEnd);
            return this;
        }

        /**
         * Lat start spatiotemporal filters builder.
         *
         * @param latStart the lat start
         * @return the spatiotemporal filters builder
         */
        public SpatiotemporalFiltersBuilder latStart(String latStart) {
            this.latStart = Integer.parseInt(latStart);
            return this;
        }

        /**
         * Lat end spatiotemporal filters builder.
         *
         * @param latEnd the lat end
         * @return the spatiotemporal filters builder
         */
        public SpatiotemporalFiltersBuilder latEnd(String latEnd) {
            this.latEnd = Integer.parseInt(latEnd);
            return this;
        }

        /**
         * Lon start spatiotemporal filters builder.
         *
         * @param lonStart the lon start
         * @return the spatiotemporal filters builder
         */
        public SpatiotemporalFiltersBuilder lonStart(String lonStart) {
            this.lonStart = Integer.parseInt(lonStart);
            return this;
        }

        /**
         * Lon end spatiotemporal filters builder.
         *
         * @param lonEnd the lon end
         * @return the spatiotemporal filters builder
         */
        public SpatiotemporalFiltersBuilder lonEnd(String lonEnd) {
            this.lonEnd = Integer.parseInt(lonEnd);
            return this;
        }

        /**
         * Build spatiotemporal filters.
         *
         * @return the spatiotemporal filters
         */
        public SpatiotemporalFilters build() {
            SpatiotemporalFilters spatiotemporalFilters = new SpatiotemporalFilters(this);
            //validateSpatiotemporalFilters(spatiotemporalFilters);
            return spatiotemporalFilters;
        }

        // TODO SpatiotemporalFilters validation, requires psql metadata
        private void validateSpatiotemporalFilters(SpatiotemporalFilters spatiotemporalFilters) {
            throw new UnsupportedOperationException("Spatiotemporal Filters validation operation not implemented yet");
        }
    }
}