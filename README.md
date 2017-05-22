# ClimateSpark
Scala Kernels:
 
	   Reduce ops:
- max :  "Computes maximum element value from input variable data over specified axes and roi"
- min :  "Computes minimum element value from input variable data over specified axes and roi"
- sum :  "Computes sum of elements value from input variable data over specified axes and roi"
- average: "Computes (weighted) means of element values from input variable data over specified axes and roi"
 
	   Combine Ops:
- min2 :  "Computes element-wise minimum values for a pair of input variables data over specified roi"
- max2 :  "Computes element-wise maximum values for a pair of input variables data over specified roi"
- sum2 :  "Computes element-wise sum values for a pair of input variables data over specified roi"
- diff2 :  "Computes element-wise difference values for a pair of input variables data over specified roi"
- mult2 :  "Computes element-wise products for a pair of input variables data over specified roi"
- div2 :  "Computes element-wise divisions for a pair of input variables data over specified roi"
- multiAve: "Computes point-by-point average over inputs within specified ROI"
 
  Scala Utilites:
 
- Subset:  “Extract ROI from dataset”
- Binning:  “Partition data into space/time bins, e.g. monthly, seasonal, etc”
 
 
  Python Kernels:
 
	   Numpy  implementations:
	- “std", "Standard Deviation", "Computes the standard deviation of the array elements along the given axes."
	- "max", "Maximum", "Computes the maximun of the array elements along the given axes."
	- "min", "Minimum", "Computes the minimun of the array elements along the given axes."
	- "sum", "Sum","Computes the sum of the array elements along the given axes."
	- "ave", "Average Kernel","Computes the average of the array elements along the given axes."
	- "avew", "Weighted Average Kernel","Computes the weighted average of the array elements along the given axes."
 
	   UVCDAT implementations:
	- “regrid", "Regridder", "Regrids the inputs using UVCDAT"
	- "ave", "Average", "Averages the inputs using UVCDAT (with area weighting by default) over specified axes and roi "
	- "zaDemo", "ZonalAverageDemo", "Zonal average from -90 to 90 (as in Potter demo)"
