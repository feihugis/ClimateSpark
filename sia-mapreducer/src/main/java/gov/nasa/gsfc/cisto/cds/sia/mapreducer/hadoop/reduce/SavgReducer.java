package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.reduce;

import gov.nasa.gsfc.cisto.cds.sia.core.io.value.FloatArrayWritable;
import gov.nasa.gsfc.cisto.cds.sia.core.io.value.FloatMatrixWritable;
import org.apache.commons.math3.fraction.Fraction;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jblas.FloatMatrix;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.Math.abs;

/**
 * Created by mkbowen on 6/2/16.
 */
public class SavgReducer extends Reducer<Text, FloatArrayWritable, Text, FloatMatrixWritable> {
    private FloatMatrixWritable outputMatrix;
    private Text outputKey;
    private ConcurrentHashMap<float[], float[]> hm;
    private float validMax = 1.0E30F; //CF compliant
    private float validMin = -1.0E30F; //CF compliant
    private float missingDataFillValue = 9.9999999E14F; //CF compliant
    Fraction latStep = Fraction.ONE_HALF;
    Fraction lonStep = Fraction.TWO_THIRDS;
    float latStart = -90f;
    float latStop = 90.5f;
    float lonStart = -180f;
    float lonStop = 180f;

    float[] values;
    int[] sums;

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    public void reduce(Text key, Iterable<FloatArrayWritable> value, Reducer<Text, FloatArrayWritable, Text, FloatMatrixWritable>.Context context)
            throws IOException, InterruptedException {

        /**
        hm = new ConcurrentHashMap<float[], float[]>();
        float[] latitudes = range(latStart, latStop, latStep);
        float[] longitudes = range(lonStart, lonStop, lonStep);


        for(FloatArrayWritable floatArray : value) {
            hashFloatArray(floatArray.getValues(), latitudes, longitudes);

            for(int i = 0; i < floatArray.getValues().length; i++) {
                System.out.println("Level " + key.toString().substring(2, key.toString().length()) + " Position " + i + " Value " + floatArray.getValues()[i]);
            }

        }
        FloatMatrix output = buildOutputMatrix();


        outputKey = new Text("Variable: " + key.toString().substring(0, 1) + " Level: " + key.toString().substring(2, key.toString().length()));
        outputMatrix = new FloatMatrixWritable(output);
        context.write(outputKey, outputMatrix);
*/
        // ---------------------------------------------------------------

        values = new float[194940];
        sums = new int[194940];

        for(FloatArrayWritable floatArray : value) {
            float[] currentArray = floatArray.getValues();

            for(int i = 0; i < currentArray.length; i++) {
                if((currentArray[i] <= validMax) && (currentArray[i] >= validMin) && (currentArray[i] != missingDataFillValue)) {
                    values[i] += currentArray[i];
                    sums[i] += 1;
                }
            }
        }

        for(int j = 0; j < values.length; j++) {
            int sum = sums[j];
            if(sum != 0) {
                values[j] /= sum;
            } else {
                values[j] = 9.9999999E14f;
            }
        }
        FloatMatrix tempOutputMatrix = new FloatMatrix(values);
        outputKey = new Text("Variable: " + key.toString().substring(0, 1) + " Level: " + key.toString().substring(2, key.toString().length()) + "\n");
        outputMatrix = new FloatMatrixWritable(tempOutputMatrix);
        context.write(outputKey, outputMatrix);
    }



    private void hashFloatArray(float[] floatArray, float[] latitudes, float[] longitudes) {
        int mMatrixCols = 540;
        int nMatrixCols = 361;

        for(int k = 0; k < floatArray.length; k++) {
            if((floatArray[k] <= validMax) && (floatArray[k] >= validMin) && (floatArray[k] != missingDataFillValue)) {
                float[] latLonKeyArray = new float[2];
                float[] sumCountValueArray;
                int iMatrixPos = k / mMatrixCols;
                int jMatrixPos = k % mMatrixCols;
                float lat = latitudes[iMatrixPos];
                float lon = longitudes[jMatrixPos];
                latLonKeyArray[0] = lat;
                latLonKeyArray[1] = lon;

                if(hm.containsKey(latLonKeyArray)) {
                    sumCountValueArray = hm.get(latLonKeyArray);
                    float oldVal = sumCountValueArray[0];
                    sumCountValueArray[0] = (oldVal + floatArray[k]);
                    float oldIter = sumCountValueArray[1];
                    sumCountValueArray[1] = (oldIter + 1F);
                    hm.replace(latLonKeyArray, sumCountValueArray);
                } else {
                    sumCountValueArray = new float[2];
                    sumCountValueArray[0] = floatArray[k];
                    sumCountValueArray[1] = 1f;
                    hm.put(latLonKeyArray, sumCountValueArray);
                }
            }
        }

/**
        for(List<Float> key: hm.keySet()){
            String keyOut = "Key " + key.get(0) + " " + key.get(1);
            String valOut = " Value " + hm.get(key).get(0) + " " + hm.get(key).get(1);
            System.out.println(keyOut);
        }
*/

    }

    private FloatMatrix buildOutputMatrix() {
        FloatMatrix averages = FloatMatrix.zeros(361, 540);
        for(float[] key: hm.keySet()) {
            int[] matrixPositions = decryptKey(key);
            float average = hm.get(key)[0] / hm.get(key)[1];
            averages.put(matrixPositions[0], matrixPositions[1], average);
        }

        return averages;
    }

    private int[] decryptKey(float[] latLonKey) {
        int[] matrixPositions = new int[2];

        matrixPositions[0] = latStep.reciprocal().multiply(new Fraction(latLonKey[0] + abs(latStart))).intValue();
        matrixPositions[1] = lonStep.reciprocal().multiply(new Fraction(latLonKey[1] + abs(lonStart))).intValue();

        return matrixPositions;
    }

    private float[] range(float start, float stop, Fraction step) {
        int arraySize = step.reciprocal().multiply(new Fraction(abs(stop - start))).intValue();
        float[] rangeValues = new float[arraySize];

        for(int i = 0; i < arraySize; i++) {
            rangeValues[i] = (step.multiply(i).add(new Fraction (start))).floatValue();
        }

        return rangeValues;
    }
}
