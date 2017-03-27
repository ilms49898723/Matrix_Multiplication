package com.github.ilms49898723.matrixmultiplication;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MatrixIndex implements WritableComparable<MatrixIndex> {
    private int mI;
    private int mJ;

    public MatrixIndex() {
        mI = 0;
        mJ = 0;
    }

    public MatrixIndex(int i, int j) {
        mI = i;
        mJ = j;
    }

    public int getI() {
        return mI;
    }

    public void setI(int i) {
        this.mI = i;
    }

    public int getJ() {
        return mJ;
    }

    public void setJ(int j) {
        this.mJ = j;
    }

    @Override
    public int compareTo(MatrixIndex o) {
        if (Integer.compare(this.getI(), o.getI()) != 0) {
            return Integer.compare(this.getI(), o.getI());
        } else {
            return Integer.compare(this.getJ(), o.getJ());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MatrixIndex that = (MatrixIndex) o;

        if (mI != that.mI) {
            return false;
        }
        return mJ == that.mJ;

    }

    @Override
    public int hashCode() {
        int result = mI;
        result = 31 * result + mJ;
        return result;
    }

    public static MatrixIndex read(DataInput dataInput) throws IOException {
        MatrixIndex matrixIndex = new MatrixIndex();
        matrixIndex.readFields(dataInput);
        return matrixIndex;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(mI);
        dataOutput.writeInt(mJ);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        mI = dataInput.readInt();
        mJ = dataInput.readInt();
    }
}
