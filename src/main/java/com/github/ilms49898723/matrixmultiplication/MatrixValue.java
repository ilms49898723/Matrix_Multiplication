package com.github.ilms49898723.matrixmultiplication;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MatrixValue implements Writable {
    private int mMatrix;
    private int mIndex;
    private int mValue;

    public MatrixValue() {
        mMatrix = 0;
        mIndex = 0;
        mValue = 0;
    }

    public MatrixValue(int matrix, int index, int value) {
        mMatrix = matrix;
        mIndex = index;
        mValue = value;
    }

    public int getMatrix() {
        return mMatrix;
    }

    public void setMatrix(int matrix) {
        this.mMatrix = matrix;
    }

    public int getIndex() {
        return mIndex;
    }

    public void setIndex(int index) {
        this.mIndex = index;
    }

    public int getValue() {
        return mValue;
    }

    public void setValue(int value) {
        this.mValue = value;
    }

    public static MatrixValue read(DataInput dataInput) throws IOException {
        MatrixValue matrixValue = new MatrixValue();
        matrixValue.readFields(dataInput);
        return matrixValue;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(mMatrix);
        dataOutput.writeInt(mIndex);
        dataOutput.writeInt(mValue);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.mMatrix = dataInput.readInt();
        this.mIndex = dataInput.readInt();
        this.mValue = dataInput.readInt();
    }
}
