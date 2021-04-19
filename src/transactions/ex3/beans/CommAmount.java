package transactions.ex3.beans;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommAmount implements Writable {

    private String comm;
    private Double amount;

    public CommAmount() {
    }

    public CommAmount(String comm, Double amount) {
        this.comm = comm;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "CommAmount{" +
                "comm=" + comm +
                ", amount=" + amount +
                '}';
    }

    public String getComm() {
        return comm;
    }

    public void setComm(String comm) {
        this.comm = comm;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(getComm());
        dataOutput.writeDouble(getAmount());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        setComm(dataInput.readUTF());
        setAmount(dataInput.readDouble());
    }
}
