package transactions.ex5;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class UnitTypeYearCategory implements WritableComparable<UnitTypeYearCategory> {
    private String unitType;
    private Integer year;
    private String category;

    public UnitTypeYearCategory() {
    }

    @Override
    public String toString() {
        return "UnitTypeYearCategory{" +
                "unitType='" + unitType + '\'' +
                ", year=" + year +
                ", category='" + category + '\'' +
                '}';
    }

    public UnitTypeYearCategory(String unitType, Integer year, String category) {
        this.unitType = unitType;
        this.year = year;
        this.category = category;
    }

    public String getUnitType() {
        return unitType;
    }

    public void setUnitType(String unitType) {
        this.unitType = unitType;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UnitTypeYearCategory that = (UnitTypeYearCategory) o;
        return Objects.equals(unitType, that.unitType) && Objects.equals(year, that.year) && Objects.equals(category, that.category);
    }

    @Override
    public int hashCode() {
        return Objects.hash(unitType, year, category);
    }

    @Override
    public int compareTo(UnitTypeYearCategory o) {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(getUnitType());
        dataOutput.writeUTF(getCategory());
        dataOutput.writeInt(getYear());

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        setUnitType(dataInput.readUTF());
        setCategory(dataInput.readUTF());
        setYear(dataInput.readInt());
    }
}
