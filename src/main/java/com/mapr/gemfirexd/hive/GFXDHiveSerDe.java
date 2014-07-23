package com.mapr.gemfirexd.hive;

import com.pivotal.gemfirexd.hadoop.mapred.Row;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * This assumes
 */
public class GFXDHiveSerDe extends AbstractSerDe {
    List<String> columnNames;
    List<TypeInfo> columnTypes;

    StructTypeInfo hiveRowTI;
    StructObjectInspector hiveRowOI;
    List<Object> hiveRow = new ArrayList<Object>();

    @Override
    public void initialize(org.apache.hadoop.conf.Configuration configuration, Properties tbl) throws SerDeException {
    // Get column names and types
        String colNamesList = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        String colTypeList = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

        // all table column names
        if (colNamesList.length() == 0) {
            columnNames = new ArrayList<String>();
        } else {
            columnNames = Arrays.asList(colNamesList.split(","));
        }

        // all column types
        if (colTypeList.length() == 0) {
            columnTypes = new ArrayList<TypeInfo>();
        } else {
            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypeList);
        }
        assert (columnNames.size() == columnTypes.size());

        // Create row related objects
        hiveRowTI = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
        hiveRowOI = (StructObjectInspector) TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(hiveRowTI);

    }

    @Override
    public Class<? extends org.apache.hadoop.io.Writable> getSerializedClass() {
        return null;
    }

    @Override
    public org.apache.hadoop.io.Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        return null;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public Object deserialize(org.apache.hadoop.io.Writable writable) throws SerDeException {
        try {
            hiveRow.clear();
            Row gfxdRow = (Row) writable;
            ResultSet gfxdResultSet = null;

            gfxdResultSet = gfxdRow.getRowAsResultSet();

            // Iterate over the Gemfire XD row and read the columns into Hive row.

            // Timestamp is a special field in Gemfire XD row that
            // can be used by hive to query records created between a time range.
            // EventType is another special field that can be used by Hive to filter out
            // deleted records.
            // These special fields need to be part of Hive schema.
            for (int i = 0; i < columnNames.size(); i++) {
                // read the timestamp and the event type from gfxdRow
                if (columnNames.get(i).equals("timestamp")){
                    hiveRow.add(gfxdRow.getTimestamp());
                }
                else if (columnNames.get(i).equals("eventtype")){
                    hiveRow.add(gfxdRow.getEventType().ordinal());
                }
                else if (columnTypes.get(i).getTypeName() == serdeConstants.INT_TYPE_NAME) {
                    hiveRow.add(gfxdResultSet.getInt(columnNames.get(i)));
                }
                else if (columnTypes.get(i).getTypeName() == serdeConstants.STRING_TYPE_NAME) {
                    hiveRow.add(gfxdResultSet.getString(columnNames.get(i)));
                }
                else if (columnTypes.get(i).getTypeName() == serdeConstants.TIMESTAMP_TYPE_NAME) {
                    hiveRow.add(gfxdResultSet.getTimestamp(columnNames.get(i)));
                }
                else if (columnTypes.get(i).getTypeName() == serdeConstants.BIGINT_TYPE_NAME) {
                    hiveRow.add(gfxdResultSet.getLong(columnNames.get(i)));
                }
                else if (columnTypes.get(i).getTypeName() == serdeConstants.BOOLEAN_TYPE_NAME) {
                    hiveRow.add(gfxdResultSet.getBoolean(columnNames.get(i)));
                }
                else if (columnTypes.get(i).getTypeName() == serdeConstants.DATE_TYPE_NAME) {
                    hiveRow.add(gfxdResultSet.getDate(columnNames.get(i)));
                }
                else if (columnTypes.get(i).getTypeName() == serdeConstants.CHAR_TYPE_NAME) {
                    hiveRow.add(gfxdResultSet.getString(columnNames.get(i)));
                }
                else if (columnTypes.get(i).getTypeName() == serdeConstants.DOUBLE_TYPE_NAME) {
                    hiveRow.add(gfxdResultSet.getDouble(columnNames.get(i)));
                }
                else {
                    throw new SerDeException("Got " + columnTypes.get(i).getTypeName() + ". As already said, only integer and string types are supported in my beautiful world");
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
//            throw new SerDeException("Failed while reading from GemFire XD row ", e);
        } catch (IOException e) {
            throw new SerDeException("Failed while reading from GemFire XD row ", e);
        } catch (Exception e) {
            throw new SerDeException("Failed while reading from GemFire XD row ", e);
        }
        return hiveRow;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return hiveRowOI;
    }
}
