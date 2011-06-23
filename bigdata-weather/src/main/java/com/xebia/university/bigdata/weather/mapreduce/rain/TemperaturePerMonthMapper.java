package com.xebia.university.bigdata.weather.mapreduce.rain;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.xebia.university.bigdata.weather.KnmiHourlyLineParser;
import com.xebia.university.bigdata.weather.KnmiLineParser.KnmiLineType;

public class TemperaturePerMonthMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final Logger LOG = Logger.getLogger(TemperaturePerMonthMapper.class);

    private KnmiHourlyLineParser parser = new KnmiHourlyLineParser();

    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
        try {
            if (parser.parse(line) == KnmiLineType.DATA) {
                context.write(keyForStationAndDate(), valueForTemperature());
            }
        } catch (NumberFormatException e) {
            // skip line
        } catch (Exception e) {
            LOG.warn("Exception caught during call to map(): ", e);
        }
    }

    private Text keyForStationAndDate() {
        return new Text(parser.getStation() + "," + parser.getDate().substring(0, 6));
    }

    private LongWritable valueForTemperature() {
        return new LongWritable(parser.getTemperature());
    }
}
