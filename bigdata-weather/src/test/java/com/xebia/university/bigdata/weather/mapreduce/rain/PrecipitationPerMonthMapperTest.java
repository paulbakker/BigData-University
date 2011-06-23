package com.xebia.university.bigdata.weather.mapreduce.rain;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class PrecipitationPerMonthMapperTest {
    private MapDriver<LongWritable, Text, Text, LongWritable> driver;
    private List<Pair<Text, LongWritable>> output;

    @Before
    public void setUp() throws Exception {
        driver = new MapDriver<LongWritable, Text, Text, LongWritable>(new PrecipitationPerMonthMapper());
    }

    @Test
    public void shouldExtractDataFromMeasurementLine() throws Exception {
        output = driver.withInputKey(new LongWritable(0))
                       .withInputValue(new Text("  391,20110101,   12,  290,   30,   20,   60,   27,    7,   27,    0,   13,    8,    4,     ,     ,     ,  100,     ,     ,     ,     ,     ,"))
                       .run();

        assertThat(output.size(), is(1));
        assertThat(output.get(0).getFirst(), equalTo(new Text("391,201101")));
        assertThat(output.get(0).getSecond(), equalTo(new LongWritable(4)));
    }

    @Test
    public void shouldNotExtractEmptyLine() throws Exception {
        output = driver.withInputKey(new LongWritable(0))
                       .withInputValue(new Text("")).run();

        assertThat(output.size(), is(0));
    }

    @Test
    public void shouldNotExtractCommentLine() throws Exception {
        output = driver.withInputKey(new LongWritable(0))
                       .withInputValue(new Text("# THESE DATA CAN BE USED FREELY PROVIDED THAT THE FOLLOWING SOURCE IS ACKNOWLEDGED:"))
                       .run();

        assertThat(output.size(), is(0));
    }

    @Test
    public void shouldExtractDataWithNeglectablePrecipitationAsZero() throws Exception {
        output = driver.withInputKey(new LongWritable(0))
                       .withInputValue(new Text("  391,20110101,   12,  290,   30,   20,   60,   27,    7,   27,    0,   13,    8,   -1,     ,     ,     ,  100,     ,     ,     ,     ,     ,"))
                       .run();

        assertThat(output.size(), is(1));
        assertThat(output.get(0).getFirst(), equalTo(new Text("391,201101")));
        assertThat(output.get(0).getSecond(), equalTo(new LongWritable(0)));
    }
}
