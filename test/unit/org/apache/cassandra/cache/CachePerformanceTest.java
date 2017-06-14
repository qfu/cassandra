/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cache;

import com.googlecode.concurrentlinkedhashmap.Weighers;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.cassandra.Util.column;
import org.apache.cassandra.utils.Pair;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotSame;

/**
 * Created by sophiayan on 6/7/17.
 */
public class CachePerformanceTest
{

    MeasureableString key1 = new MeasureableString("key1");
    MeasureableString key2 = new MeasureableString("key2");
    MeasureableString key3 = new MeasureableString("key3");
    MeasureableString key4 = new MeasureableString("key4");
    MeasureableString key5 = new MeasureableString("key5");
    MeasureableString key6 = new MeasureableString("key6");
    MeasureableString key7 = new MeasureableString("key7");
    MeasureableString key8 = new MeasureableString("key8");
    private static final long CAPACITY = 5;
    private static final String KEYSPACE1 = "CachePerformanceTest";
    private static final String CF_STANDARD1 = "Standard1";


    // for concurrent test cases
    //public volatile List LRUConcurrentHitRate = Collections.synchronizedList(new ArrayList<Float>());
    //public volatile AtomicInteger hits = new AtomicInteger();
    //public volatile AtomicInteger testtimes = new AtomicInteger();



    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    public MeasureableString weightedRandom(List<MeasureableString> items, List<Integer> weights) {
        Double completeWeight = 0.0;

        int length = items.size();
        assert length == weights.size();
        for (int i = 0; i < length; i++) {
            completeWeight += weights.get(i);
        }
        double r = Math.random() * completeWeight;
        double countWeight = 0.0;
        for (int i = 0; i < length; i++) {
            countWeight += weights.get(i);
            if (countWeight >= r)
                return items.get(i);
        }
        throw new RuntimeException("Should never be shown.");
    }

    private void simpleCase(ColumnFamily cf, ICache<MeasureableString, IRowCacheEntry> cache)
    {
        String fileName = "hit-rates.txt";
        FileWriter writer = null;
        try {
            writer = new FileWriter(fileName);
            double hits = 0;
            double total = 0;

            List<MeasureableString> keys = Arrays.asList(key1, key2, key3, key4, key5, key6, key7, key8);
            List<Integer> weights = Arrays.asList(1, 1, 2, 3, 1, 1, 1, 1);

            int testNumbers = 10000;
            while (testNumbers-- > 0) {
                total++;
                MeasureableString curr = weightedRandom(keys, weights);
                if (cache.containsKey(curr)) {
                    hits++;
                    cache.get(curr);
                } else {
                    cache.put(curr, cf);
                }
                Double hitRate = hits / total;
                writer.write(hitRate.toString() + '\n');
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                writer.close();
            } catch (Exception ex) {
                /*ignore*/
            }
        }
    }

    private void assertDigests(IRowCacheEntry one, ColumnFamily two)
    {
        // CF does not implement .equals
        assertTrue(one instanceof ColumnFamily);
        assertEquals(ColumnFamily.digest((ColumnFamily)one), ColumnFamily.digest(two));
    }


    private ColumnFamily createCF()
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, CF_STANDARD1);
        cf.addColumn(column("vijay", "great", 1));
        cf.addColumn(column("awesome", "vijay", 1));
        return cf;
    }

    @Test
    public void testSerializingCache() throws InterruptedException
    {
        ICache<MeasureableString, IRowCacheEntry> cache = SerializingCache.create(CAPACITY, Weighers.<RefCountedMemory>singleton(), new SerializingCacheProvider.RowCacheSerializer());
        ColumnFamily cf = createCF();
        simpleCase(cf, cache);
    }


    private class MeasureableString implements IMeasurableMemory
    {
        public final String string;

        public MeasureableString(String input)
        {
            this.string = input;
        }

        public long unsharedHeapSize()
        {
            return string.length();
        }
    }



}
