package cassandra.cache.test;

import com.googlecode.concurrentlinkedhashmap.Weighers;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cache.*;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.Pair;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.cassandra.Util.column;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNotSame;

/**
 * Created by fuyinlin on 5/30/17.
 */
public class CassandraCacheTest {

    MeasureableString key1 = new MeasureableString("key1");
    MeasureableString key2 = new MeasureableString("key2");
    MeasureableString key3 = new MeasureableString("key3");
    MeasureableString key4 = new MeasureableString("key4");
    MeasureableString key5 = new MeasureableString("key5");
    private static final long CAPACITY = 4;
    private static final String KEYSPACE1 = "CacheProviderTest1";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    private void simpleCase(ColumnFamily cf, ICache<MeasureableString, IRowCacheEntry> cache)
    {
        cache.put(key1, cf);

        if(cache.get(key1) == null)
            System.out.println("Getting key1 failed");


        //assertDigests(cache.get(key1), cf);
        cache.put(key2, cf);
        cache.put(key3, cf);
        cache.put(key4, cf);
        cache.put(key5, cf);

        if(cache.size() == CAPACITY) {
            System.out.println(cache.size());
            System.out.println("Capacity is correct");
        }


    }

    private void assertDigests(IRowCacheEntry one, ColumnFamily two)
    {
        // CF does not implement .equals
        assertTrue(one instanceof ColumnFamily);
        assertEquals(ColumnFamily.digest((ColumnFamily)one), ColumnFamily.digest(two));
    }

    // TODO this isn't terribly useful
    private void concurrentCase(final ColumnFamily cf, final ICache<MeasureableString, IRowCacheEntry> cache) throws InterruptedException
    {
        Runnable runable = new Runnable()
        {
            public void run()
            {
                for (int j = 0; j < 10; j++)
                {
                    cache.put(key1, cf);
                    cache.put(key2, cf);
                    cache.put(key3, cf);
                    cache.put(key4, cf);
                    cache.put(key5, cf);
                }
            }
        };

        List<Thread> threads = new ArrayList<Thread>(100);
        for (int i = 0; i < 100; i++)
        {
            Thread thread = new Thread(runable);
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads)
            thread.join();
    }

    private ColumnFamily createCF()
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, CF_STANDARD1);
        cf.addColumn(column("vijay", "great", 1));
        cf.addColumn(column("awesome", "vijay", 1));
        return cf;
    }


    public void testSerializingCache() throws InterruptedException
    {
        ICache<MeasureableString, IRowCacheEntry> cache = SerializingCache.create(CAPACITY, Weighers.<RefCountedMemory>singleton(), new SerializingCacheProvider.RowCacheSerializer());
        ColumnFamily cf = createCF();
        simpleCase(cf, cache);
        //concurrentCase(cf, cache);
    }


    public void testKeys()
    {
        Pair<String, String> ksAndCFName = Pair.create(KEYSPACE1, CF_STANDARD1);
        byte[] b1 = {1, 2, 3, 4};
        RowCacheKey key1 = new RowCacheKey(ksAndCFName, ByteBuffer.wrap(b1));
        byte[] b2 = {1, 2, 3, 4};
        RowCacheKey key2 = new RowCacheKey(ksAndCFName, ByteBuffer.wrap(b2));
        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());

        byte[] b3 = {1, 2, 3, 5};
        RowCacheKey key3 = new RowCacheKey(ksAndCFName, ByteBuffer.wrap(b3));
        assertNotSame(key1, key3);
        assertNotSame(key1.hashCode(), key3.hashCode());
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

    public static void main(String[] args) throws Exception{
        CassandraCacheTest test = new CassandraCacheTest();
        CassandraCacheTest.defineSchema();
        test.testSerializingCache();
    }
}





