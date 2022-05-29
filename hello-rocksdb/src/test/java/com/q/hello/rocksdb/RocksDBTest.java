package com.q.hello.rocksdb;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.rocksdb.*;
import org.rocksdb.util.Environment;
import org.rocksdb.util.SizeUnit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class RocksDBTest {

    private static final String DB_PATH_BASIC_TEST = "./data/";
    private static final String DB_PATH_CF_TEST = "./data-cf/";
    private static final String DB_PATH_TX_TEST = "./data-tx/";

    @Test
    public void basic() throws RocksDBException {
        Options options = new Options();
        BloomFilter bloomFilter = new BloomFilter(10);
        // ReadOptions readOptions = new ReadOptions().setFillCache(false);
        Statistics stats = new Statistics();
        final RateLimiter rateLimiter = new RateLimiter(10_000_000, 10_000, 10);

        options.setCreateIfMissing(true)
            .setStatistics(stats)
            .setWriteBufferSize(8 * SizeUnit.KB)
            .setMaxWriteBufferNumber(3)
            .setMaxBackgroundJobs(10)
            .setCompactionStyle(CompactionStyle.UNIVERSAL);

        if (!Environment.isWindows()) {
            options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
        }

        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        Cache cache = new LRUCache(64 * 1024, 6);
        tableConfig.setBlockCache(cache)
            .setFilterPolicy(bloomFilter)
            .setBlockSizeDeviation(5)
            .setBlockRestartInterval(10)
            .setCacheIndexAndFilterBlocks(true)
            .setBlockCacheCompressed(new LRUCache(64 * 1000, 10));
        options.setTableFormatConfig(tableConfig);
        options.setRateLimiter(rateLimiter);

        try (RocksDB db = RocksDB.open(options, DB_PATH_BASIC_TEST)) {
            List<byte[]> keys = new ArrayList<>();

            {
                keys.add("hello".getBytes(UTF_8));
                db.put("hello".getBytes(UTF_8), "world".getBytes(UTF_8));

                byte[] value = db.get("hello".getBytes(UTF_8));

                log.info("Get 'hello': " + new String(value, UTF_8));
            }

            try (final WriteOptions writeOptions = new WriteOptions()) {
                for (int i = 0; i < 10; i++) {
                    try (final WriteBatch batch = new WriteBatch()) {
                        for (int j = 1; j < 10; ++j) {
                            byte[] key = String.format("%dx%d", i, j).getBytes();
                            batch.put(key, String.format("%d", i * j).getBytes());
                            keys.add(key);
                        }
                        db.write(writeOptions, batch);
                    }
                }
            }

            log.info("multi get as list");
            List<byte[]> values = db.multiGetAsList(keys);
            for (int i = 0; i < keys.size(); i++) {
                byte[] key = keys.get(i);
                byte[] value = values.get(i);
                log.info(String.format("key:%s,value:%s", new String(key), (value != null ? new String(value) : null)));
            }

            log.info("new iterator");
            RocksIterator it = db.newIterator();
            for (it.seekToFirst(); it.isValid(); it.next()) {
                log.info(String.format("key:%s,value:%s",
                    new String(it.key()), new String(it.value())));
            }
        }
    }

    @Test
    public void custom_column_family() {
        log.info("测试自定义的列簇...");
        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeLevelStyleCompaction()) {
            String cfName = "cf";
            // list of column family descriptors, first entry must always be default column family
            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                new ColumnFamilyDescriptor(cfName.getBytes(), cfOpts)
            );

            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
            try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
                 final RocksDB db = RocksDB.open(dbOptions, DB_PATH_CF_TEST, cfDescriptors, cfHandles)) {

                ColumnFamilyHandle cfHandle = cfHandles.stream().filter(x -> {
                    try {
                        return (new String(x.getName())).equals(cfName);
                    } catch (RocksDBException e) {
                        return false;
                    }
                }).collect(Collectors.toList()).get(0);

                try {
                    // put and get from non-default column family
                    db.put(cfHandles.get(1), new WriteOptions(), "key".getBytes(), "value".getBytes());

                    // atomic write
                    try (final WriteBatch wb = new WriteBatch()) {
                        wb.put(cfHandles.get(0), "key2".getBytes(),
                            "value2".getBytes());
                        wb.put(cfHandles.get(1), "key3".getBytes(),
                            "value3".getBytes());
//                        wb.delete(cfHandles.get(1), "key".getBytes());
                        db.write(new WriteOptions(), wb);
                    }

                    log.info("newIterator方法获取");
                    //如果不传columnFamilyHandle，则获取默认的列簇，如果传了columnFamilyHandle，则获取指定列簇的
                    RocksIterator iter = db.newIterator(cfHandles.get(1));
                    for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                        log.info(String.format("key:%s,value:%s",
                            new String(iter.key()), new String(iter.value())));
                    }

                    // drop column family
                    db.dropColumnFamily(cfHandles.get(1));

                } finally {
                    for (final ColumnFamilyHandle handle : cfHandles) {
                        handle.close();
                    }
                }
            }
        } catch (RocksDBException e) {
            log.error("error while db", e);
        }
    }

    @Test
    public void transaction() {
        log.info("测试事务开始...");
        try (final Options options = new Options().setCreateIfMissing(true);
             final OptimisticTransactionDB txnDb = OptimisticTransactionDB.open(options, DB_PATH_TX_TEST)) {

            try (final WriteOptions writeOptions = new WriteOptions();
                 final ReadOptions readOptions = new ReadOptions()) {

                log.info("=========================================");
                log.info("Demonstrates \"Read Committed\" isolation");
                readCommitted(txnDb, writeOptions, readOptions);
                iteratorReadData(txnDb);

                log.info("=========================================");
                log.info("Demonstrates \"Repeatable Read\" (Snapshot Isolation) isolation");
                repeatableRead(txnDb, writeOptions, readOptions);
                iteratorReadData(txnDb);

                log.info("=========================================");
                log.info("Demonstrates \"Read Committed\" (Monotonic Atomic Views) isolation");
                readCommitted_monotonicAtomicViews(txnDb, writeOptions, readOptions);
                iteratorReadData(txnDb);
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    private static void iteratorReadData(RocksDB db) {
        log.info("newIterator方法获取");
        RocksIterator iter = db.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            log.info(String.format("key:%s,value:%s",
                new String(iter.key()), new String(iter.value())));
        }
    }

    /**
     * Demonstrates "Read Committed" isolation
     */
    private static void readCommitted(final OptimisticTransactionDB txnDb,
                                      final WriteOptions writeOptions, final ReadOptions readOptions)
        throws RocksDBException {
        final byte key1[] = "abc".getBytes(UTF_8);
        final byte value1[] = "def".getBytes(UTF_8);

        final byte key2[] = "xyz".getBytes(UTF_8);
        final byte value2[] = "zzz".getBytes(UTF_8);

        // Start a transaction
        try (final Transaction txn = txnDb.beginTransaction(writeOptions)) {
            // Read a key in this transaction
            byte[] value = txn.get(readOptions, key1);
            assert (value == null);

            // Write a key in this transaction
            txn.put(key1, value1);

            // Read a key OUTSIDE this transaction. Does not affect txn.
            value = txnDb.get(readOptions, key1);
            assert (value == null);

            // Write a key OUTSIDE of this transaction.
            // Does not affect txn since this is an unrelated key.
            // If we wrote key 'abc' here, the transaction would fail to commit.
            txnDb.put(writeOptions, key2, value2);

            // Commit transaction
            txn.commit();
        }
    }

    /**
     * Demonstrates "Repeatable Read" (Snapshot Isolation) isolation
     */
    private static void repeatableRead(final OptimisticTransactionDB txnDb,
                                       final WriteOptions writeOptions, final ReadOptions readOptions)
        throws RocksDBException {

        final byte key1[] = "ghi".getBytes(UTF_8);
        final byte value1[] = "jkl".getBytes(UTF_8);

        // Set a snapshot at start of transaction by setting setSnapshot(true)
        try (final OptimisticTransactionOptions txnOptions =
                 new OptimisticTransactionOptions().setSetSnapshot(true);
             final Transaction txn =
                 txnDb.beginTransaction(writeOptions, txnOptions)) {

            final Snapshot snapshot = txn.getSnapshot();

            // Write a key OUTSIDE of transaction
            txnDb.put(writeOptions, key1, value1);

            // Read a key using the snapshot.
            readOptions.setSnapshot(snapshot);
            final byte[] value = txn.getForUpdate(readOptions, key1, true);
            assert (value == null);

            try {
                // Attempt to commit transaction
                txn.commit();
                throw new IllegalStateException();
            } catch (final RocksDBException e) {
                // Transaction could not commit since the write outside of the txn
                // conflicted with the read!
                log.error("error while transaction", e);
                assert (e.getStatus().getCode() == Status.Code.Busy);
            }

            txn.rollback();
        } finally {
            // Clear snapshot from read options since it is no longer valid
            readOptions.setSnapshot(null);
        }
    }

    /**
     * Demonstrates "Read Committed" (Monotonic Atomic Views) isolation
     * <p>
     * In this example, we set the snapshot multiple times.  This is probably
     * only necessary if you have very strict isolation requirements to
     * implement.
     */
    private static void readCommitted_monotonicAtomicViews(
        final OptimisticTransactionDB txnDb, final WriteOptions writeOptions,
        final ReadOptions readOptions) throws RocksDBException {

        final byte keyX[] = "x".getBytes(UTF_8);
        final byte valueX[] = "x".getBytes(UTF_8);

        final byte keyY[] = "y".getBytes(UTF_8);
        final byte valueY[] = "y".getBytes(UTF_8);

        try (final OptimisticTransactionOptions txnOptions =
                 new OptimisticTransactionOptions().setSetSnapshot(true);
             final Transaction txn =
                 txnDb.beginTransaction(writeOptions, txnOptions)) {

            // Do some reads and writes to key "x"
            Snapshot snapshot = txnDb.getSnapshot();
            readOptions.setSnapshot(snapshot);
            byte[] value = txn.get(readOptions, keyX);
            txn.put(keyX, valueX);

            // Do a write outside of the transaction to key "y"
            txnDb.put(writeOptions, keyY, valueY);

            // Set a new snapshot in the transaction
            txn.setSnapshot();
            snapshot = txnDb.getSnapshot();
            readOptions.setSnapshot(snapshot);

            // Do some reads and writes to key "y"
            // Since the snapshot was advanced, the write done outside of the
            // transaction does not conflict.
            value = txn.getForUpdate(readOptions, keyY, true);
            txn.put(keyY, valueY);

            // Commit.  Since the snapshot was advanced, the write done outside of the
            // transaction does not prevent this transaction from Committing.
            txn.commit();

        } finally {
            // Clear snapshot from read options since it is no longer valid
            readOptions.setSnapshot(null);
        }
    }
}
