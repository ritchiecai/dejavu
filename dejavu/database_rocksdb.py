import rocksdb
import uuid
from dejavu.database import Database

DATABASE_NAME = "dejavu"

opts = rocksdb.Options()
opts.create_if_missing = True
opts.db_log_dir = ''
opts.wal_dir = ''
opts.max_log_file_size = 0
opts.keep_log_file_num = 1000
opts.write_buffer_size = 4194304 # 67108864
opts.max_write_buffer_number = 2
opts.target_file_size_base = 2097152 # 67108864

opts.table_factory = rocksdb.BlockBasedTableFactory(
    filter_policy=rocksdb.BloomFilterPolicy(10),
    block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
    block_cache_compressed=rocksdb.LRUCache(500 * (1024 ** 2)))

class RocksdbDatabase(Database):
    """
    2 type datas:
        songs:
            key: song_{uuid}, uuid = uuid.uuid(uuid.NAMESPACE_DNS, sid)
            value: {sid},{song_name},{is_fingerprinted},{file_sha1}

        fingerprints:
            key: {fingerprint_hash}
            value: {sid},{offset}
    """
    type = "rocksdb"

    def __init__(self, **options):
        super(RocksdbDatabase, self).__init__()
        # init rocksdb instrance
        self.db = rocksdb.DB(DATABASE_NAME, opts)
        if not self.db.key_may_exist(bytes('sid_counter')):
            self.db.put(bytes('sid_counter'), bytes(1))

    def _new_sid(self):
        new_sid = int(self.db.get(bytes('sid_counter')))

        next_sid = new_sid + 1
        self.db.put(bytes('sid_counter'))
        return new_sid
    
    def _gen_song_key(self, sid):
        return uuid.uuid3(uuid.NAMESPACE_DNS, str(sid))

    def _encode_song_value(self, sid, song_name, is_fingerprinted, file_sha1):
        return ','.join([str(sid), str(song_name), str(is_fingerprinted), str(file_sha1)])
    
    def _decode_song_value(self, value):
        li = value.split(',', 3)
        return int(li[0]), li[1], int(li[2]), li[3]

    def _encode_fingerprint_value(self, sid, offset):
        return ','.join([str(sid), str(offset)])
    
    def _decode_fingerprint_value(self, value):
        li = value.split(',', 1)
        return int(li[0]), int(li[1])

    def empty(self):
        """
        Drops tables.
        seems rocksdb python don't have this
        """
        pass
    
    def delete_unfingerprinted_songs(self):
        """
        """
        items = self.db.iteritems()
        items.seek_to_first()
        
        for key, value in items:
            is_fingerprinted = value.split(',', 1)[1]
            if is_fingerprinted == 0:
                db.delete(key)

    def get_num_songs(self):
        pass

    def get_num_fingerprints(self):
        pass

    def set_song_fingerprinted(self, sid):
        song_key = self._gen_song_key(sid)
        value = self.db.get(bytes(song_key))
        if value:
            sid, song_name, is_fingerprinted, file_sha1 = self._decode_song_value(value)
            if is_fingerprinted == 0:
                # update 
                self.db.put(bytes(song_key), bytes(self._encode_song_value(sid, song_name, 1, file_sha1)))
            return True
        else:
            # no existing record
            return False

    def get_songs(self):
        items = self.db.iteritems()
        items.seek_to_first()

        for song_key, value in items:
            sid, song_name, is_fingerprinted, file_sha1 = self._decode_song_value(value)
            if is_fingerprinted == 1:
                yield (sid, song_name, is_fingerprinted, file_sha1)
    
    def get_song_by_id(self, sid):
        song_key = self._gen_song_key(sid)
        value = self.db.get(bytes(song_key))
        if value:
            return self._decode_song_value(value)

    def insert(self, hash, sid, offset):
        value = self._encode_fingerprint_value(sid, offset)
        self.db.put(bytes(hash), bytes(value))

    def insert_song(self, song_name, file_hash):
        sid = self._new_sid()
        song_key = self._gen_song_key(sid)
        value = self._encode_song_value(sid, song_name, 0, file_hash)
        self.db.put(bytes(song_key), bytes(value))
        return sid

    def query(self, hash):
        
