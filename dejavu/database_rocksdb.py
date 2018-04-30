import rocksdb
import uuid
from dejavu.database import Database
import pickle

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

SONG_SID = 0
SONG_SONG_NAME = 1
SONG_IS_FINGERPRINTED = 2
SONG_FILE_SHA1 = 3
SONG_FINGERPRINTS = 4

FP_SID = 0
FP_SONG_NAME = 1
FP_OFFSET = 2

class RocksdbDatabase(Database):
    """
    2 type datas:
        songs:
            key: song_{uuid}, uuid = uuid.uuid(uuid.NAMESPACE_DNS, sid)
            value: 
            [sid,track_id,is_fingerprinted,file_sha1,[(hash,offset),(hash,offset),...]]
            

        fingerprints:
            key: {fingerprint_hash}
            value: 
            [(sid,track_id,offset),(sid,track_id,offset),...]
    """
    type = "rocksdb"

    def __init__(self, **options):
        super(RocksdbDatabase, self).__init__()
        # init rocksdb instrance
        self.db = rocksdb.DB(DATABASE_NAME, opts)
        exist, v = self.db.key_may_exist(bytes('sid_counter'))
        if not exist:
            self.db.put(bytes('sid_counter'), bytes(1))

    def get_db_instance(self):
        return self.db

    def _new_sid(self):
        new_sid = int(self.db.get(bytes('sid_counter')))

        next_sid = new_sid + 1
        self.db.put(bytes('sid_counter'), bytes(next_sid))
        return new_sid
    
    def _gen_song_key(self, sid):
        return bytes('s_'+ str(uuid.uuid3(uuid.NAMESPACE_DNS, str(sid))))

    def _decode_song_details(self, value):
        return pickle.loads(value)

    def _get_song_details(self, sid):
        value = self.db.get(self._gen_song_key(sid))
        if value:
            return self._decode_song_details(value)
        else:
            return None

    def _set_song_details(self, song_details):
        """
        song_details: [sid,track_id,is_fingerprinted,file_sha1,[(hash,offset),(hash,offset),...]]
        """
        sid = song_details[SONG_SID]
        self.db.put(self._gen_song_key(sid), bytes(pickle.dumps(song_details)))

    def _gen_fingerprint_key(self, hash):
        return bytes('fp_' + hash)

    def _set_fingerprint_details(self, hash, fingerprint):
        """
        fingerprint: [(sid,track_id,offset),(sid,track_id,offset),...]
        """
        self.db.put(self._gen_fingerprint_key(hash), bytes(pickle.dumps(fingerprint)))

    def _get_fingerprint_details(self, hash):
        """
        return [(sid,track_id,offset),(sid,track_id,offset),...]
        """
        value = self.db.get(self._gen_fingerprint_key(hash))
        if value:
            return pickle.loads(value)
        else:
            return []

    def _encode_fingerprint_value(self, sid, offset):
        return ','.join([str(sid), str(offset)])

    def _song_details_4_dejavu(self, song_details):
        return dict(
            song_id=song_details[SONG_SID],
            song_name=song_details[SONG_SONG_NAME],
            file_sha1=song_details[SONG_FILE_SHA1]
        )
    
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
            if key.startswith('s_'):
                song_details = self._decode_song_details(value)
                if song_details[SONG_IS_FINGERPRINTED] == 0:
                    self.db.delete(key)

    def get_num_songs(self):
        pass

    def get_num_fingerprints(self):
        pass

    def set_song_fingerprinted(self, sid):
        song_details = self._get_song_details(sid)
        if song_details:
            if song_details[SONG_IS_FINGERPRINTED] == 0:
                # update 
                song_details[SONG_IS_FINGERPRINTED] = 1
                self._set_song_details(song_details)
            return True
        else:
            # no existing record
            return False

    def get_songs(self):
        items = self.db.iteritems()
        items.seek_to_first()

        for song_key, value in items:
            if song_key.startswith('s_'):
                song_details = self._decode_song_details(value)
                if song_details[SONG_IS_FINGERPRINTED] == 1:
                    yield self._song_details_4_dejavu(song_details)
    
    def get_song_by_id(self, sid):
        song_details = self._get_song_details(sid)
        if song_details:
            return self._song_details_4_dejavu(song_details)
        else:
            return None

    def insert(self, hash, sid, offset):
        """
        insert a fingerprint into database
        """
        # 1. update song detail
        song_details = self._get_song_details(sid)
        assert song_details is not None
        if song_details[SONG_FINGERPRINTS].count((hash, offset)) == 0:
            # new fingerprint
            song_details[SONG_FINGERPRINTS].append((hash, offset))
            self._set_song_details(song_details)

        # 2. update fingerprints
        track_id = song_details[SONG_SONG_NAME]
        fingerprint_details = self._get_fingerprint_details(hash)
        fingerprint_record = (sid, track_id, offset)
        if fingerprint_details.count(fingerprint_record) == 0:
            # new fingerprint record
            fingerprint_details.append(fingerprint_record)
            self._set_fingerprint_details(hash, fingerprint_details)

    def insert_hashes(self, sid, hashes):
        print "insert_hahes, sid[%s], len_of_hashes[%s]" % (sid, len(hashes))
        for hash, offset in hashes:
            self.insert(hash, sid, offset)

    def insert_song(self, song_name, file_hash):
        sid = self._new_sid()
        song_details = [
            sid,
            song_name,
            0,
            file_hash,
            []
        ]
        self._set_song_details(song_details)
        return sid

    def query(self, hash):
        """
        query fingerprint
        """
        fingerprint_details = self._get_fingerprint_details(hash)
        for it in fingerprint_details:
            yield (it[FP_SID], it[FP_OFFSET])

    def get_iterable_kv_pairs(self):
        pass
    
    def return_matches(self, hashes):
        for hash, offset in hashes:
            fp_details = self._get_fingerprint_details(hash)
            for it in fp_details:
                yield (it[FP_SID], offset - it[FP_OFFSET])
