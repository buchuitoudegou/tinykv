package standalone_storage

import (
	badger "github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	Db  *badger.DB
	Opt badger.Options
}

type StandAloneReader struct {
	Txn *badger.Txn
}

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.Txn, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
	}
	return val, nil
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.Txn)
}

func (s *StandAloneReader) Close() {
	s.Txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	s := &StandAloneStorage{
		Db:  nil,
		Opt: badger.DefaultOptions,
	}
	s.Opt.Dir = conf.DBPath
	s.Opt.ValueDir = conf.DBPath
	return s
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// if _, err := os.Stat(s.Opt.Dir); os.IsNotExist(err) {
	// 	os.Mkdir(s.Opt.Dir, 0666)
	// }
	db, err := badger.Open(s.Opt)
	if err != nil {
		return err
	}
	s.Db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.Db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.Db.NewTransaction(true)
	return &StandAloneReader{
		Txn: txn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, item := range batch {
		cf, key, val := item.Cf(), item.Key(), item.Value()
		switch item.Data.(type) {
		case storage.Put:
			engine_util.PutCF(s.Db, cf, key, val)
		case storage.Delete:
			engine_util.DeleteCF(s.Db, cf, key)
		}
	}
	return nil
}
