package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	engine_util "github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	kvEngine := engine_util.CreateDB(conf.DBPath, false)

	return &StandAloneStorage{
		engine: engine_util.NewEngines(kvEngine, nil, conf.DBPath, ""),
	}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.engine.Close()
}

type StandAloneStorageReader struct {
	tx *badger.Txn
}

var _ storage.StorageReader = (*StandAloneStorageReader)(nil)

func NewStandAloneStorageReader(tx *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		tx: tx,
	}
}

func (sr *StandAloneStorageReader) Close() {
	sr.tx.Discard()
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(sr.tx, cf, key)
	if err != nil {
		return nil, nil
	}
	return val, nil
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.tx)
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return NewStandAloneStorageReader(s.engine.Kv.NewTransaction(false)), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, b := range batch {
		switch b.Data.(type) {
		case storage.Put:
			if err := engine_util.PutCF(s.engine.Kv, b.Cf(), b.Key(), b.Value()); err != nil {
				return err
			}
		case storage.Delete:
			if err := engine_util.DeleteCF(s.engine.Kv, b.Cf(), b.Key()); err != nil {
				return err
			}
		default:
			return errors.New("Invalid operation")
		}
	}
	return nil
}
