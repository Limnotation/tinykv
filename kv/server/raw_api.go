package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}

	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil || value == nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}

	return &kvrpcpb.RawGetResponse{Value: value}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modifyPut := storage.Modify{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		},
	}
	if err := server.storage.Write(req.Context, []storage.Modify{modifyPut}); err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, err
	}
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modifyDelete := storage.Modify{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	}
	if err := server.storage.Write(req.Context, []storage.Modify{modifyDelete}); err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, err
	}
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	kvPairs := make([]*kvrpcpb.KvPair, 0)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
	}

	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	counter := 0
	for counter < int(req.Limit) && iter.Valid() {
		item := iter.Item()
		key := item.Key()
		value, err := item.Value()
		if err != nil {
			return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
		}
		kvPairs = append(kvPairs, &kvrpcpb.KvPair{Key: key, Value: value})
		iter.Next()
		counter++
	}

	return &kvrpcpb.RawScanResponse{Kvs: kvPairs}, nil
}
