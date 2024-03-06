package gocuckoo

func New(cfg *LayeredCuckooFilterConfig) *LayeredCuckooFilterImpl {
	return &LayeredCuckooFilterImpl{
		localCuckooFilter: cfg.Filters,
		source:            NewSourceCuckooFilter(cfg.Rdb, cfg.Log,cfg.WatchKey,cfg.ReadStreamMessageBlock,cfg.ReadStreamMessageSize),
		log:               cfg.Log,
	}

}
