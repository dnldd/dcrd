// Copyright (c) 2016 The btcsuite developers
// Copyright (c) 2016-2019 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

/*
Package indexers implements optional block chain indexes.
*/
package indexers

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/decred/dcrd/chaincfg/chainhash"
	"github.com/decred/dcrd/chaincfg/v3"
	"github.com/decred/dcrd/database/v2"
	"github.com/decred/dcrd/dcrutil/v3"
	"github.com/decred/dcrd/wire"
)

var (
	// byteOrder is the preferred byte order used for serializing numeric
	// fields for storage in the database.
	byteOrder = binary.LittleEndian

	// errInterruptRequested indicates that an operation was cancelled due
	// to a user-requested interrupt.
	errInterruptRequested = errors.New("interrupt requested")
)

// NeedsInputser provides a generic interface for an indexer to specify the it
// requires the ability to look up inputs for a transaction.
type NeedsInputser interface {
	NeedsInputs() bool
}

// PrevScripter defines an interface that provides access to scripts and their
// associated version keyed by an outpoint.  It is used within this package as a
// generic means to provide the scripts referenced by the inputs to transactions
// within a block that are needed to index it.  The boolean return indicates
// whether or not the script and version for the provided outpoint was found.
type PrevScripter interface {
	PrevScript(*wire.OutPoint) (uint16, []byte, bool)
}

// ChainQueryer provides a generic interface that is used to provide access to
// the chain details required by indexes.
//
// All functions MUST be safe for concurrent access.
type ChainQueryer interface {
	// MainChainHasBlock returns whether or not the block with the given hash is
	// in the main chain.
	MainChainHasBlock(*chainhash.Hash) bool

	// Best returns the height and hash of the current best block.
	Best() (int64, *chainhash.Hash)

	// BlockHashByHeight returns the hash of the block at the given height in
	// the main chain.
	BlockHashByHeight(int64) (*chainhash.Hash, error)

	// BlockByHash returns the block of the provided hash.
	BlockByHash(*chainhash.Hash) (*dcrutil.Block, error)

	// PrevScripts returns a source of previous transaction scripts and their
	// associated versions spent by the given block.
	PrevScripts(database.Tx, *dcrutil.Block) (PrevScripter, error)
}

// Indexer defines a generic interface for an indexer.
type Indexer interface {
	// Key returns the key of the index as a byte slice.
	Key() []byte

	// Name returns the human-readable name of the index.
	Name() string

	// Version returns the current version of the index.
	Version() uint32

	// DB returns the database of the index.
	DB() database.DB

	// Tip returns the current index tip.
	Tip() (int64, *chainhash.Hash, error)

	// Create is invoked when the indexer is being created.
	Create(dbTx database.Tx) error

	// Init is invoked when the index is being initialized.
	// This differs from the Create method in that it is called on
	// every load, including the case the index was just created.
	Init(ctx context.Context, chainParams *chaincfg.Params) error

	// ProcessNotification indexes the provided notification based on its
	// notification type.
	ProcessNotification(dbTx database.Tx, ntfn *IndexNtfn) error

	// WaitForSync subscribes clients for the next index sync update.
	WaitForSync() chan bool
}

// indexNeedsInputs returns whether or not the index needs access to
// the txouts referenced by the transaction inputs being indexed.
func indexNeedsInputs(index Indexer) bool {
	if idx, ok := index.(NeedsInputser); ok {
		return idx.NeedsInputs()
	}

	return false
}

// IndexDropper provides a method to remove an index from the database. Indexers
// may implement this for a more efficient way of deleting themselves from the
// database rather than simply dropping a bucket.
type IndexDropper interface {
	DropIndex(context.Context, database.DB) error
}

// AssertError identifies an error that indicates an internal code consistency
// issue and should be treated as a critical and unrecoverable error.
type AssertError string

// Error returns the assertion error as a huma-readable string and satisfies
// the error interface.
func (e AssertError) Error() string {
	return "assertion failed: " + string(e)
}

// errDeserialize signifies that a problem was encountered when deserializing
// data.
type errDeserialize string

// Error implements the error interface.
func (e errDeserialize) Error() string {
	return string(e)
}

// isDeserializeErr returns whether or not the passed error is an errDeserialize
// error.
func isDeserializeErr(err error) bool {
	_, ok := err.(errDeserialize)
	return ok
}

// internalBucket is an abstraction over a database bucket.  It is used to make
// the code easier to test since it allows mock objects in the tests to only
// implement these functions instead of everything a database.Bucket supports.
type internalBucket interface {
	Get(key []byte) []byte
	Put(key []byte, value []byte) error
	Delete(key []byte) error
}

// interruptRequested returns true when the provided channel has been closed.
// This simplifies early shutdown slightly since the caller can just use an if
// statement instead of a select.
func interruptRequested(ctx context.Context) bool {
	return ctx.Err() != nil
}

// tip returns the current tip hash and height of the provided index.
func tip(db database.DB, key []byte) (int64, *chainhash.Hash, error) {
	var hash *chainhash.Hash
	var height int32
	err := db.View(func(dbTx database.Tx) error {
		var err error
		hash, height, err = dbFetchIndexerTip(dbTx, key)
		return err
	})
	if err != nil {
		return 0, nil, err
	}
	return int64(height), hash, err
}

// recover reverts the index to a block on the main chain by repeatedly
// disconnecting the index tip if it is not on the main chain.
func recover(ctx context.Context, db database.DB, indexer Indexer, chain ChainQueryer) error {
	var cachedBlock *dcrutil.Block

	// Fetch the current tip for the index.
	height, hash, err := indexer.Tip()
	if err != nil {
		return err
	}

	// Nothing to do if the index does not have any entries yet.
	if height == 0 {
		return nil
	}

	var interrupted bool
	initialHeight := height
	err = db.Update(func(dbTx database.Tx) error {
		// Loop until the tip is a block that exists in the main chain.
		for !chain.MainChainHasBlock(hash) {
			// Get the block, unless it's already cached.
			var block *dcrutil.Block
			if cachedBlock == nil && height > 0 {
				block, err = chain.BlockByHash(hash)
				if err != nil {
					return err
				}
			} else {
				block = cachedBlock
			}

			// Load the parent block for the height since it is
			// required to remove it.
			parentHash := &block.MsgBlock().Header.PrevBlock
			parent, err := chain.BlockByHash(parentHash)
			if err != nil {
				return err
			}
			cachedBlock = parent

			// When the index requires all of the referenced
			// txouts they need to be retrieved from the
			// database.
			var prevScripts PrevScripter
			if indexNeedsInputs(indexer) {
				var err error
				prevScripts, err = chain.PrevScripts(dbTx, block)
				if err != nil {
					return err
				}
			}

			// Remove all of the index entries associated
			// with the block and update the indexer tip.
			err = indexer.ProcessNotification(dbTx, &IndexNtfn{
				NtfnType:    DisconnectNtfn,
				Block:       block,
				Parent:      parent,
				PrevScripts: prevScripts,
			})
			if err != nil {
				return err
			}

			// Update the tip to the previous block.
			hash = &block.MsgBlock().Header.PrevBlock
			height--

			// NOTE: This does not return as it does
			// elsewhere since it would cause the
			// database transaction to rollback and
			// undo all work that has been done.
			if interruptRequested(ctx) {
				interrupted = true
				break
			}
		}

		if initialHeight != height {
			log.Infof("Removed %d orphaned blocks from %s "+
				"(heights %d to %d)", initialHeight-height,
				indexer.Name(), height+1, initialHeight)
		}

		return nil
	})
	if err != nil {
		return err
	}
	if interrupted {
		return errInterruptRequested
	}
	return nil
}

// catchUp syncs the index to the the main chain by connecting blocks
// after its current tip to the current main chain tip. This should only
// be called for an index with its current tip on the main chain.
func catchUp(ctx context.Context, db database.DB, indexer Indexer, chain ChainQueryer) error {
	var cachedBlock *dcrutil.Block

	// Fetch the current tip for the index.
	height, hash, err := indexer.Tip()
	if err != nil {
		return err
	}

	var interrupted bool
	initialTipHeight := height
	err = db.Update(func(dbTx database.Tx) error {
		for {
			// Nothing to do if index is synced.
			bestHeight, _ := chain.Best()
			if bestHeight == height {
				return nil
			}

			nextTipHeight := height + 1
			nextTipHash, err := chain.BlockHashByHeight(nextTipHeight)
			if err != nil {
				return err
			}

			// Ensure the next tip hash is on the main chain.
			if !chain.MainChainHasBlock(nextTipHash) {
				return fmt.Errorf("the next block being synced to (%s) "+
					"at height %d is "+"not on the main chain.",
					nextTipHash, nextTipHeight)
			}

			var parent *dcrutil.Block
			if cachedBlock == nil {
				parent, err = chain.BlockByHash(hash)
				if err != nil {
					return err
				}
			} else {
				parent = cachedBlock
			}

			child, err := chain.BlockByHash(nextTipHash)
			if err != nil {
				return err
			}

			// When the index requires all of the referenced
			// txouts they need to be retrieved from the
			// database.
			var prevScripts PrevScripter
			if indexNeedsInputs(indexer) {
				var err error
				prevScripts, err = chain.PrevScripts(dbTx, child)
				if err != nil {
					return err
				}
			}

			// Add all of the index entries associated
			// with the block and update the index tip.
			err = indexer.ProcessNotification(dbTx, &IndexNtfn{
				NtfnType:    ConnectNtfn,
				Block:       child,
				Parent:      parent,
				PrevScripts: prevScripts,
			})
			if err != nil {
				return err
			}

			cachedBlock = child
			hash = nextTipHash
			height = nextTipHeight

			// NOTE: This does not return as it does
			// elsewhere since it would cause the
			// database transaction to rollback and
			// undo all work that has been done.
			if interruptRequested(ctx) {
				interrupted = true
				break
			}
		}

		if initialTipHeight != height {
			log.Infof("Synced %d blocks to %s "+
				"(heights %d to %d)", height-initialTipHeight,
				indexer.Name(), initialTipHeight, height)
		}

		return nil
	})
	if err != nil {
		return err
	}
	if interrupted {
		return errInterruptRequested
	}
	return nil
}

// finishDrop determines if the provided index is in the middle
// of being dropped and finishes dropping it when it is.  This is necessary
// because dropping an index has to be done in several atomic steps rather
// than one big atomic step due to the massive number of entries.
func finishDrop(ctx context.Context, db database.DB, indexer Indexer) error {
	var drop bool
	err := db.View(func(dbTx database.Tx) error {
		// The index does not need to be dropped if the index tips
		// bucket hasn't been created yet.
		indexesBucket := dbTx.Metadata().Bucket(indexTipsBucketName)
		if indexesBucket == nil {
			return nil
		}

		// Mark the indexer as requiring a drop if one is already in
		// progress.
		dropKey := indexDropKey(indexer.Key())
		if indexesBucket.Get(dropKey) != nil {
			drop = true
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Nothing to do if the index does not need dropping.
	if !drop {
		return nil
	}

	if interruptRequested(ctx) {
		return errInterruptRequested
	}

	log.Infof("Resuming %s drop", indexer.Name())

	switch d := indexer.(type) {
	case IndexDropper:
		err := d.DropIndex(ctx, db)
		if err != nil {
			return err
		}
	default:
		err := dropIndex(db, indexer.Key(), indexer.Name())
		if err != nil {
			return err
		}
	}

	return nil
}

// createIndex determines if each of the provided index has already
// been created and creates it if not.
func createIndex(db database.DB, genesisHash *chainhash.Hash, indexer Indexer) error {
	return db.Update(func(dbTx database.Tx) error {
		// Create the bucket for the current tips as needed.
		meta := dbTx.Metadata()
		indexesBucket, err := meta.CreateBucketIfNotExists(indexTipsBucketName)
		if err != nil {
			return err
		}

		// Nothing to do if the index tip already exists.
		idxKey := indexer.Key()
		if indexesBucket.Get(idxKey) != nil {
			return nil
		}

		// Store the index version.
		err = dbPutIndexerVersion(dbTx, idxKey, indexer.Version())
		if err != nil {
			return err
		}

		// The tip for the index does not exist, so create it and
		// invoke the create callback for the index so it can perform
		// any one-time initialization it requires.
		if err := indexer.Create(dbTx); err != nil {
			return err
		}

		// Set the tip for the index to values which represent an
		// uninitialized index (the genesis block hash and height).
		err = dbPutIndexerTip(dbTx, idxKey, genesisHash, 0)
		if err != nil {
			return err
		}

		return nil
	})
}

// upgradeIndex determines if the provided index needs to be upgraded.
// If it does it is dropped and recreeated.
func upgradeIndex(ctx context.Context, db database.DB, genesisHash *chainhash.Hash, indexer Indexer) error {
	if err := finishDrop(ctx, db, indexer); err != nil {
		return err
	}
	return createIndex(db, genesisHash, indexer)
}

// IndexNtfnType represents an index notification type.
type IndexNtfnType int

const (
	// ConnectNtfn indicates the index notification signals a block
	// connected to the main chain.
	ConnectNtfn IndexNtfnType = iota

	// DisconnectionNtfn indicates the index notification signals a block
	// disconnected from the main chain.
	DisconnectNtfn
)

// IndexNtfn represents an index notification detailing a block connecting
// or disconnecting from the main chain.
type IndexNtfn struct {
	NtfnType    IndexNtfnType
	Block       *dcrutil.Block
	Parent      *dcrutil.Block
	PrevScripts PrevScripter
}

// IndexSubscription represents a subscription for index updates.
type IndexSubscription struct {
	id         string
	idx        Indexer
	subscriber *IndexSubscriber
	c          chan *IndexNtfn
	mtx        sync.Mutex

	// prerequisite defines the notification processing hierarchy for this
	// subscription. It is expected that the subscriber associated with the
	// prerequisite provided processes notifications before they are
	// delivered by this subscription to its subscriber. An empty string
	// indicates the subscription has no prerequisite.
	prerequisite string

	// dependency defines the index subscription that requires the subscriber
	// associated with this subscription to have processed incoming
	// notifications before it does. A nil dependency indicates the subscription
	// has no dependencies.
	dependency *IndexSubscription
}

// C returns a channel that produces a stream of index notifications.
// Successive calls to C return the same channel.
func (s *IndexSubscription) C() chan *IndexNtfn {
	return s.c
}

// Stop prevents any future index updates from being delivered and
// unsubscribes the associated subscription.
//
// NOTE: The channel is not closed to prevent a read from the channel
// succeeding incorrectly.
func (s *IndexSubscription) Stop() error {
	s.subscriber.mtx.Lock()
	defer s.subscriber.mtx.Unlock()

	// If the subscription has a prerequisite, find it and remove the
	// subscription as a dependency.
	if len(s.prerequisite) > 0 {
		prereq, ok := s.subscriber.subscriptions[s.prerequisite]
		if !ok {
			return fmt.Errorf("no subscription found with id %s", s.prerequisite)
		}

		prereq.mtx.Lock()
		prereq.dependency = nil
		prereq.mtx.Unlock()

		return nil
	}

	// If the subscription does not have a prerequisite, remove it from the
	// index subscriber's subscriptions.
	delete(s.subscriber.subscriptions, s.id)

	return nil
}

// publishIndexNtfn sends the provided index notification on the channel
// associated with the subscription.
func (s *IndexSubscription) publishIndexNtfn(ntfn *IndexNtfn) {
	s.c <- ntfn
}

// IndexSubscriber subscribes clients for index updates.
type IndexSubscriber struct {
	c             chan *IndexNtfn
	subscriptions map[string]*IndexSubscription
	mtx           sync.Mutex
}

// NewIndexSubsciber creates a new index subscriber. It also starts
// handler for incoming index update subscriptions.
func NewIndexSubscriber(ctx context.Context) *IndexSubscriber {
	s := &IndexSubscriber{
		c:             make(chan *IndexNtfn),
		subscriptions: make(map[string]*IndexSubscription),
	}

	go s.handleSubscriptions(ctx)

	return s
}

// Subscribe subscribes an index for updates.  The returned index subscription
// has functions to retrieve a channel that produces a stream of index
// updates and to stop the stream when the caller no longer wishes to receive
// updates.
func (s *IndexSubscriber) Subscribe(index Indexer, prerequisite string) (*IndexSubscription, error) {
	sub := &IndexSubscription{
		id:           index.Name(),
		idx:          index,
		prerequisite: prerequisite,
		subscriber:   s,
		c:            make(chan *IndexNtfn),
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	// If the subscription has a prequisite, find it and set the subscription
	// as a dependency.
	if len(prerequisite) > 0 {
		prereq, ok := s.subscriptions[prerequisite]
		if !ok {
			return nil, fmt.Errorf("no subscription found with id %s", prerequisite)
		}

		prereq.mtx.Lock()
		defer prereq.mtx.Unlock()

		if prereq.dependency != nil {
			return nil, fmt.Errorf("%s already has a dependency set: %s",
				prereq.id, prereq.dependency.id)
		}

		prereq.dependency = sub

		return sub, nil
	}

	// If the subscription does not have a prerequisite, add it to the index
	// subscriber's subscriptions.
	s.subscriptions[sub.id] = sub

	return sub, nil
}

// Notify relays an index notification to subscribed indexes for processing.
func (s *IndexSubscriber) Notify(ntfn *IndexNtfn) {
	s.mtx.Lock()
	clients := len(s.subscriptions)
	s.mtx.Unlock()

	// Only relay notifications when there are subscribed indexes
	// to be notified.
	if clients > 0 {
		s.c <- ntfn
	}
}

// handleSubscriptions forwards index updates to subscribed clients.
//
// This should be run as a goroutine.
func (s *IndexSubscriber) handleSubscriptions(ctx context.Context) {
	for {
		select {
		case ntfn := <-s.c:
			// Publish the index update to subscribed indexes.
			s.mtx.Lock()
			for _, sub := range s.subscriptions {
				sub.publishIndexNtfn(ntfn)
			}
			s.mtx.Unlock()

		case <-ctx.Done():
			// Stop all updates to subscribed indexes.
			s.mtx.Lock()
			for _, sub := range s.subscriptions {
				_ = sub.Stop()
			}
			s.mtx.Unlock()
			return
		}
	}
}
