use alloy_primitives::{keccak256, Address, B256};
use anyhow::Result;
use reth_db::mdbx::cursor::Cursor;
use reth_db::mdbx::RO;
use reth_db::{mdbx::tx::Tx, tables};
use reth_db_api::cursor::{DbCursorRO, DbDupCursorRO};
use reth_db_api::transaction::DbTx;
use reth_provider::providers::{RocksDBIter, RocksDBProvider};

use super::{AccountStorageItem, PreimageIterator};

/// Iterates all current accounts and their storage slots in plain (address-sorted) order.
///
/// In storage v2, plain state tables are empty. This iterator reconstructs the full
/// set of live (address, slot) pairs by:
///   - Reading plain addresses from `AccountsHistory` in RocksDB (deduplicated)
///   - Verifying each address is live via `HashedAccounts` in MDBX
///   - Reading plain (address, slot) pairs from `StoragesHistory` in RocksDB (deduplicated)
///   - Verifying each slot is live via `HashedStorages` in MDBX
pub struct PlainIterator<'a> {
    hashed_accounts_cursor: Cursor<RO, tables::HashedAccounts>,
    hashed_storage_cursor: Cursor<RO, tables::HashedStorages>,
    accounts_iter: std::iter::Peekable<RocksDBIter<'a, tables::AccountsHistory>>,
    storages_iter: std::iter::Peekable<RocksDBIter<'a, tables::StoragesHistory>>,
    last_account_addr: Option<Address>,
    last_storage_slot: Option<(Address, B256)>,
    state: State,
}

#[derive(Copy, Clone)]
enum State {
    Account,
    StorageSlot(Address),
    End,
}

impl<'a> PlainIterator<'a> {
    pub fn new(tx: &Tx<RO>, rocksdb: &'a RocksDBProvider) -> Result<Self> {
        Ok(PlainIterator {
            hashed_accounts_cursor: tx.cursor_read::<tables::HashedAccounts>()?,
            hashed_storage_cursor: tx.cursor_dup_read::<tables::HashedStorages>()?,
            accounts_iter: rocksdb.iter::<tables::AccountsHistory>()?.peekable(),
            storages_iter: rocksdb.iter::<tables::StoragesHistory>()?.peekable(),
            last_account_addr: None,
            last_storage_slot: None,
            state: State::Account,
        })
    }

    /// Advance `storages_iter` past all entries whose address equals `address`.
    fn advance_storage_past(&mut self, address: Address) {
        loop {
            let should_advance = matches!(
                self.storages_iter.peek(),
                Some(Ok((key, _))) if key.address == address
            );
            if !should_advance {
                break;
            }
            self.storages_iter.next();
        }
    }
}

impl PreimageIterator for PlainIterator<'_> {}

impl Iterator for PlainIterator<'_> {
    type Item = Result<AccountStorageItem>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let cur = self.state;
            match cur {
                State::End => return None,

                State::Account => {
                    // Advance accounts_iter, deduplicating multiple shards per address.
                    let address = 'dedup: loop {
                        match self.accounts_iter.next() {
                            None => {
                                self.state = State::End;
                                return None;
                            }
                            Some(Err(e)) => return Some(Err(e.into())),
                            Some(Ok((key, _))) => {
                                let addr = key.key; // ShardedKey<Address>.key
                                if self.last_account_addr == Some(addr) {
                                    continue 'dedup; // same address, different shard
                                }
                                self.last_account_addr = Some(addr);
                                break 'dedup addr;
                            }
                        }
                    };

                    // Verify the account is currently live in HashedAccounts.
                    match self.hashed_accounts_cursor.seek_exact(keccak256(address)) {
                        Err(e) => return Some(Err(e.into())),
                        Ok(None) => {
                            // Account deleted: skip all storage entries for this address
                            // so that storages_iter stays aligned with accounts_iter.
                            self.advance_storage_past(address);
                            // Stay in Account state; outer loop picks up the next account.
                        }
                        Ok(Some(_)) => {
                            self.state = State::StorageSlot(address);
                            return Some(Ok(AccountStorageItem::Account(address)));
                        }
                    }
                }

                State::StorageSlot(address) => {
                    // Peek at the next storage entry without consuming it.
                    let (peek_addr, peek_slot) = {
                        match self.storages_iter.peek() {
                            None => {
                                self.state = State::Account;
                                continue;
                            }
                            Some(Err(_)) => {
                                let e = self.storages_iter.next().unwrap().unwrap_err();
                                return Some(Err(e.into()));
                            }
                            Some(Ok((key, _))) => (key.address, key.sharded_key.key),
                        }
                    };

                    if peek_addr < address {
                        // StoragesHistory has entries for an address that AccountsHistory
                        // skipped (e.g. an account deleted before our scan started). Drain them.
                        self.advance_storage_past(peek_addr);
                        continue;
                    }

                    if peek_addr > address {
                        // No more storage for the current account.
                        self.state = State::Account;
                        continue;
                    }

                    // peek_addr == address: consume this entry.
                    self.storages_iter.next();

                    // Deduplicate multiple shards for the same (addr, slot) pair.
                    if self.last_storage_slot == Some((peek_addr, peek_slot)) {
                        continue;
                    }
                    self.last_storage_slot = Some((peek_addr, peek_slot));

                    // Verify the slot is currently live in HashedStorages.
                    let hashed_slot = keccak256(peek_slot);
                    match self
                        .hashed_storage_cursor
                        .seek_by_key_subkey(keccak256(address), hashed_slot)
                    {
                        Err(e) => return Some(Err(e.into())),
                        Ok(None) => continue, // slot does not exist
                        Ok(Some(entry)) => {
                            // seek_by_key_subkey returns the first entry with subkey >= hashed_slot;
                            // verify it is an exact match.
                            if entry.key != hashed_slot {
                                continue;
                            }
                            return Some(Ok(AccountStorageItem::StorageSlot(address, peek_slot)));
                        }
                    }
                }
            }
        }
    }
}
