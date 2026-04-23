use alloy_primitives::{keccak256, Address, B256};
use anyhow::Result;
use rayon::slice::ParallelSliceMut;
use reth_db::mdbx::RO;
use reth_db::{mdbx::tx::Tx, tables};
use reth_db_api::cursor::{DbCursorRO, DbDupCursorRO};
use reth_db_api::transaction::DbTx;
use reth_provider::providers::RocksDBProvider;
use std::collections::HashMap;

use super::{AccountStorageItem, PreimageIterator};

/// Iterates all current accounts and their storage slots in EIP-7748 order:
/// accounts sorted by keccak256(address), storage slots sorted by keccak256(slot).
///
/// Construction is two-phase:
///   Phase 1 — addresses: scan AccountsHistory (RocksDB), verify against HashedAccounts
///             (MDBX), sort by hash.
///   Phase 2 — storage:   scan StoragesHistory (RocksDB) once, verify each unique
///             (address, slot) against HashedStorages (MDBX), store live slots per address
///             sorted by keccak256(slot).
pub struct Eip7748Iterator {
    ordered_addresses: Vec<Address>,
    addr_idx: usize,
    storage_map: HashMap<Address, Vec<B256>>,
    storage_slot_idx: usize,
    state: State,
}

#[derive(Copy, Clone)]
enum State {
    Account,
    StorageSlot(Address),
    End,
}

impl PreimageIterator for Eip7748Iterator {}

impl Eip7748Iterator {
    pub fn new<P>(tx: &Tx<RO>, rocksdb: &RocksDBProvider, mut progress: Option<P>) -> Result<Self>
    where
        P: FnMut(Address),
    {
        // ── Phase 1: collect all live plain addresses ──────────────────────────────
        let mut hashed_accounts_cursor = tx.cursor_read::<tables::HashedAccounts>()?;

        let mut addr_with_hash: Vec<(Address, B256)> = Vec::with_capacity(300_000_000);
        let mut last_addr: Option<Address> = None;

        for item in rocksdb.iter::<tables::AccountsHistory>()? {
            let (key, _) = item?;
            let addr = key.key; // ShardedKey<Address>.key

            // Deduplicate multiple shards for the same address.
            if last_addr == Some(addr) {
                continue;
            }
            last_addr = Some(addr);

            if let Some(ref mut cb) = progress {
                cb(addr);
            }

            // Keep only currently-live accounts.
            if hashed_accounts_cursor.seek_exact(keccak256(addr))?.is_some() {
                addr_with_hash.push((addr, keccak256(addr)));
            }
        }

        addr_with_hash.par_sort_unstable_by_key(|(_, h)| *h);
        let ordered_addresses: Vec<Address> = addr_with_hash.into_iter().map(|(a, _)| a).collect();

        // ── Phase 2: collect live storage slots per address ────────────────────────
        let mut hashed_storage_cursor = tx.cursor_dup_read::<tables::HashedStorages>()?;
        let mut storage_map: HashMap<Address, Vec<B256>> = HashMap::new();
        let mut last_slot: Option<(Address, B256)> = None;

        for item in rocksdb.iter::<tables::StoragesHistory>()? {
            let (key, _) = item?;
            let addr = key.address;
            let slot = key.sharded_key.key;

            // Deduplicate multiple shards for the same (address, slot) pair.
            if last_slot == Some((addr, slot)) {
                continue;
            }
            last_slot = Some((addr, slot));

            // Verify the slot is currently live in HashedStorages.
            let hashed_addr = keccak256(addr);
            let hashed_slot = keccak256(slot);
            let entry = hashed_storage_cursor.seek_by_key_subkey(hashed_addr, hashed_slot)?;
            let is_live = entry.is_some_and(|e| e.key == hashed_slot);
            if is_live {
                storage_map.entry(addr).or_default().push(slot);
            }
        }

        // Sort each address's storage slots by keccak256(slot) for EIP-7748 order.
        for slots in storage_map.values_mut() {
            slots.sort_unstable_by_key(|s| keccak256(*s));
        }

        Ok(Eip7748Iterator {
            ordered_addresses,
            addr_idx: 0,
            storage_map,
            storage_slot_idx: 0,
            state: State::Account,
        })
    }
}

impl Iterator for Eip7748Iterator {
    type Item = Result<AccountStorageItem>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let cur = self.state;
            match cur {
                State::End => return None,

                State::Account => {
                    match self.ordered_addresses.get(self.addr_idx) {
                        None => {
                            self.state = State::End;
                            return None;
                        }
                        Some(&address) => {
                            self.addr_idx += 1;
                            self.storage_slot_idx = 0;
                            self.state = State::StorageSlot(address);
                            return Some(Ok(AccountStorageItem::Account(address)));
                        }
                    }
                }

                State::StorageSlot(address) => {
                    let slots = self.storage_map.get(&address);
                    match slots.and_then(|v| v.get(self.storage_slot_idx)) {
                        None => {
                            self.state = State::Account;
                            continue;
                        }
                        Some(&slot) => {
                            self.storage_slot_idx += 1;
                            return Some(Ok(AccountStorageItem::StorageSlot(address, slot)));
                        }
                    }
                }
            }
        }
    }
}
