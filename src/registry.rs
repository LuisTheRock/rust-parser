use std::collections::{HashMap, HashSet};
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Default)]
pub struct Registry {
    imei_to_host: RwLock<HashMap<String, String>>,
    valid_keys: RwLock<HashSet<String>>,
}

impl Registry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn imei_to_host(&self, imei: &str) -> Option<String> {
        self.hosts().get(imei).cloned()
    }

    pub fn load_hosts(&self, entries: impl IntoIterator<Item = (String, String)>) {
        *self.hosts_mut() = entries.into_iter().collect();
    }

    pub fn remove_host(&self, imei: &str) {
        self.hosts_mut().remove(imei);
    }

    pub fn add_host(&self, imei: String, host_id: String) {
        self.hosts_mut().insert(imei, host_id);
    }

    #[must_use]
    pub fn is_valid_key(&self, key: &str) -> bool {
        self.keys().contains(key)
    }

    pub fn load_valid_keys(&self, keys: impl IntoIterator<Item = String>) {
        *self.keys_mut() = keys.into_iter().collect();
    }

    pub fn remove_valid_key(&self, key: &str) {
        self.keys_mut().remove(key);
    }

    pub fn add_valid_key(&self, key: String) {
        self.keys_mut().insert(key);
    }

    fn hosts(&self) -> RwLockReadGuard<'_, HashMap<String, String>> {
        self.imei_to_host
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn hosts_mut(&self) -> RwLockWriteGuard<'_, HashMap<String, String>> {
        self.imei_to_host
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn keys(&self) -> RwLockReadGuard<'_, HashSet<String>> {
        self.valid_keys
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn keys_mut(&self) -> RwLockWriteGuard<'_, HashSet<String>> {
        self.valid_keys
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }
}
