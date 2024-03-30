use std::{collections::HashMap, sync::{RwLock, RwLockReadGuard, RwLockWriteGuard}, time::SystemTime};

pub(crate) enum Value {
    String(Vec<u8>),
}

pub(crate) struct Dataset {
    data: HashMap<Box<[u8]>, Value>,
    expiry: HashMap<Box<[u8]>, SystemTime>,
}

impl Dataset {
    pub(crate) fn new() -> Self {
        Self {
            data: HashMap::new(),
            expiry: HashMap::new(),
        }
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<&Value> {
        // TODO: Instead clear keys on expiry
        if let Some(expiry) = self.expiry.get(key) {
            if expiry < &SystemTime::now() {
                return None;
            }
        }
        self.data.get(key)
    }

    pub(crate) fn set(&mut self, key: Box<[u8]>, value: Value) {
        self.data.insert(key, value);
    }

    pub(crate) fn set_expiry(&mut self, key: Box<[u8]>, expiry: SystemTime) {
        self.expiry.insert(key, expiry);
    }

    pub(crate) fn unset_expiry(&mut self, key: &[u8]) {
        self.expiry.remove(key);
    }

    pub(crate) fn all_keys(&self) -> Vec<&[u8]> {
        self.data.keys().map(|key| key.as_ref()).collect()
    }
}

pub(crate) struct Database {
    config: RwLock<HashMap<Box<[u8]>, String>>,
    datasets: Vec<RwLock<Dataset>>
}

impl Database {
    pub(crate) fn new() -> Self {
        Self {
            config: RwLock::new(HashMap::new()),
            datasets: vec![RwLock::new(Dataset::new())]
        }
    }

    pub(crate) fn get_config(&self, key: &[u8]) -> Option<String> {
        self.config.read().unwrap().get(key).cloned()
    }

    pub(crate) fn set_config(&self, key: &[u8], value: String) {
        let key = key.to_vec().into_boxed_slice();
        self.config.write().unwrap().insert(key, value);
    }

    pub(crate) fn read(&self, dataset: usize) -> RwLockReadGuard<Dataset> {
        self.datasets[dataset].read().unwrap()
    }

    pub(crate) fn write(&self, dataset: usize) -> RwLockWriteGuard<Dataset> {
        self.datasets[dataset].write().unwrap()
    }

    pub(crate) fn swap_datasets(&mut self, datasets: Vec<Dataset>) {
        self.datasets = datasets.into_iter().map(|data| RwLock::new(data)).collect();
    }
}
