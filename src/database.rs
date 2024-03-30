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

    // pub(crate) fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
    //     let lock = self.datasets[0].read().unwrap();
    //
    //     let value = match lock.get(key) {
    //         Some(value) => value,
    //         None => return None,
    //     };
    //
    //     if let Some(expiry) = lock.get_expiry(key) {
    //         if expiry < &Instant::now() {
    //             return None;
    //         }
    //     }
    //
    //     match value {
    //         Value::String(data) => Some(data.clone()),
    //     }
    // }
    //
    // pub(crate) fn set(&self, key: &[u8], data: &[u8], expiry: Option<Instant>) {
    //     let key = key.to_vec().into_boxed_slice();
    //     let data = data.to_vec();
    //
    //     let lock = &mut self.datasets[0].write().unwrap();
    //
    //     if let Some(expiry) = expiry {
    //         lock.set_expiry(key.clone(), expiry);
    //     } else {
    //         lock.unset_expiry(&key);
    //     }
    //     lock.set(key, Value::String(data));
    // }

    pub(crate) fn swap_datasets(&mut self, datasets: Vec<Dataset>) {
        self.datasets = datasets.into_iter().map(|data| RwLock::new(data)).collect();
    }
}
