// Copyright 2023 The SeamDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;

mod memory;
mod opendal;

use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{bail, Result};
use async_trait::async_trait;
use either::Either;
use futures::{AsyncRead, AsyncWrite};
use hashbrown::hash_map::HashMap;
use thiserror::Error;

pub use self::memory::MemoryFileSystemFactory;
use crate::endpoint::{OwnedResourceUri, OwnedServiceUri, ResourceUri};

pub struct FileMeta {
    pub last_modified: SystemTime,
    pub size: u64,
}

#[derive(Debug, Error)]
pub enum FileError {
    #[error("empty file")]
    EmptyFile,
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
}

pub trait FileReader: AsyncRead + Send + 'static {}

pub trait FileWriter: AsyncWrite + Send + 'static {}

#[async_trait]
pub trait FileSystem: Send + Sync {
    fn location(&self) -> ResourceUri<'_>;

    async fn open(&self, path: &str) -> Result<Box<dyn FileReader>>;

    async fn stat(&self, path: &str) -> Result<FileMeta>;

    async fn create(&self, path: &str) -> Result<Box<dyn FileWriter>>;

    async fn delete(&self, path: &str) -> Result<()>;
}

#[async_trait]
pub trait FileSystemFactory: Send + Sync {
    fn schemes(&self) -> &[&'static str];

    async fn open(&self, uri: OwnedServiceUri) -> Result<Arc<dyn FileSystem>>;
}

#[derive(Default, Clone)]
pub struct FileSystemFactories {
    factories: HashMap<&'static str, Arc<dyn FileSystemFactory>>,
}

impl FileSystemFactories {
    pub fn new(factory: Arc<dyn FileSystemFactory>) -> Result<Self> {
        let mut factories = Self::default();
        factories.register(factory)?;
        Ok(factories)
    }

    pub fn register(&mut self, factory: Arc<dyn FileSystemFactory>) -> Result<()> {
        let schemes = factory.schemes();
        for scheme in schemes {
            if self.find(scheme).is_some() {
                bail!("filesystem factory already registered for scheme {}", scheme)
            }
        }
        schemes.iter().for_each(|scheme| {
            self.factories.insert(scheme, factory.clone());
        });
        Ok(())
    }

    pub async fn into_manager(self, filesystem: OwnedServiceUri) -> Result<FileSystemManager> {
        FileSystemManager::new(self, filesystem).await
    }

    async fn open(&self, uri: OwnedServiceUri) -> Result<Arc<dyn FileSystem>> {
        let Some(factory) = self.factories.get(uri.scheme()) else {
            bail!("filesystem factory not registered for scheme {}", uri.scheme())
        };
        factory.open(uri).await
    }

    fn find(&self, scheme: &str) -> Option<&dyn FileSystemFactory> {
        self.factories.get(scheme).map(|f| f.as_ref())
    }
}

pub struct FileSystemManager {
    factories: FileSystemFactories,
    default: Arc<dyn FileSystem>,
    filesystems: HashMap<OwnedResourceUri, Arc<dyn FileSystem>>,
}

impl From<MemoryFileSystemFactory> for FileSystemManager {
    fn from(factory: MemoryFileSystemFactory) -> Self {
        let uri = OwnedServiceUri::parse(Cow::Borrowed("memory://files")).unwrap();
        let fs = MemoryFileSystemFactory::open(uri.clone()).unwrap();
        let mut factories = FileSystemFactories::default();
        factories.register(Arc::new(factory)).unwrap();
        let mut manager = Self { factories, default: fs.clone(), filesystems: Default::default() };
        manager.mount_fs(fs);
        manager
    }
}

impl From<MemoryFileSystemFactory> for Arc<FileSystemManager> {
    fn from(factory: MemoryFileSystemFactory) -> Self {
        FileSystemManager::from(factory).into()
    }
}

impl FileSystemManager {
    pub async fn new(factories: FileSystemFactories, filesystem: OwnedServiceUri) -> Result<Self> {
        let default = factories.open(filesystem).await?;
        let mut manager = Self { factories, default: default.clone(), filesystems: Default::default() };
        manager.mount_fs(default);
        Ok(manager)
    }

    pub async fn open(&self, uri: &ResourceUri<'_>) -> Result<Box<dyn FileReader>> {
        let (fs, path) = self.locate_file(uri)?;
        fs.open(path).await
    }

    pub async fn stat(&self, uri: &ResourceUri<'_>) -> Result<FileMeta> {
        let (fs, path) = self.locate_file(uri)?;
        fs.stat(path).await
    }

    pub async fn create(&self, uri: &ResourceUri<'_>) -> Result<Box<dyn FileWriter>> {
        let (fs, path) = self.locate_file(uri)?;
        fs.create(path).await
    }

    pub fn resource_of(&self, name: &str) -> Result<OwnedResourceUri> {
        let uri = self.default.location();
        uri.into_child(name)
    }

    pub async fn delete(&self, uri: &ResourceUri<'_>) -> Result<()> {
        let (fs, path) = self.locate_file(uri)?;
        fs.delete(path).await?;
        Ok(())
    }

    pub fn register(&mut self, factory: Arc<dyn FileSystemFactory>) -> Result<()> {
        self.factories.register(factory)
    }

    pub async fn mount(&mut self, uri: OwnedServiceUri) -> Result<Arc<dyn FileSystem>> {
        let fs = self.factories.open(uri).await?;
        self.mount_fs(fs.clone());
        Ok(fs)
    }

    fn mount_fs(&mut self, fs: Arc<dyn FileSystem>) {
        match fs.location().split() {
            Either::Left(uri) => {
                self.filesystems.insert(uri.into_owned(), fs);
            },
            Either::Right(iter) => {
                for uri in iter {
                    self.filesystems.insert(uri, fs.clone());
                }
                self.filesystems.insert(fs.location().into_owned(), fs);
            },
        }
    }

    fn filesystem_of<'a>(&'a self, uri: &ResourceUri<'_>) -> Option<&'a dyn FileSystem> {
        match uri.clone().split() {
            Either::Left(uri) => self.filesystems.get(uri.as_str()).map(Arc::as_ref),
            Either::Right(iter) => {
                if let Some(fs) = self.filesystems.get(iter.uri().as_str()) {
                    return Some(fs.as_ref());
                }
                for uri in iter {
                    if let Some(fs) = self.filesystems.get(uri.as_str()) {
                        return Some(fs.as_ref());
                    }
                }
                None
            },
        }
    }

    fn locate_file<'a, 'b>(&'a self, file: &ResourceUri<'b>) -> Result<(&'a dyn FileSystem, &'b str)> {
        let Some(mut parent) = file.parent() else { bail!("{} does not have file path", file) };
        loop {
            if let Some(fs) = self.filesystem_of(&parent) {
                let path = &file.path()[fs.location().path().len() + 1..];
                return Ok((fs, path));
            }
            parent = match parent.parent() {
                None => break,
                // Safety: both `parent` and `uri` are references to `file`
                Some(uri) => unsafe { std::mem::transmute(uri) },
            }
        }
        bail!("filesystem not found for {}", file)
    }
}
