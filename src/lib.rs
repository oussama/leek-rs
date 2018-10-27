extern crate bincode;
extern crate serde;
#[macro_use]
extern crate serde_derive;

use bincode::{deserialize, serialize};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::Write;

use std::collections::HashMap;
use std::hash::Hash;
use std::time::Instant;

use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom};

use std::result::Result;

use std::io::ErrorKind;
use std::sync::mpsc::*;
use std::thread;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Op<T, U> {
    Add(T, U),
    Remove(T, U),
    Destroy(T),
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Err {
    FileCreate,
    FileOpen,
    Rcv,
    Encode,
    Write,
    Insert,
    Seek,
}

pub struct StorageMap<T, U> {
    pub data: HashMap<T, U>,
    //pub buf: BufWriter<File>,
    sx: Sender<Op<T, U>>,
}

impl<T, U> StorageMap<T, U>
where
    T: 'static + Hash + Eq + Clone + Serialize + DeserializeOwned + Debug + Send + Sized,
    U: 'static + Serialize + DeserializeOwned + Clone + Debug + Send + Sized,
{
    fn write_ops<W: Write>(writer: &mut W, ops: &mut Vec<Op<T, U>>) {
        for op in &mut *ops {
            if let Ok(encoded) = serialize(op) {
                use std::mem::transmute;
                let len = encoded.len() as u32;
                let bytes: [u8; 4] = unsafe { transmute(len) }; // or .to_le()
                writer.write(&bytes).expect("failed to write data");
                writer.write(&encoded).expect("failed to write data");
            }
        }
        writer.flush().expect("failed to flush data");
        ops.clear();
    }

    pub fn new(file_path: String) -> Result<StorageMap<T, U>, Err> {
        println!("StorageMap::new");
        let mut hash_map = HashMap::new();
        let mut file = if let Ok(mut file) = File::open(&file_path) {
            use std::io::Read;

            let now = Instant::now();
            let mut count = 0;
            let mut read = true;
            while (read) {
                let mut bytes: [u8; 4] = [0, 0, 0, 0];
                match file.read_exact(&mut bytes) {
                    Ok(_) => {
                        let size = unsafe { std::mem::transmute::<[u8; 4], u32>(bytes) as usize };
                        let mut bytes = Vec::with_capacity(size);
                        unsafe { bytes.set_len(size) };
                        file.read_exact(&mut bytes)
                            .expect(&format!("failed to read {} bytes", size));
                        if let Ok(op) = deserialize::<Op<T, U>>(&bytes) {
                            count += 1;
                            //println!("op {:?}",&op );
                            match op {
                                Op::Add(key, val) => {
                                    hash_map.insert(key, val);
                                }
                                Op::Destroy(key) => {
                                    hash_map.remove(&key);
                                }
                                _ => (),
                            }
                        } else {
                            println!("failed to deserialize");
                        }
                    }
                    Err(err) => {
                        if err.kind() == ErrorKind::UnexpectedEof {
                            println!("end of file {}", count);
                            read = false;
                        } else {
                            panic!("failed to read {} {:?}", count, err);
                        }
                    }
                }
            }

            let seconds = now.elapsed().as_secs();
            if seconds > 0 {
                println!(
                    "DB loaded {} records in {} seconds, {}/s",
                    count,
                    now.elapsed().as_secs(),
                    count / seconds
                );
            }
            file
        } else {
            println!("create new file");
            File::create(&file_path).expect("failed to create file")
        };
        let (sx, rx) = channel::<Op<T, U>>();
        thread::spawn(move || {
            let capacity = 1000;
            let file = OpenOptions::new()
                .write(true)
                .append(true)
                .open(file_path)
                .expect("failed to open file for write");

            let mut writer = BufWriter::new(file);
            writer.seek(SeekFrom::Start(0)).unwrap();
            let mut ops = Vec::with_capacity(capacity);
            while let Ok(op) = rx.recv() {
                ops.push(op);
                if ops.len() == capacity {
                    Self::write_ops(&mut writer, &mut ops);
                }
            }
            if ops.len() > 0 {
                Self::write_ops(&mut writer, &mut ops);
            }
        });

        Ok(StorageMap { data: hash_map, sx })
    }

    pub fn set(&mut self, key: T, val: U) -> Result<(), Err> {
        self.data.insert(key.clone(), val.clone());
        let _ = self.sx.send(Op::Add(key, val));
        Ok(())
    }
}

#[cfg(test)]
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct Email {
    pub email: String,
    pub password: String,
}

#[test]
fn it_works() {
    thread::spawn(move || {
        let mut data_map = StorageMap::<u32, Email>::new("/test2129".to_string()).unwrap();
        println!("INIIIIIIIIIIIIIT {:?}", data_map.data);
        if let Some(val) = data_map.data.get(&334) {
            println!("Value: {:?}", val.email);
        }
        let mut i = 0;
        while (i < 10000) {
            data_map
                .set(
                    334,
                    Email {
                        email: "test@email.com".into(),
                        password: "sdqfsdf".into(),
                    },
                )
                .unwrap();
            i += 1;
        }
    });
    thread::sleep_ms(100000);
}
