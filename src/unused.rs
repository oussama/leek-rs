
trait Id {
    fn get_id(&self) -> u32;
    fn set_id(&mut self, val: u32);
}



pub struct DataSet<T, U> {
    name: String,
    data: HashMap<T, HashSet<U>>,
    sender: Sender<Op<T, U>>,
    buf_writer: BufWriter<File>,
}


type DataHashSet<T, U> = HashMap<T, HashSet<U>>;

use std::ops::Deref;


pub trait OpWrite {
    fn write_op<T>(&mut self, op: T) -> Result<usize, LeekErr> where T: Encodable;
}

impl<U> OpWrite for BufWriter<U>
    where U: Write
{
    fn write_op<T>(&mut self, op: T) -> Result<usize, LeekErr>
        where T: Encodable
    {
        encode_into(&op,&self, SizeLimit::Infinite)
            .map_err(|err| LeekErr::EncodeFailed)
    }
}

pub trait OpRead {
    fn read_op<T>(&mut self, op: T) -> Result<usize, LeekErr> where T: Encodable;
}

impl<U> OpRead for BufWriter<U>
    where U: Write
{
    fn read_op<T>(&mut self, op: T) -> Result<usize, LeekErr>
        where T: Encodable
    {
        encode(&op, SizeLimit::Infinite)
            .map_err(|err| LeekErr::EncodeFailed)
            .and_then(|data| self.write(&data).map_err(|err| LeekErr::WriteFailed))
    }
}

impl<T: 'static + Eq + Hash + Sized + Send+Encodable+Clone, U: 'static + Eq + Hash + Sized + Send +Clone +Encodable+Decodable> DataSet<T, U> {
        pub fn new(name: String,n:usize) -> Result<DataSet<T, U>,LeekErr> {

            let file_path:String = name.clone()+".data";

            let file = File::create::<String>(file_path).map_err(|err| LeekErr::FileCreateFailed )?;


            let (tx, rx) = channel();
            let data_set = DataSet {
                name: name.clone(),
                data: HashMap::new(),
                sender: tx,
                buf_writer: BufWriter::new(file)
            };
            Ok(data_set)
        }

        pub fn add(&mut self, key: T, value: U) -> bool {
            if !self.data.contains_key(&key) {
                self.data.insert(key.clone(),HashSet::new());
            }
            if let Some(hash_set) = self.data.get_mut(&key){
// hash_set.
                self.sender.send(Op::Add(key, value));
                return true
            }
            false
        }


        pub fn remove(&mut self, key: T, value: U) -> bool {
            match self.data.get_mut(&key) {
                Some(hash_set) => {
                    if hash_set.remove(&value) {
                        self.sender.send(Op::Remove(key, value));
                        true
                    } else {
                        false
                    }
                }
                None => false,
            }
        }

    }