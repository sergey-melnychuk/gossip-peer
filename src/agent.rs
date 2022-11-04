use std::net::SocketAddr;
use std::fmt::{Debug, Formatter, Error};
use std::time::{SystemTime, UNIX_EPOCH};

extern crate borrowed_byte_buffer;
use self::borrowed_byte_buffer::{ByteBuf, ByteBufMut};

pub fn get_current_millis() -> u64 {
    let now = SystemTime::now();
    let epoch = now.duration_since(UNIX_EPOCH)
        .expect("Failed to get unix epoch time");
    epoch.as_millis() as u64
}

#[derive(Copy, Clone)]
pub struct Addr {
    pub host: u32,
    pub port: u16,
}

impl Addr {
    fn ip(&self) -> [u8; 4] {
        let a = ((self.host >> 24) & 0xFF) as u8;
        let b = ((self.host >> 16) & 0xFF) as u8;
        let c = ((self.host >> 8) & 0xFF) as u8;
        let d = (self.host & 0xFF) as u8;
        [a, b, c, d]
    }
    pub fn get_socket_addr(&self) -> SocketAddr {
        SocketAddr::from((self.ip(), self.port))
    }
}

impl Debug for Addr {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        let addr = format!("{}", self.get_socket_addr());
        f.write_str(addr.as_str()).expect("failed to format Addr");
        Ok(())
    }
}

impl PartialEq for Addr {
    fn eq(&self, that: &Addr) -> bool {
        self.host == that.host && self.port == that.port
    }
}

impl Eq for Addr {}

#[derive(Debug)]
pub enum Message {
    Join(Record),
    List(Vec<Record>)
}

impl Message {
    pub fn bytes(&self) -> Vec<u8> {
        let mut out = vec![0u8; 128];
        let mut buf = ByteBufMut::wrap(&mut out);
        match self {
            Message::Join(from) => {
                buf.put_u8(MessageKind::Join as u8);
                buf.put_u32(from.addr.host);
                buf.put_u16(from.addr.port);
                buf.put_u64(from.beat);
            },
            Message::List(list) => {
                buf.put_u8(MessageKind::List as u8);
                buf.put_u32(list.len() as u32);
                for rec in list {
                    buf.put_u32(rec.addr.host);
                    buf.put_u16(rec.addr.port);
                    buf.put_u64(rec.beat);
                }
            }
        }
        let len = buf.pos();
        out.into_iter().take(len).collect()
    }

    pub fn parse(buf: &[u8]) -> Option<Message> {
        let mut bb = ByteBuf::wrap(buf);
        let code = bb.get_u8().unwrap();
        match code {
            0 /*Join*/ => {
                let host = bb.get_u32().unwrap();
                let port = bb.get_u16().unwrap();
                let beat = bb.get_u64().unwrap();
                let rec = Record{ addr: Addr {host, port}, beat, time: get_current_millis() };
                Some(Message::Join(rec))
            },
            1 /*List*/ => {
                let num_peers = bb.get_u32().unwrap() as usize;
                let mut peers = Vec::with_capacity(num_peers);
                for _ in 0..num_peers {
                    let host = bb.get_u32().unwrap();
                    let port = bb.get_u16().unwrap();
                    let beat = bb.get_u64().unwrap();
                    let rec = Record{ addr: Addr {host, port}, beat, time: get_current_millis() };
                    peers.push(rec);
                }
                Some(Message::List(peers))
            },
            _ => None
        }
    }
}

#[repr(u8)]
enum MessageKind {
    Join = 0,
    List,
}

#[derive(Debug, Copy, Clone)]
pub struct Record {
    pub addr: Addr,
    pub beat: u64,
    pub time: u64,
}

#[derive(Debug)]
pub struct Agent {
    pub this: Record,
    pub peers: Vec<Record>,
    pub handler: fn(Event),
}

pub enum Event {
    Append(Record),
    Remove(Record)
}

impl Agent {
    pub fn new(this: Record) -> Agent {
        Agent { this, peers: vec![], handler: |_| {} }
    }

    pub fn set_handler(&mut self, handler: fn(Event)) {
        self.handler = handler;
    }

    pub fn tick(&mut self, time: u64) {
        self.this.beat += 1;
        self.this.time = time;
    }

    fn find(&mut self, addr: Addr) -> Option<&mut Record> {
        for rec in &mut self.peers {
            if rec.addr == addr {
                return Some(rec)
            }
        }
        None
    }

    pub fn update(&mut self, records: Vec<Record>, time: u64, ping_cutoff: u64, fail_cutoff: u64) -> Vec<Event> {
        let mut events = vec![];
        let (keep, drop) = self.peers.iter()
            .partition(|&rec| rec.time >= time - ping_cutoff - fail_cutoff);
        self.peers = keep;
        drop.into_iter()
            .for_each(|r| events.push(Event::Remove(r)));

        let addr = self.this.addr;
        records.into_iter()
            .filter(|r| r.addr != addr)
            .for_each(|r| {
                if let Some(present) = self.find(r.addr) {
                    if r.beat > present.beat {
                        present.beat = r.beat;
                        present.time = time;
                    }
                } else {
                    // TODO make use of ping_cutoff
                    let appended = Record { time, ..r };
                    self.peers.push(appended);
                    events.push(Event::Append(appended));
                }
            });

        events
    }
}
