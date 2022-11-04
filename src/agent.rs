use std::fmt::{Debug, Error, Formatter};
use std::net::{IpAddr, SocketAddr};
use std::time::{SystemTime, UNIX_EPOCH};

extern crate borrowed_byte_buffer;
use self::borrowed_byte_buffer::{ByteBuf, ByteBufMut};

#[derive(Debug, Copy, Clone)]
pub struct Record {
    addr: Addr,
    beat: u64,
    time: u64,
    down: u64,
}

impl Record {
    pub fn new(addr: Addr, time: u64) -> Self {
        Self {
            addr,
            beat: 0,
            time,
            down: 0,
        }
    }

    fn is_down(&self) -> bool {
        self.down > 0
    }
}

#[derive(Debug)]
pub enum Event {
    Append(Record),
    Remove(Record),
}

#[derive(Debug)]
pub struct Agent {
    this: Record,
    seeds: Vec<Addr>,
    peers: Vec<Record>,
    ping_cutoff: u64,
    fail_cutoff: u64,
}

impl Agent {
    pub fn new(this: Record, seeds: Vec<Addr>, ping_cutoff: u64, fail_cutoff: u64) -> Agent {
        Agent {
            this,
            seeds,
            peers: vec![],
            ping_cutoff,
            fail_cutoff,
        }
    }

    pub fn is_ready(&self) -> bool {
        !self.peers.is_empty()
    }

    pub fn tick(&mut self, time: u64) {
        self.this.beat += 1;
        self.this.time = time;
    }

    pub fn ping(&self) -> Vec<&Addr> {
        self.seeds
            .iter()
            .filter(|peer| self.peers.iter().all(|p| &p.addr != *peer))
            .collect()
    }

    fn get_mut(&mut self, addr: &Addr) -> Option<&mut Record> {
        self.peers.iter_mut().find(|rec| &rec.addr == addr)
    }

    pub fn detect(&mut self, time: u64) -> Vec<Event> {
        let total_cutoff = self.ping_cutoff + self.fail_cutoff;
        self.peers
            .iter_mut()
            .filter(|record| !record.is_down())
            .filter(|record| record.time <= time - total_cutoff)
            .map(|record| {
                record.down = time;
                Event::Remove(*record)
            })
            .collect()
    }

    pub fn accept(&mut self, message: &Message, time: u64) -> Vec<Event> {
        let mut events = self.detect(time);

        match message {
            Message::Ping(peer) => {
                if let Some(event) = self.touch(peer, time) {
                    events.push(event);
                }
            }
            Message::List(list) => {
                list.iter()
                    .filter_map(|received| self.touch(received, time))
                    .for_each(|event| events.push(event));
            }
        }

        events
    }

    fn touch(&mut self, received: &Record, time: u64) -> Option<Event> {
        if let Some(existing) = self.get_mut(&received.addr) {
            if !existing.is_down() && received.beat > existing.beat {
                existing.beat = received.beat;
                existing.time = time;
            }
            None
        } else {
            let record = Record { time, ..*received };
            self.peers.push(record);
            Some(Event::Append(record))
        }
    }

    pub fn gossip(&mut self, time: u64) -> Vec<(Addr, Message)> {
        let mut peers: Vec<Record> = self
            .peers
            .clone()
            .into_iter()
            .filter(|record| !record.is_down())
            .filter(|record| record.time > time - self.ping_cutoff)
            .collect();
        peers.push(self.this);

        self.peers
            .iter()
            .filter(|record| !record.is_down())
            .filter(|record| record.time > time - self.ping_cutoff)
            .map(|record| {
                let selected = peers
                    .clone()
                    .into_iter()
                    .filter(|r| r.addr != record.addr)
                    .collect();
                (record.addr, Message::List(selected))
            })
            .collect()
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct Addr {
    pub host: u32,
    pub port: u16,
}

impl Addr {
    pub fn addr(&self) -> SocketAddr {
        SocketAddr::from((self.host.to_be_bytes(), self.port))
    }
}

impl Debug for Addr {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        let addr = format!("{}", self.addr());
        f.write_str(addr.as_str()).expect("failed to format Addr");
        Ok(())
    }
}

impl From<SocketAddr> for Addr {
    fn from(addr: SocketAddr) -> Self {
        Self {
            host: match addr.ip() {
                IpAddr::V4(ip) => ip.into(),
                _ => panic!("IPv6 is not unsupported"),
            },
            port: addr.port(),
        }
    }
}

#[derive(Debug)]
pub enum Message {
    Ping(Record),
    List(Vec<Record>),
}

impl Message {
    pub fn patch(&mut self, ip: Addr) {
        match self {
            Message::Ping(peer) => {
                peer.addr.host = ip.host;
            }
            Message::List(list) => {
                for peer in list {
                    if peer.addr.host == 0 {
                        peer.addr.host = ip.host;
                    }
                }
            }
        }
    }

    pub fn bytes(&self) -> Vec<u8> {
        let mut out = vec![0u8; 128];
        let mut buf = ByteBufMut::wrap(&mut out);
        match self {
            Message::Ping(from) => {
                buf.put_u8(MessageKind::Join as u8);
                buf.put_u32(from.addr.host);
                buf.put_u16(from.addr.port);
                buf.put_u64(from.beat);
            }
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
            0 /* Ping */ => {
                let host = bb.get_u32().unwrap();
                let port = bb.get_u16().unwrap();
                let beat = bb.get_u64().unwrap();
                let rec = Record{ addr: Addr {host, port}, beat, time: get_current_millis(), down: 0 };
                Some(Message::Ping(rec))
            },
            1 /* List */ => {
                let num_peers = bb.get_u32().unwrap() as usize;
                let mut peers = Vec::with_capacity(num_peers);
                for _ in 0..num_peers {
                    let host = bb.get_u32().unwrap();
                    let port = bb.get_u16().unwrap();
                    let beat = bb.get_u64().unwrap();
                    let rec = Record{ addr: Addr {host, port}, beat, time: get_current_millis(), down: 0 };
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

pub fn get_current_millis() -> u64 {
    let now = SystemTime::now();
    let epoch = now
        .duration_since(UNIX_EPOCH)
        .expect("Failed to get unix epoch time");
    epoch.as_millis() as u64
}
