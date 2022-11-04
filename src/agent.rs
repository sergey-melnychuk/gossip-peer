use std::fmt::{Debug, Error, Formatter};
use std::net::{IpAddr, SocketAddr};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Info {
    addr: Addr,
    beat: u64,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Record {
    info: Info,
    time: u64,
    down: u64,
}

impl Record {
    pub fn new(addr: Addr, time: u64, beat: u64) -> Self {
        Self {
            info: Info { addr, beat },
            time,
            down: 0,
        }
    }

    pub fn info(&self) -> Info {
        self.info
    }

    fn is_down(&self) -> bool {
        self.down > 0
    }
}

#[derive(Debug, Eq, PartialEq)]
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
        self.this.info.beat += 1;
        self.this.time = time;
    }

    pub fn ping(&self) -> Vec<&Addr> {
        self.seeds
            .iter()
            .filter(|peer| {
                self.peers
                    .iter()
                    .filter(|p| !p.is_down())
                    .all(|p| &p.info.addr != *peer)
            })
            .collect()
    }

    fn get_mut(&mut self, addr: &Addr) -> Option<&mut Record> {
        self.peers.iter_mut().find(|rec| &rec.info.addr == addr)
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

    fn touch(&mut self, info: &Info, time: u64) -> Option<Event> {
        if let Some(record) = self.get_mut(&info.addr) {
            if info.beat > record.info.beat || info.beat == 0 {
                record.info.beat = info.beat;
                record.time = time;
                record.down = 0;
            }
            None
        } else {
            let record = Record {
                info: *info,
                time,
                down: 0,
            };
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
                    .map(|r| r.info)
                    .filter(|info| info.addr != record.info.addr)
                    .collect();
                (record.info.addr, Message::List(selected))
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

#[derive(Debug, Eq, PartialEq)]
pub enum Message {
    Ping(Info),
    List(Vec<Info>),
}

impl Message {
    pub fn patch(&mut self, ip: Addr) {
        match self {
            Message::Ping(info) => {
                info.addr.host = ip.host;
            }
            Message::List(list) => {
                for info in list {
                    if info.addr.host == 0 {
                        info.addr.host = ip.host;
                    }
                }
            }
        }
    }

    pub fn bytes(&self) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(128);
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
                for info in list {
                    buf.put_u32(info.addr.host);
                    buf.put_u16(info.addr.port);
                    buf.put_u64(info.beat);
                }
            }
        }
        let vec = buf.to_vec();
        assert!(vec.len() < 128);
        vec
    }

    pub fn parse(buf: &[u8]) -> Option<Message> {
        let mut bb = Bytes::copy_from_slice(buf);
        let code = bb.get_u8();
        match code {
            0 /* Ping */ => {
                let host = bb.get_u32();
                let port = bb.get_u16();
                let beat = bb.get_u64();
                let info = Info { addr: Addr {host, port}, beat };
                Some(Message::Ping(info))
            },
            1 /* List */ => {
                let count = bb.get_u32() as usize;
                let mut infos = Vec::with_capacity(count);
                for _ in 0..count {
                    let host = bb.get_u32();
                    let port = bb.get_u16();
                    let beat = bb.get_u64();
                    let info = Info { addr: Addr {host, port}, beat };
                    infos.push(info);
                }
                Some(Message::List(infos))
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

#[cfg(test)]
mod tests {
    use super::*;

    const PING_CUTOFF: u64 = 1000;
    const FAIL_CUTOFF: u64 = 5000;

    fn info(i: u8, beat: u64) -> Info {
        Info {
            addr: addr(i),
            beat,
        }
    }

    fn addr(i: u8) -> Addr {
        Addr {
            host: u32::from_be_bytes([i, i, i, i]),
            port: i as u16,
        }
    }

    fn agent(i: u8, t: u64, b: u64) -> Agent {
        Agent::new(Record::new(addr(i), t, b), vec![], PING_CUTOFF, FAIL_CUTOFF)
    }

    #[test]
    fn test_gossip() {
        let mut time = 1000000000;

        let mut agent = agent(1, time, 101);

        let join = Message::Ping(info(2, 101));
        assert_eq!(
            agent.accept(&join, time),
            vec![Event::Append(Record::new(addr(2), time, 101))]
        );
        assert_eq!(agent.peers, vec![Record::new(addr(2), time, 101)]);

        time += PING_CUTOFF / 2;
        assert!(agent.detect(time).is_empty());
        assert_eq!(
            agent.gossip(time),
            vec![(addr(2), Message::List(vec![info(1, 101)]))]
        );

        time += PING_CUTOFF;
        assert!(agent.gossip(time).is_empty());
    }
}
