use std::env;
use std::net::{UdpSocket, SocketAddr};
use std::time::Duration;

mod agent;
use agent::{Addr, Agent, Event, Message, Record};

fn str_to_host(ip: String) -> u32 {
    let chunks: Vec<&str> = ip.split(".").collect();
    let a: u8 = chunks[0].parse().unwrap();
    let b: u8 = chunks[1].parse().unwrap();
    let c: u8 = chunks[2].parse().unwrap();
    let d: u8 = chunks[3].parse().unwrap();
    let mut host: u32 = a as u32;
    host = (host << 8) + b as u32;
    host = (host << 8) + c as u32;
    host = (host << 8) + d as u32;
    host
}

fn main() {
    let ping_cutoff_millis: u64 = 500;
    let fail_cutoff_millis: u64 = 1000;
    let gossip_interval_millis: u64 = (ping_cutoff_millis + fail_cutoff_millis) / 5;
    let read_timeout_millis: u64 = gossip_interval_millis / 5;

    let args: Vec<String> = env::args().collect();
    let host: u32 = 0;
    let port: u16 = args[1].parse().unwrap();
    let socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], port)))
        .expect("failed to bind");
    socket.set_read_timeout(Some(Duration::from_millis(read_timeout_millis)))
        .expect("fail to set read timeout");
    println!("listening at :{}", port);

    let this = Record { addr: Addr { host, port }, beat: 0, time: agent::get_current_millis() };

    let message = Message::Join(this.clone());
    let buf = message.into_bytes();
    for arg in args.iter().skip(2) {
        //println!("sending to {}", arg);
        socket.send_to(&buf[0..buf.len()], arg).expect("failed to send");
    }

    let mut agent = Agent::new(this);
    agent.set_handler(|e| {
        match e {
            Event::Append(rec) => println!("append: {:?}", rec),
            Event::Remove(rec) => println!("remove: {:?}", rec),
        }
    });
    //println!("agent: {:?}", agent);

    let mut last_gossip_millis: u64 = 0;
    let mut buf: [u8; 1024] = [0 as u8; 1024];
    loop {
        let now = agent::get_current_millis();
        //println!("loop: now={}", now);

        let res = socket.recv_from(&mut buf);
        if res.is_ok() {
            let (_, from) = res.unwrap();
            //println!("received from {:?}", from);

            if let Some(message) = Message::from_bytes(&buf) {
                //println!("received message: {:?}", message);
                match message {
                    Message::Join(mut peer) => {
                        peer.addr.host = str_to_host(from.ip().to_string());
                        let events = agent
                            .update(vec![peer], now, ping_cutoff_millis, fail_cutoff_millis);
                        for e in events {
                            (agent.handler)(e);
                        }
                    },
                    Message::List(mut peers) => {
                        peers.iter_mut().for_each(|mut peer| {
                            peer.addr.host = str_to_host(from.ip().to_string());
                        });
                        let events = agent
                            .update(peers, now, ping_cutoff_millis, fail_cutoff_millis);
                        for e in events {
                            (agent.handler)(e);
                        }
                    }
                }
            }
        }

        if now - last_gossip_millis >= gossip_interval_millis && agent.peers.len() > 0 {
            agent.tick(now);
            last_gossip_millis = now;

            let mut peers: Vec<Record> = agent.peers.clone().into_iter()
                .filter(|r| r.time > now - ping_cutoff_millis)
                .collect();
            peers.push(agent.this);

            agent.peers.iter().for_each(|&peer| {
                let selected = peers.clone().into_iter().filter(|r| {
                    r.addr != peer.addr
                }).collect();
                let message = Message::List(selected);
                let buf = message.into_bytes();
                //println!("gossip: {:?} ({} bytes)", message, buf.len());

                let to: SocketAddr = peer.addr.get_socket_addr();
                socket.send_to(&buf, to).expect("failed to send");
            });
        } else {
            // If there is no need to gossip, run failure detection only
            let events = agent
                .update(vec![], now, ping_cutoff_millis, fail_cutoff_millis);
            for e in events {
                (agent.handler)(e);
            }
        }
    }
}

