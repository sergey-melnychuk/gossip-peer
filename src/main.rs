use std::env;
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

use log::{self, debug, info, trace};

mod agent;
use agent::{Addr, Agent, Message, Record};

fn main() {
    env_logger::init();

    let ping_interval_millis: u64 = 10000;

    let ping_cutoff_millis: u64 = 1000;
    let fail_cutoff_millis: u64 = 5000;
    let gossip_interval_millis: u64 = (ping_cutoff_millis + fail_cutoff_millis) / 10;

    let args: Vec<String> = env::args().collect();
    let host: u32 = 0;
    let port: u16 = args[1].parse().unwrap();

    let socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], port))).expect("bind failed");
    let read_timeout_millis: u64 = gossip_interval_millis / 5;
    socket
        .set_read_timeout(Some(Duration::from_millis(read_timeout_millis)))
        .expect("set read timeout failed");
    info!("listening at :{}", port);

    let seeds = args
        .into_iter()
        .skip(2)
        .flat_map(|addr| addr.parse().ok())
        .map(|addr: SocketAddr| addr.into())
        .collect::<Vec<Addr>>();
    debug!("seeds: {:?}", seeds);

    let addr = Addr { host, port };
    let this = Record::new(addr, agent::get_current_millis(), 0);
    let ping = Message::Ping(this.info()).bytes();

    let mut agent = Agent::new(this, seeds, ping_cutoff_millis, fail_cutoff_millis);

    let mut last_ping_millis: u64 = 0;
    let mut last_gossip_millis: u64 = 0;
    let mut buf: [u8; 1024] = [0_u8; 1024];
    loop {
        let now = agent::get_current_millis();
        agent.tick(now);
        trace!("loop: now={}", now);

        if now - last_ping_millis >= ping_interval_millis {
            last_ping_millis = now;
            for addr in agent.ping() {
                socket.send_to(&ping, addr.addr()).expect("send failed");
                debug!("ping: {:?}", addr);
            }
        }

        if let Ok((len, from)) = socket.recv_from(&mut buf) {
            let addr: Addr = from.into();
            if let Some(mut message) = Message::parse(&buf[0..len]) {
                message.patch(addr);
                debug!("message from {:?}: {:?}", addr, message);
                let events = agent.accept(&message, now);
                for e in events {
                    info!("event: {:?}", e);
                }
            }
        }

        if now - last_gossip_millis >= gossip_interval_millis && agent.is_ready() {
            last_gossip_millis = now;
            for (addr, message) in agent.gossip(now) {
                debug!("gossip for peer {:?}: {:?}", addr, message);
                socket
                    .send_to(&message.bytes(), addr.addr())
                    .expect("failed to send");
            }
        }

        let delay_millis = gossip_interval_millis - (agent::get_current_millis() - now);
        trace!("delay: {} ms", delay_millis);
        std::thread::sleep(Duration::from_millis(delay_millis));

        let events = agent.detect(now);
        for e in events {
            info!("event: {:?}", e);
        }
    }
}
