use rusqlite::Connection;
use serde::Deserialize;
use std::{
    process,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, TrySendError},
    },
    thread,
    time::{Duration, Instant},
};

const SAMPLE_TIMEOUT: Duration = Duration::from_micros(10);
const TEST_LEN: Duration = Duration::from_secs(1);
const BUFFER_LEN: usize = 500;

#[derive(Deserialize)]
struct Packet {
    gyro_x: f32,
    gyro_y: f32,
    gyro_z: f32,
}

fn main() {
    let file_name = format!("/tmp/orca_{}.db", process::id());
    let conn = Connection::open(&file_name).unwrap();
    println!("Opening db @ `{file_name}`");

    conn.execute(
        "create table samples
            ( id integer primary key
            , gyro_x real
            , gyro_y real
            , gyro_z real
            , created_at text default current_timestamp
        )",
        (),
    )
    .unwrap();

    let dropped_packets = AtomicUsize::new(0);
    let start = Instant::now();

    thread::scope(|s| {
        let (tx, rx) = mpsc::sync_channel::<Packet>(BUFFER_LEN);

        s.spawn({
            let dropped_packets = &dropped_packets;
            move || {
                let mut now = Instant::now();
                loop {
                    if now.elapsed() >= SAMPLE_TIMEOUT {
                        now = Instant::now();
                        let packet = Packet {
                            gyro_x: 0.0,
                            gyro_y: 0.0,
                            gyro_z: 0.0,
                        };

                        match tx.try_send(packet) {
                            Ok(()) => (),
                            Err(TrySendError::Full(..)) => {
                                // No happens before relationship needed
                                dropped_packets.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(TrySendError::Disconnected(..)) => break,
                        }
                    }
                }
            }
        });

        while start.elapsed() < TEST_LEN {
            let packet = rx.recv().unwrap();
            conn.execute(
                "insert into samples (gyro_x, gyro_y, gyro_z) values (?1, ?2, ?3)",
                (packet.gyro_x, packet.gyro_y, packet.gyro_z),
            )
            .unwrap();
        }
    });

    // We dont care about happens before relationship here
    let dropped_packets = dropped_packets.load(Ordering::Relaxed);
    println!("Dropped Packets: {dropped_packets}");

    let packets_read: u32 = conn
        .query_one("select count(*) from samples", (), |row| row.get(0))
        .unwrap();

    println!("Packets Read: {packets_read}");
}
