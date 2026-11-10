// Co-op memory bridge server.
//
// Exposes guest RDRAM and controller input over a local TCP socket so an
// external bridge process (see the coop-anything "ultimate-trial" project)
// can read and write game state once per frame.
//
// Design notes:
// - One client at a time (the bridge). The listener stays up; a client can
//   disconnect and reconnect without restarting the emulator.
// - The emulator thread executes queued commands at the top of every VI
//   frame (see device::vi::vertical_interrupt_event), so responses are
//   deterministic functions of the frame state.
// - Addresses on the wire are N64 virtual/physical addresses as the game
//   sees them. The server converts to host RDRAM storage (KSEG0 mask,
//   RDRAM mask, byte_swap) so clients see plain big-endian game bytes.
//
// Wire protocol (little-endian scalars, one batch per round trip):
//   client -> server: u32 payload_len, payload
//     payload is a concatenation of requests:
//       0x01 READ   u32 addr, u16 len
//       0x02 WRITE  u32 addr, u16 len, len bytes
//       0x03 INPUT  u8 channel, u8 active, u32 data
//       0x04 STAT
//   server -> client: u32 payload_len, payload
//     one response per request, same order:
//       0x81 READ   u8 status, u16 len, len bytes
//       0x82 WRITE  u8 status
//       0x83 INPUT  u8 status
//       0x84 STAT   u8 status, u64 frame, u32 rdram_size
//   status 0 = ok, 1 = bad arguments. A malformed batch closes the connection.

use crate::device;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc::{Receiver, Sender};

pub const OP_READ: u8 = 0x01;
pub const OP_WRITE: u8 = 0x02;
pub const OP_INPUT: u8 = 0x03;
pub const OP_STAT: u8 = 0x04;

const OP_READ_RESP: u8 = 0x81;
const OP_WRITE_RESP: u8 = 0x82;
const OP_INPUT_RESP: u8 = 0x83;
const OP_STAT_RESP: u8 = 0x84;

const STATUS_OK: u8 = 0;
const STATUS_BAD: u8 = 1;

const MAX_BATCH: usize = 4 * 1024 * 1024;
const COMMANDS_PER_FRAME: usize = 4096;

pub enum Command {
    Read { conn: u64, addr: u32, len: u16 },
    Write { conn: u64, addr: u32, bytes: Vec<u8> },
    Input { conn: u64, channel: u8, active: bool, data: u32 },
    Stat { conn: u64 },
}

pub enum Response {
    Data { conn: u64, bytes: Vec<u8> },
    Ok { conn: u64, marker: u8 },
    Stat { conn: u64, frame: u64, rdram_size: u32 },
    Bad { conn: u64 },
}

impl Response {
    fn conn(&self) -> u64 {
        match self {
            Response::Data { conn, .. }
            | Response::Ok { conn, .. }
            | Response::Stat { conn, .. }
            | Response::Bad { conn } => *conn,
        }
    }
}

pub struct Coop {
    pub cmd_rx: Receiver<Command>,
    pub resp_tx: Sender<Response>,
    pub inputs: [Option<u32>; 4],
    pub frame: u64,
    pub port: u16,
}

/// Start the bridge server and return the emulator-side handle.
/// `port` 0 picks an ephemeral port, but callers should pass an explicit one.
pub fn init(port: u16) -> std::io::Result<Coop> {
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    let port = listener.local_addr()?.port();
    let (cmd_tx, cmd_rx) = std::sync::mpsc::channel::<Command>();
    let (resp_tx, resp_rx) = std::sync::mpsc::channel::<Response>();

    std::thread::Builder::new()
        .name("coop-bridge".to_string())
        .spawn(move || accept_loop(listener, cmd_tx, resp_rx))?;

    Ok(Coop {
        cmd_rx,
        resp_tx,
        inputs: [None; 4],
        frame: 0,
        port,
    })
}

/// Called once per VI frame from the emulator thread.
pub fn on_frame(device: &mut device::Device) {
    let Some(coop) = device.coop.as_mut() else {
        return;
    };
    coop.frame = coop.frame.wrapping_add(1);

    let byte_swap = device.byte_swap;
    let byte_swap_present = byte_swap != 0;
    let mut executed = 0;
    while executed < COMMANDS_PER_FRAME {
        let Ok(cmd) = coop.cmd_rx.try_recv() else {
            break;
        };
        executed += 1;
        match cmd {
            Command::Read { conn, addr, len } => {
                let base = guest_to_index(addr);
                let mut out = Vec::with_capacity(len as usize);
                for i in 0..len as usize {
                    let index = if byte_swap_present {
                        (base + i) ^ byte_swap
                    } else {
                        base + i
                    };
                    out.push(*device.rdram.mem.get(index).unwrap_or(&0));
                }
                let _ = coop.resp_tx.send(Response::Data { conn, bytes: out });
            }
            Command::Write { conn, addr, bytes } => {
                let base = guest_to_index(addr);
                for (i, value) in bytes.iter().enumerate() {
                    let index = if byte_swap_present {
                        (base + i) ^ byte_swap
                    } else {
                        base + i
                    };
                    if let Some(slot) = device.rdram.mem.get_mut(index) {
                        *slot = *value;
                    }
                }
                let _ = coop.resp_tx.send(Response::Ok {
                    conn,
                    marker: OP_WRITE_RESP,
                });
            }
            Command::Input {
                conn,
                channel,
                active,
                data,
            } => {
                if let Some(slot) = coop.inputs.get_mut(channel as usize) {
                    *slot = if active { Some(data) } else { None };
                }
                let _ = coop.resp_tx.send(Response::Ok {
                    conn,
                    marker: OP_INPUT_RESP,
                });
            }
            Command::Stat { conn } => {
                let _ = coop.resp_tx.send(Response::Stat {
                    conn,
                    frame: coop.frame,
                    rdram_size: device.rdram.size,
                });
            }
        }
    }
}

fn guest_to_index(addr: u32) -> usize {
    // KSEG0/KSEG1 virtual addresses map to RDRAM physical by masking the top
    // three bits; RDRAM itself is mirrored inside a 16 MiB window.
    (addr & 0x1FFF_FFFF) as usize & 0x00FF_FFFF
}

fn accept_loop(
    listener: TcpListener,
    cmd_tx: Sender<Command>,
    resp_rx: Receiver<Response>,
) {
    let mut next_conn: u64 = 1;
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else {
            continue;
        };
        let conn = next_conn;
        next_conn = next_conn.wrapping_add(1);
        if next_conn == 0 {
            next_conn = 1; // 0 is reserved for stale/disconnect traffic
        }
        let _ = stream.set_nodelay(true);
        let _ = handle_client(&mut stream, &cmd_tx, &resp_rx, conn);
        // Hand control back to physical input on disconnect. These commands use
        // connection 0, so their acknowledgements are discarded by the next
        // client instead of corrupting its response stream.
        for channel in 0..4u8 {
            let _ = cmd_tx.send(Command::Input {
                conn: 0,
                channel,
                active: false,
                data: 0,
            });
        }
    }
}

fn handle_client(
    stream: &mut std::net::TcpStream,
    cmd_tx: &Sender<Command>,
    resp_rx: &Receiver<Response>,
    conn: u64,
) -> std::io::Result<()> {
    loop {
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        }
        let payload_len = u32::from_le_bytes(len_buf) as usize;
        if payload_len > MAX_BATCH {
            return Ok(());
        }
        let mut payload = vec![0u8; payload_len];
        stream.read_exact(&mut payload)?;

        let count = match forward_batch(&payload, cmd_tx, conn) {
            Ok(count) => count,
            Err(()) => return Ok(()),
        };

        let mut out = Vec::with_capacity(64 * count);
        let mut received = 0;
        while received < count {
            let response = match resp_rx.recv() {
                Ok(response) => response,
                Err(_) => return Ok(()),
            };
            if response.conn() != conn {
                // Leftover from a batch whose client disconnected.
                continue;
            }
            encode_response(response, &mut out);
            received += 1;
        }
        stream.write_all(&(out.len() as u32).to_le_bytes())?;
        stream.write_all(&out)?;
        stream.flush()?;
    }
}

/// Parse a batch and queue one command per request. Returns the request count.
fn forward_batch(payload: &[u8], cmd_tx: &Sender<Command>, conn: u64) -> Result<usize, ()> {
    let mut offset = 0usize;
    let mut count = 0usize;
    while offset < payload.len() {
        let op = payload[offset];
        offset += 1;
        match op {
            OP_READ => {
                let addr = read_u32(payload, &mut offset)?;
                let len = read_u16(payload, &mut offset)?;
                cmd_tx
                    .send(Command::Read { conn, addr, len })
                    .map_err(|_| ())?;
            }
            OP_WRITE => {
                let addr = read_u32(payload, &mut offset)?;
                let len = read_u16(payload, &mut offset)? as usize;
                if offset + len > payload.len() {
                    return Err(());
                }
                let bytes = payload[offset..offset + len].to_vec();
                offset += len;
                cmd_tx
                    .send(Command::Write { conn, addr, bytes })
                    .map_err(|_| ())?;
            }
            OP_INPUT => {
                if offset + 6 > payload.len() {
                    return Err(());
                }
                let channel = payload[offset];
                let active = payload[offset + 1] != 0;
                let data = u32::from_le_bytes(payload[offset + 2..offset + 6].try_into().unwrap());
                offset += 6;
                if channel >= 4 {
                    return Err(());
                }
                cmd_tx
                    .send(Command::Input {
                        conn,
                        channel,
                        active,
                        data,
                    })
                    .map_err(|_| ())?;
            }
            OP_STAT => {
                cmd_tx.send(Command::Stat { conn }).map_err(|_| ())?;
            }
            _ => return Err(()),
        }
        count += 1;
    }
    Ok(count)
}

fn encode_response(response: Response, out: &mut Vec<u8>) {
    match response {
        Response::Data { bytes, .. } => {
            out.push(OP_READ_RESP);
            out.push(STATUS_OK);
            out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            out.extend_from_slice(&bytes);
        }
        Response::Ok { marker, .. } => {
            out.push(marker);
            out.push(STATUS_OK);
        }
        Response::Stat {
            frame, rdram_size, ..
        } => {
            out.push(OP_STAT_RESP);
            out.push(STATUS_OK);
            out.extend_from_slice(&frame.to_le_bytes());
            out.extend_from_slice(&rdram_size.to_le_bytes());
        }
        Response::Bad { .. } => {
            out.push(OP_WRITE_RESP);
            out.push(STATUS_BAD);
        }
    }
}

fn read_u32(payload: &[u8], offset: &mut usize) -> Result<u32, ()> {
    if *offset + 4 > payload.len() {
        return Err(());
    }
    let value = u32::from_le_bytes(payload[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    Ok(value)
}

fn read_u16(payload: &[u8], offset: &mut usize) -> Result<u16, ()> {
    if *offset + 2 > payload.len() {
        return Err(());
    }
    let value = u16::from_le_bytes(payload[*offset..*offset + 2].try_into().unwrap());
    *offset += 2;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::mpsc;
    use std::time::Duration;

    fn read_exact(stream: &mut TcpStream, len: usize) -> Vec<u8> {
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).unwrap();
        buf
    }

    fn round_trip(stream: &mut TcpStream, payload: &[u8]) -> Vec<u8> {
        stream
            .write_all(&(payload.len() as u32).to_le_bytes())
            .unwrap();
        stream.write_all(payload).unwrap();
        stream.flush().unwrap();
        let len = u32::from_le_bytes(read_exact(stream, 4).try_into().unwrap()) as usize;
        read_exact(stream, len)
    }

    /// Exercises the wire protocol without a ROM: the test drives the same
    /// per-frame entry point the emulator uses.
    #[test]
    fn bridge_serves_read_write_input_and_stat() {
        let mut device = crate::device::Device::new(false);
        device.byte_swap = 3;
        device.rdram.size = 0x800000;
        device.rdram.mem = vec![0u8; 0x800000];

        let server = init(0).unwrap();
        let port = server.port;
        device.coop = Some(server);

        let (done_tx, done_rx) = mpsc::channel::<()>();
        let (close_tx, close_rx) = mpsc::channel::<()>();
        let client = std::thread::spawn(move || {
            let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();

            let mut payload = Vec::new();
            payload.push(OP_WRITE);
            payload.extend_from_slice(&0x8010_0000u32.to_le_bytes());
            payload.extend_from_slice(&4u16.to_le_bytes());
            payload.extend_from_slice(&[1, 2, 3, 4]);
            payload.push(OP_READ);
            payload.extend_from_slice(&0x8010_0000u32.to_le_bytes());
            payload.extend_from_slice(&4u16.to_le_bytes());
            payload.push(OP_INPUT);
            payload.push(0);
            payload.push(1);
            payload.extend_from_slice(&0x0F0Fu32.to_le_bytes());
            payload.push(OP_STAT);

            let resp = round_trip(&mut stream, &payload);
            assert_eq!(resp.len(), 26, "unexpected response length");
            assert_eq!(&resp[0..2], &[OP_WRITE_RESP, STATUS_OK]);
            assert_eq!(&resp[2..4], &[OP_READ_RESP, STATUS_OK]);
            assert_eq!(u16::from_le_bytes([resp[4], resp[5]]), 4);
            assert_eq!(&resp[6..10], &[1, 2, 3, 4]);
            assert_eq!(&resp[10..12], &[OP_INPUT_RESP, STATUS_OK]);
            assert_eq!(&resp[12..14], &[OP_STAT_RESP, STATUS_OK]);
            let frame = u64::from_le_bytes(resp[14..22].try_into().unwrap());
            let rdram_size = u32::from_le_bytes(resp[22..26].try_into().unwrap());
            assert!(frame >= 1);
            assert_eq!(rdram_size, 0x800000);

            done_tx.send(()).unwrap();
            close_rx.recv().unwrap();
        });

        let mut finished = false;
        for _ in 0..200_000 {
            if done_rx.try_recv().is_ok() {
                finished = true;
                break;
            }
            on_frame(&mut device);
        }
        assert!(finished, "client did not finish");
        assert_eq!(device.coop.as_ref().unwrap().inputs[0], Some(0x0F0F));

        close_tx.send(()).unwrap();
        client.join().unwrap();
    }
}
