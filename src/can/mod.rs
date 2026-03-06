//! Contains code related to sending/receiving CAN messages.mod can_thread;

use std::{
    sync::{Arc, mpsc},
    thread,
};

use anyhow::{Context, Result};
use socketcan::{CanAnyFrame, CanFdSocket, Socket};

type CanThreadHandles = [thread::JoinHandle<()>; 2];

pub fn spawn_can_threads(
    interface: &str,
) -> Result<(
    mpsc::Receiver<CanAnyFrame>,
    mpsc::Sender<CanAnyFrame>,
    CanThreadHandles,
)> {
    let socket =
        Arc::new(CanFdSocket::open(interface).with_context(|| {
            format!("failed to open can fd socket for interface {}", interface)
        })?);

    let (recv_sender, recv_receiver) = mpsc::channel();
    let (send_sender, send_receiver) = mpsc::channel();

    let socket_clone = Arc::clone(&socket);
    let handle1 = std::thread::spawn(move || can_recv_thread(socket_clone, recv_sender));
    let handle2 = std::thread::spawn(move || can_send_thread(socket, send_receiver));

    Ok((recv_receiver, send_sender, [handle1, handle2]))
}

fn can_recv_thread(socket: Arc<CanFdSocket>, sender: mpsc::Sender<CanAnyFrame>) {
    loop {
        match socket.read_frame() {
            Ok(frame) => {
                if let Err(e) = sender.send(frame) {
                    eprintln!("Failed to send received CAN frame to channel: {}", e);
                }
            }
            Err(e) => eprintln!("Failed to read CAN frame: {}", e),
        }
    }
}

fn can_send_thread(socket: Arc<CanFdSocket>, receiver: mpsc::Receiver<CanAnyFrame>) {
    while let Ok(frame) = receiver.recv() {
        if let Err(e) = socket.write_frame(&frame) {
            eprintln!("Failed to send CAN frame: {}", e);
        }
    }
}
