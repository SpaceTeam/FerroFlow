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
        if let Err(error) = receive_frame(&socket, &sender) {
            eprintln!("CAN receive thread error: {error:#}");
        }
    }
}

fn can_send_thread(socket: Arc<CanFdSocket>, receiver: mpsc::Receiver<CanAnyFrame>) {
    while let Ok(frame) = receiver.recv() {
        if let Err(error) = send_frame(&socket, &frame) {
            eprintln!("CAN send thread error: {error:#}");
        }
    }
}

fn receive_frame(socket: &CanFdSocket, sender: &mpsc::Sender<CanAnyFrame>) -> Result<()> {
    let frame = socket.read_frame().context("failed to read CAN frame")?;
    sender
        .send(frame)
        .context("failed to forward received CAN frame to channel")?;
    Ok(())
}

fn send_frame(socket: &CanFdSocket, frame: &CanAnyFrame) -> Result<()> {
    socket
        .write_frame(frame)
        .context("failed to send CAN frame")?;
    Ok(())
}
