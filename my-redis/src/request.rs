use tokio::sync::oneshot;
use crate::cmd::Command;
use crate::frame::Frame;

#[derive(Debug)]
pub struct Request {
    pub cmd: Command,
    pub sender: oneshot::Sender<Frame>,
}

impl Request {
    pub fn new(cmd: Command) -> (Request, oneshot::Receiver<Frame>) {
        let (sender, receiver) = oneshot::channel::<Frame>();
        let request = Request {
            cmd,
            sender,
        };
        (request, receiver)
    }

    pub fn set_command(&mut self, cmd: Command) {
        self.cmd = cmd
    }
}