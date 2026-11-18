//! The S3 test client's requests (`support/s3.rs`) asked under a cell's watch: each on a thread of
//! its own, the client moved there and back, and the test waiting on it under the watch
//! (`Cell::watched`), so a listener that stops answering fails the test once the cell has been
//! quiet for its quiet period, rather than holding it. Apart from the client, which the node
//! crate's listener suites share with no cell.

use mantle_s3::sigv4;

use crate::cell::Cell;
use crate::s3::{Client, Response};

impl Client {
    /// What `method` on `target` came to, its body's SHA-256 signed, asked under `cell`'s watch
    /// (`Client::watched`).
    #[track_caller]
    pub(crate) fn request(
        &mut self,
        cell: &mut Cell,
        method: &str,
        target: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Response {
        let payload = sigv4::hex(&mantle_s3::crypto::sha256(body).unwrap());
        self.request_as(cell, method, target, headers, body, &payload)
    }

    /// What `method` on `target` came to, its `x-amz-content-sha256` `payload` (`Client::send_as`),
    /// asked under `cell`'s watch (`Client::watched`).
    #[track_caller]
    pub(crate) fn request_as(
        &mut self,
        cell: &mut Cell,
        method: &str,
        target: &str,
        headers: &[(&str, &str)],
        body: &[u8],
        payload: &str,
    ) -> Response {
        let what = format!("{method} {target}");
        let (method, target) = (method.to_owned(), target.to_owned());
        let (body, payload) = (body.to_vec(), payload.to_owned());
        let headers: Vec<(String, String)> = headers
            .iter()
            .map(|(n, v)| ((*n).to_owned(), (*v).to_owned()))
            .collect();
        self.watched(cell, &what, move |client| {
            let headers: Vec<(&str, &str)> = headers
                .iter()
                .map(|(n, v)| (n.as_str(), v.as_str()))
                .collect();
            client.send_as(&method, &target, &headers, &body, &payload)
        })
    }

    /// What `ask` came to, asked of this client on a thread of its own and waited on under `cell`'s
    /// watch (`Cell::watched`): a failure names `what` through this client's listener at the
    /// test's own line, and says what the cell said last.
    #[track_caller]
    pub(crate) fn watched<T: Send + 'static>(
        &mut self,
        cell: &mut Cell,
        what: &str,
        ask: impl FnOnce(&mut Self) -> T + Send + 'static,
    ) -> T {
        let what = format!("{what} through {}", self.listener(cell));
        let fresh = Self::new(self.at, self.key.clone(), &self.region);
        let mut client = std::mem::replace(self, fresh);
        let thread = std::thread::spawn(move || {
            let came = ask(&mut client);
            (client, came)
        });
        let (client, came) = cell.watched(&what, thread);
        *self = client;
        came
    }

    /// The listener this client asks, as a failure names it: its node's, where the cell heard it
    /// bound.
    fn listener(&self, cell: &Cell) -> String {
        cell.s3_bound
            .iter()
            .find(|(_, (at, _))| *at == self.at)
            .map_or_else(
                || format!("the listener at {}", self.at),
                |(id, _)| format!("node {id}'s listener"),
            )
    }
}
