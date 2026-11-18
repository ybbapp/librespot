//! A small HTTP server that hands out a track's original audio, to another
//! program on this machine.
//!
//! It exists for apps that play Spotify through this librespot but save the
//! files themselves, such as MyGo Music's download manager: they ask
//! `GET /stream?uri=spotify:track:<id>&quality=high` and read the original,
//! not re-encoded, audio from the body. Ogg Vorbis is remuxed into a
//! well-formed Ogg stream; MP3, FLAC and AAC are the decrypted bytes as they
//! are.
//!
//! It answers on the loopback address only, and it uses the same Session as
//! playback, so a track already in the audio cache is read from there.

use std::{
    convert::Infallible,
    io::{self, Read, Seek, SeekFrom},
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Method, Request, Response, StatusCode, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use log::{debug, error, info};

use librespot::{
    audio::{AudioDecrypt, AudioFile},
    core::{Session, SpotifyId, SpotifyUri},
    metadata::audio::{AudioFileFormat, AudioFiles, AudioItem},
};

#[cfg(feature = "passthrough-decoder")]
use librespot::playback::decoder::{AudioDecoder, AudioPacket, PassthroughDecoder};

/// The session playback is using, or none until one is signed in. It is
/// replaced whenever the session is, as on reconnect.
pub type Sessions = Arc<Mutex<Option<Session>>>;

/// Spotify's custom, malformed Ogg header packet ends here; the Ogg stream
/// itself begins after it, where playback reads from too.
const SPOTIFY_OGG_HEADER_END: u64 = 0xa7;

/// Starts the server on `127.0.0.1:port`, answering in the background.
pub fn spawn(port: u16, sessions: Sessions) -> io::Result<u16> {
    let listener = std::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port)))?;
    let port = listener.local_addr()?.port();
    listener.set_nonblocking(true)?;

    tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::from_std(listener) {
            Ok(l) => l,
            Err(e) => {
                error!("stream server: {e}");
                return;
            }
        };
        info!("Stream server listening on 127.0.0.1:{port}");
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    error!("stream server: {e}");
                    continue;
                }
            };
            let sessions = sessions.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = service_fn(move |req| {
                    let sessions = sessions.clone();
                    async move { Ok::<_, Infallible>(handle(sessions, req).await) }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await;
            });
        }
    });
    Ok(port)
}

async fn handle(sessions: Sessions, req: Request<Incoming>) -> Response<Full<Bytes>> {
    if req.method() != Method::GET || req.uri().path() != "/stream" {
        return status(StatusCode::NOT_FOUND, "not found");
    }
    let query = req.uri().query().unwrap_or("");
    let mut uri = None;
    let mut quality = "high".to_string();
    for (k, v) in form_urlencoded::parse(query.as_bytes()) {
        match k.as_ref() {
            "uri" => uri = Some(v.into_owned()),
            "quality" => quality = v.into_owned(),
            _ => {}
        }
    }
    let Some(uri) = uri else {
        return status(StatusCode::BAD_REQUEST, "uri is required");
    };
    let spotify_uri = match SpotifyUri::from_uri(&uri) {
        Ok(u) => u,
        Err(_) => return status(StatusCode::BAD_REQUEST, "uri is not a Spotify URI"),
    };

    let session = match sessions.lock() {
        Ok(s) => s.clone(),
        Err(_) => None,
    };
    let Some(session) = session else {
        return status(StatusCode::SERVICE_UNAVAILABLE, "not signed in yet");
    };

    match render(&session, spotify_uri, &quality).await {
        Ok((body, mime)) => {
            let mut res = Response::new(Full::new(body));
            res.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static(mime),
            );
            res
        }
        Err(e) => {
            debug!("stream server: {uri}: {e}");
            status(StatusCode::BAD_GATEWAY, "couldn't get the audio")
        }
    }
}

fn status(code: StatusCode, message: &str) -> Response<Full<Bytes>> {
    let mut res = Response::new(Full::new(Bytes::from(message.to_string())));
    *res.status_mut() = code;
    res
}

async fn render(
    session: &Session,
    uri: SpotifyUri,
    quality: &str,
) -> Result<(Bytes, &'static str), String> {
    let track_id: SpotifyId = (&uri).try_into().map_err(|_| "not a track".to_string())?;

    let item = AudioItem::get_file(session, uri)
        .await
        .map_err(|e| format!("metadata: {e}"))?;

    let (format, file_id) = choose(&item.files, quality).ok_or("no audio for this track")?;
    let bytes_per_second = estimate(format);

    let file = AudioFile::open(session, file_id, bytes_per_second)
        .await
        .map_err(|e| format!("open: {e}"))?;
    let key = session.audio_key().request(track_id, file_id).await.ok();

    let mime = "audio/ogg";
    let body = tokio::task::spawn_blocking(move || collect(file, key, format))
        .await
        .map_err(|e| format!("join: {e}"))?
        .map_err(|e| format!("read: {e}"))?;

    Ok((Bytes::from(body), mime))
}

/// Reads the decrypted stream whole: Ogg Vorbis remuxed into a well-formed Ogg
/// file, everything else as the decrypted bytes.
fn collect(file: AudioFile, key: Option<librespot::core::audio_key::AudioKey>, format: AudioFileFormat) -> io::Result<Vec<u8>> {
    let mut reader = AudioDecrypt::new(key, file);
    let mut out = Vec::new();
    if AudioFiles::is_ogg_vorbis(format) {
        write_ogg(&mut reader, &mut out, format)?;
    } else {
        reader.seek(SeekFrom::Start(0))?;
        io::copy(&mut reader, &mut out)?;
    }
    Ok(out)
}

/// Remuxes Spotify's Ogg Vorbis into a well-formed Ogg file, dropping the
/// custom header packet. The audio packets are copied as they are.
#[cfg(feature = "passthrough-decoder")]
fn write_ogg<R: Read + Seek, W: io::Write>(
    mut reader: R,
    out: &mut W,
    format: AudioFileFormat,
) -> io::Result<()> {
    reader.seek(SeekFrom::Start(SPOTIFY_OGG_HEADER_END))?;
    let mut decoder = PassthroughDecoder::new(reader, format)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    while let Some((_position, packet)) = decoder
        .next_packet()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
    {
        if let AudioPacket::Raw(bytes) = packet {
            out.write_all(&bytes)?;
        }
    }
    Ok(())
}

#[cfg(not(feature = "passthrough-decoder"))]
fn write_ogg<R: Read + Seek, W: io::Write>(
    _reader: R,
    _out: &mut W,
    _format: AudioFileFormat,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "needs the passthrough-decoder feature",
    ))
}

/// The Ogg Vorbis to serve at this quality, best first. Ogg Vorbis alone keeps
/// the file's type, and so its name, known before it is fetched, and it is what
/// Spotify serves at every quality.
fn choose(files: &AudioFiles, quality: &str) -> Option<(AudioFileFormat, librespot::core::FileId)> {
    use AudioFileFormat::*;
    let order: &[AudioFileFormat] = match quality {
        "standard" => &[OGG_VORBIS_96, OGG_VORBIS_160, OGG_VORBIS_320],
        "lossless" => &[OGG_VORBIS_320, OGG_VORBIS_160, OGG_VORBIS_96],
        _ => &[OGG_VORBIS_320, OGG_VORBIS_160, OGG_VORBIS_96],
    };
    order
        .iter()
        .find_map(|format| files.get(format).map(|file_id| (*format, *file_id)))
}

/// A rough rate, only used to size the download requests. It mirrors what
/// playback asks AudioFile with.
fn estimate(format: AudioFileFormat) -> usize {
    let kbps: f64 = match format {
        AudioFileFormat::OGG_VORBIS_96 | AudioFileFormat::MP3_96 => 12.0,
        AudioFileFormat::OGG_VORBIS_160 | AudioFileFormat::MP3_160 | AudioFileFormat::MP3_160_ENC => 20.0,
        AudioFileFormat::OGG_VORBIS_320 | AudioFileFormat::MP3_320 => 40.0,
        AudioFileFormat::FLAC_FLAC | AudioFileFormat::FLAC_FLAC_24BIT => 112.0,
        _ => 40.0,
    };
    (kbps * 1024.0).ceil() as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn get(port: u16, path: &str) -> (StatusCode, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut conn = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
        conn.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        conn.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf).to_string();
        let code = text
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse::<u16>().ok())
            .unwrap_or(0);
        (StatusCode::from_u16(code).unwrap(), text)
    }

    #[tokio::test]
    async fn answers_only_its_own_paths() {
        // No session: the server still has to answer, saying it isn't signed in.
        let sessions: Sessions = Arc::new(Mutex::new(None));
        let port = spawn(0, sessions).unwrap();

        let (code, _) = get(port, "/").await;
        assert_eq!(code, StatusCode::NOT_FOUND);

        let (code, _) = get(port, "/stream").await;
        assert_eq!(code, StatusCode::BAD_REQUEST, "uri is required");

        let (code, _) = get(port, "/stream?uri=not-a-uri").await;
        assert_eq!(code, StatusCode::BAD_REQUEST);

        let (code, body) = get(port, "/stream?uri=spotify:track:4cOdK2wGLETKBW3PvgPWqT").await;
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("not signed in"));
    }
}
