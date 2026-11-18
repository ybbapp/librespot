//! Saving the original (not re-encoded) audio of a track while it plays.
//!
//! Playback decodes the track to PCM and streams it to the audio backend. The
//! bytes on the wire are the original, encrypted Ogg Vorbis / MP3 / FLAC / AAC.
//! With `--download-dir` set, this module fetches the same file again through
//! the same session, decrypts it, and writes the *original* audio to disk:
//! Ogg Vorbis is remuxed into a well-formed Ogg file (Spotify's custom header
//! packet is dropped), everything else is written as the decrypted stream.
//!
//! It runs on a thread of its own so playback is never blocked, and it goes
//! through the session's cache when one is configured, so a track played and
//! then downloaded is fetched from the network only once.

use std::{
    fs,
    io::{self, SeekFrom, Write},
    path::{Path, PathBuf},
};

use log::{debug, error, info};

use librespot_core::{FileId, Session, audio_key::AudioKey};

use crate::{
    audio::{AudioDecrypt, AudioFile},
    metadata::audio::{AudioFileFormat, AudioItem},
};

/// Starts saving `file_id` to `dir`, in the background.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_save(
    session: Session,
    handle: tokio::runtime::Handle,
    file_id: FileId,
    key: Option<AudioKey>,
    format: AudioFileFormat,
    bytes_per_second: usize,
    audio_item: AudioItem,
    dir: PathBuf,
) {
    std::thread::spawn(move || {
        match save(
            &session,
            &handle,
            file_id,
            key,
            format,
            bytes_per_second,
            &audio_item,
            &dir,
        ) {
            Ok(path) => info!("Saved <{}> to {}", audio_item.name, path.display()),
            Err(e) => error!("Could not save <{}>: {e}", audio_item.name),
        }
    });
}

#[allow(clippy::too_many_arguments)]
fn save(
    session: &Session,
    handle: &tokio::runtime::Handle,
    file_id: FileId,
    key: Option<AudioKey>,
    format: AudioFileFormat,
    bytes_per_second: usize,
    audio_item: &AudioItem,
    dir: &Path,
) -> io::Result<PathBuf> {
    if !supported(format) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("{format:?} is not a format this build can save"),
        ));
    }

    let path = dir.join(file_name(audio_item, format));
    if path.exists() {
        debug!("Already saved: {}", path.display());
        return Ok(path);
    }

    let file = handle
        .block_on(AudioFile::open(session, file_id, bytes_per_second))
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    let mut reader = AudioDecrypt::new(key, file);

    fs::create_dir_all(dir)?;
    let tmp = path.with_extension("part");
    {
        let mut out = fs::File::create(&tmp)?;
        if crate::metadata::audio::AudioFiles::is_ogg_vorbis(format) {
            write_ogg(&mut reader, &mut out, format)?;
        } else {
            io::copy(&mut reader, &mut out)?;
        }
        out.flush()?;
    }
    fs::rename(&tmp, &path)?;
    Ok(path)
}

/// Whether this build can turn the format into a playable file.
fn supported(format: AudioFileFormat) -> bool {
    if crate::metadata::audio::AudioFiles::is_ogg_vorbis(format) {
        // Remuxing needs the passthrough decoder.
        cfg!(feature = "passthrough-decoder")
    } else {
        true
    }
}

/// Remuxes Spotify's Ogg Vorbis into a well-formed Ogg file, dropping the
/// custom header packet players balk at. The audio packets are copied as they
/// are, so the file holds the original compressed audio, not a re-encode.
#[cfg(feature = "passthrough-decoder")]
fn write_ogg<R: io::Read + io::Seek, W: Write>(
    mut reader: R,
    out: &mut W,
    format: AudioFileFormat,
) -> io::Result<()> {
    use crate::decoder::{AudioDecoder, AudioPacket, PassthroughDecoder};

    // The stream Spotify serves begins with a custom, malformed header packet;
    // the Ogg stream itself starts after it, where playback reads from too.
    reader.seek(SeekFrom::Start(crate::player::SPOTIFY_OGG_HEADER_END))?;

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
fn write_ogg<R: io::Read + io::Seek, W: Write>(
    _reader: R,
    _out: &mut W,
    _format: AudioFileFormat,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "saving Ogg Vorbis needs the passthrough-decoder feature",
    ))
}

/// "Artists - Title.ext", with what a file name may not hold taken out.
fn file_name(item: &AudioItem, format: AudioFileFormat) -> String {
    let artist = sanitize(&item.unique_fields.artists_display());
    let artist = if artist.is_empty() {
        "Unknown Artist".to_string()
    } else {
        artist
    };
    let name = sanitize(&item.name);
    let name = if name.is_empty() {
        item.track_id.to_id()
    } else {
        name
    };
    format!("{artist} - {name}.{}", extension(format))
}

fn extension(format: AudioFileFormat) -> &'static str {
    use AudioFileFormat::*;
    match format {
        OGG_VORBIS_96 | OGG_VORBIS_160 | OGG_VORBIS_320 => "ogg",
        MP3_256 | MP3_320 | MP3_160 | MP3_96 | MP3_160_ENC => "mp3",
        FLAC_FLAC | FLAC_FLAC_24BIT => "flac",
        AAC_24 | AAC_48 | AAC_160 | AAC_320 => "m4a",
        XHE_AAC_12 | XHE_AAC_16 | XHE_AAC_24 => "m4a",
        MP4_128 => "m4a",
        OTHER5 => "bin",
    }
}

/// Keeps a name within one path component: separators and control characters
/// become underscores, and it is capped so the file name fits.
fn sanitize(text: &str) -> String {
    let cleaned: String = text
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\n' | '\r' | '\t' => '_',
            c if (c as u32) < 0x20 => '_',
            c => c,
        })
        .collect();
    let trimmed = cleaned.trim().trim_matches('.').to_string();
    if trimmed.chars().count() > 180 {
        trimmed.chars().take(180).collect()
    } else {
        trimmed
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    // Builds a stream shaped like Spotify's: 0xa7 bytes of custom header, then
    // a well-formed Ogg stream. The remux must skip the header and copy the Ogg.
    #[test]
    #[cfg(feature = "passthrough-decoder")]
    fn remuxes_ogg_after_the_custom_header() {
        let ogg = include_bytes!("../tests/data/tone.ogg");
        let mut stream = vec![0u8; crate::player::SPOTIFY_OGG_HEADER_END as usize];
        stream.extend_from_slice(ogg);

        let mut out = Vec::new();
        write_ogg(io::Cursor::new(stream), &mut out, AudioFileFormat::OGG_VORBIS_320).unwrap();

        assert!(out.starts_with(b"OggS"), "output is an Ogg stream");
        assert!(out.len() > 1000, "audio packets were written, got {}", out.len());

        // What was written has to be a stream a decoder can actually read,
        // which is what makes the saved file playable.
        use crate::decoder::{AudioDecoder, SymphoniaDecoder};
        use symphonia::core::probe::Hint;
        let mut hint = Hint::new();
        hint.mime_type("audio/ogg");
        let mut decoder =
            SymphoniaDecoder::new(io::Cursor::new(out), hint).expect("saved file is decodable");
        let mut packets = 0;
        while let Some(_) = decoder.next_packet().unwrap() {
            packets += 1;
        }
        assert!(packets > 0, "the saved file decodes to audio");
    }

    #[test]
    fn names_the_file_from_the_credits() {
        // sanitize is what keeps a name within one path component.
        assert_eq!(sanitize("A/B: C*?"), "A_B_ C__");
        assert_eq!(sanitize("  ..Trailing..  "), "Trailing");
    }
}
