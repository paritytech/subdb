/// Error type.
#[derive(Debug, derive_more::Display, derive_more::From)]
pub enum Error {
	/// An I/O error.
	#[display(fmt="I/O error: {}", _0)]
	Io(std::io::Error),

	/// Metadata is bad.
	#[display(fmt="Bad metadata")]
	BadMetadata,

	/// Unsupported version.
	#[display(fmt="Unsupported version")]
	UnsupportedVersion,

	/// The index has become full.
	#[display(fmt="Index full")]
	IndexFull,
}
impl std::error::Error for Error {}
