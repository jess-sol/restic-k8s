#[derive(snafu::Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum AppError {
    #[snafu(display("SerializationError: {source}"))]
    SerializationError { source: serde_json::Error },

    #[snafu(display("Kube Error: {source}"))]
    KubeError { msg: String, source: kube::Error },

    #[snafu(display("Finalizer Error: {source}"))]
    // NB: awkward type because finalizer::Error embeds the reconciler error (which is this)
    // so boxing this error to break cycles
    FinalizerError {
        #[snafu(source(from(kube::runtime::finalizer::Error<AppError>, Box::new)))]
        source: Box<kube::runtime::finalizer::Error<AppError>>,
    },

    #[snafu(display("InvalidPVC"))]
    InvalidPVC,

    #[snafu(whatever, display("{message}"))]
    Whatever {
        message: String,
        #[snafu(source(from(Box<dyn std::error::Error + Send + Sync>, Some)))]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
        backtrace: snafu::Backtrace,
    },
}

impl AppError {
    pub fn metric_label(&self) -> String {
        format!("{self:?}").to_lowercase()
    }
}

pub type Result<T, E = AppError> = std::result::Result<T, E>;
