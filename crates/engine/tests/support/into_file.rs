pub fn finished_file<O, F>(
    outcome: mantle_engine::store::IntoFile<O, F>,
) -> (F, Result<(), mantle_engine::Error>) {
    match outcome {
        mantle_engine::store::IntoFile::Finished { file, result } => (file, result),
        mantle_engine::store::IntoFile::Refused { error, .. } => {
            panic!("cold file extraction refused: {error:?}")
        }
    }
}
