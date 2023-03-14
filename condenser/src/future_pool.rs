/// Represents a pool of futures.
pub struct FuturePool<R>
where
    R: Default + merge::Merge + Send,
{
    /// Future handles.
    handles: Vec<tokio::task::JoinHandle<R>>,

    /// Data type of the result.
    result: std::marker::PhantomData<R>,
}

impl<R> FuturePool<R>
where
    R: Default + merge::Merge + Send + 'static,
{
    /// Constructs a new `FuturePool`.
    pub fn new() -> Self {
        Self { handles: Vec::new(), result: std::marker::PhantomData::default() }
    }

    /// Spawns a new task and saved the handler.
    pub fn spawn<F>(&mut self, future: F)
    where
        F: std::future::Future<Output = R> + Send + 'static,
    {
        self.handles.push(tokio::spawn(future));
    }

    /// Waits for all the tasks to finish and merges returned results.
    pub async fn join(self) -> Result<R, tokio::task::JoinError> {
        let mut result = R::default();
        for handle in self.handles {
            result.merge(handle.await?);
        }
        Ok(result)
    }
}
