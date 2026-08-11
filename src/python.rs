//! pyo3 bindings for the `ptymini` Python package (`ptymini._core`).

use pyo3::exceptions::{PyProcessLookupError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::collections::HashMap;
use std::sync::Arc;

use crate::ring::Ring;
use crate::session::PtyCore;

#[pyclass(name = "Ring")]
struct PyRing {
    inner: Ring,
}

#[pymethods]
impl PyRing {
    #[new]
    #[pyo3(signature = (max_bytes=1_000_000))]
    fn new(max_bytes: i64) -> PyResult<Self> {
        if max_bytes <= 0 {
            return Err(PyValueError::new_err("max_bytes must be > 0"));
        }
        Ok(Self {
            inner: Ring::new(max_bytes as usize),
        })
    }

    fn append(&mut self, data: Vec<u8>) {
        self.inner.append(&data);
    }

    #[pyo3(signature = (offset, max_bytes_out=None))]
    fn read_from<'py>(
        &self,
        py: Python<'py>,
        offset: u64,
        max_bytes_out: Option<usize>,
    ) -> (Bound<'py, PyBytes>, u64, u64) {
        let (d, off, dropped) = self.inner.read_from(offset, max_bytes_out);
        (PyBytes::new(py, &d), off, dropped)
    }

    #[getter]
    fn start(&self) -> u64 {
        self.inner.start
    }
    #[getter]
    fn end(&self) -> u64 {
        self.inner.end
    }
    #[getter]
    fn max_bytes(&self) -> usize {
        self.inner.max_bytes
    }
    fn __len__(&self) -> usize {
        self.inner.len()
    }
}

#[pyclass(name = "PtyCore", frozen)]
struct PyPtyCore {
    core: PtyCore,
}

#[pymethods]
impl PyPtyCore {
    #[new]
    #[pyo3(signature = (argv, cwd=None, env=None, rows=24, cols=80, buffer_bytes=1_000_000))]
    fn new(
        argv: Vec<String>,
        cwd: Option<String>,
        env: Option<HashMap<String, String>>,
        rows: u16,
        cols: u16,
        buffer_bytes: i64,
    ) -> PyResult<Self> {
        if buffer_bytes <= 0 {
            return Err(PyValueError::new_err("buffer_bytes must be > 0"));
        }
        let core = PtyCore::spawn(
            &argv,
            cwd.as_deref(),
            env.as_ref(),
            rows,
            cols,
            buffer_bytes as usize,
        )?;
        Ok(Self { core })
    }

    /// Replace the change hook. It runs on the reader thread after every ring
    /// append and once at EOF; exceptions are printed and swallowed.
    fn set_callback(&self, cb: Py<PyAny>) {
        self.core.set_callback(Arc::new(move || {
            Python::attach(|py| {
                if let Err(e) = cb.call0(py) {
                    e.print(py);
                }
            });
        }));
    }

    fn write(&self, data: Vec<u8>) -> PyResult<()> {
        Ok(self.core.write(&data)?)
    }

    fn resize(&self, rows: u16, cols: u16) -> PyResult<()> {
        Ok(self.core.resize(rows, cols)?)
    }

    fn kill(&self, sig: i32) -> PyResult<()> {
        match self.core.kill(sig) {
            Err(e) if e.raw_os_error() == Some(libc::ESRCH) => {
                Err(PyProcessLookupError::new_err("process not found"))
            }
            r => Ok(r?),
        }
    }

    #[pyo3(signature = (offset, max_bytes_out=None))]
    fn read_from<'py>(
        &self,
        py: Python<'py>,
        offset: u64,
        max_bytes_out: Option<usize>,
    ) -> (Bound<'py, PyBytes>, u64, u64) {
        let (d, off, dropped) = self.core.read_from(offset, max_bytes_out);
        (PyBytes::new(py, &d), off, dropped)
    }

    #[pyo3(signature = (seen_end=None, timeout=None))]
    fn wait_change(&self, py: Python, seen_end: Option<u64>, timeout: Option<f64>) -> bool {
        py.detach(|| self.core.wait_change(seen_end, timeout))
    }

    #[pyo3(signature = (timeout=None))]
    fn wait(&self, py: Python, timeout: Option<f64>) -> Option<i32> {
        py.detach(|| self.core.wait(timeout))
    }

    #[getter]
    fn start(&self) -> u64 {
        self.core.start()
    }
    #[getter]
    fn end(&self) -> u64 {
        self.core.end()
    }
    #[getter]
    fn alive(&self) -> bool {
        self.core.alive()
    }
    #[getter]
    fn exit_code(&self) -> Option<i32> {
        self.core.exit_code()
    }
    #[getter]
    fn last_activity(&self) -> f64 {
        self.core.last_activity()
    }
    #[setter]
    fn set_last_activity(&self, v: f64) {
        self.core.set_last_activity(v)
    }
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRing>()?;
    m.add_class::<PyPtyCore>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
