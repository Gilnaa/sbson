use crate::CursorError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

impl From<CursorError> for PyErr {
    fn from(err: CursorError) -> PyErr {
        PyValueError::new_err(format!("{err:?}"))
    }
}
