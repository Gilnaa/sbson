use std::sync::Arc;

use pyo3::{
    prelude::*,
    types::{IntoPyDict, PyList},
};
use sbson::{Cursor, CursorError, ElementTypeCode};

enum CursorImpl {
    Generic(sbson::Cursor<Arc<[u8]>>),
    // CachedMap(sbson::CachedMapCursor),
}

#[derive(Debug, Clone, FromPyObject)]
enum PathSegment {
    Key(String),
    Index(usize),
}

#[pyclass(name = "Cursor")]
struct PyCursor {
    path_segments: Vec<PathSegment>,
    cursor_impl: CursorImpl,
}

#[pymethods]
impl PyCursor {
    #[new]
    fn new(data: Vec<u8>) -> PyResult<Self> {
        let cursor = sbson::Cursor::new(data.into())?;
        Ok(PyCursor {
            path_segments: vec![],
            cursor_impl: CursorImpl::Generic(cursor),
        })
    }

    #[staticmethod]
    fn new_from_file(file_name: &str) -> PyResult<Self> {
        let data = std::fs::read(file_name)?;
        let cursor = Cursor::new(data.into())?;
        Ok(PyCursor {
            path_segments: vec![],
            cursor_impl: CursorImpl::Generic(cursor),
        })
    }

    fn __len__(&self, _py: Python<'_>) -> usize {
        match &self.cursor_impl {
            // CursorImpl::CachedMap(cursor) => cursor.children.len(),
            CursorImpl::Generic(cursor) => cursor.get_children_count(),
        }
    }

    fn __getattr__(&self, _py: Python<'_>, attr: &str) -> PyResult<Self> {
        let cursor = match &self.cursor_impl {
            // CursorImpl::CachedMap(cursor) => cursor.get_value_by_key(attr)?,
            CursorImpl::Generic(cursor) => cursor.get_value_by_key(attr)?,
        };
        let mut path_segments = self.path_segments.clone();
        path_segments.push(PathSegment::Key(attr.into()));
        let cursor = PyCursor {
            path_segments: path_segments,
            cursor_impl: CursorImpl::Generic(cursor),
        };
        Ok(cursor)
    }

    fn __getitem__<'a>(&'a self, index: PathSegment) -> PyResult<Self> {
        let cursor = match (&index, &self.cursor_impl) {
            // (PathSegment::Index(_), CursorImpl::CachedMap(_)) => {
            //     return Err(pyo3::exceptions::PyNotImplementedError::new_err(
            //         "Whoopsie Doopsie",
            //     ))
            // }
            (PathSegment::Index(index), CursorImpl::Generic(cursor)) => {
                cursor.get_value_by_index(*index)?
            }
            // (PathSegment::Key(key), CursorImpl::CachedMap(cursor)) => {
            //     cursor.get_value_by_key(key)?
            // }
            (PathSegment::Key(key), CursorImpl::Generic(cursor)) => cursor.get_value_by_key(key)?,
        };
        let mut path_segments = self.path_segments.clone();
        path_segments.push(index);
        let cursor = PyCursor {
            path_segments: path_segments,
            cursor_impl: CursorImpl::Generic(cursor),
        };
        Ok(cursor)
    }

    fn __repr__(&self) -> String {
        let node_type = match &self.cursor_impl {
            // CursorImpl::CachedMap(_) => ElementTypeCode::Map,
            CursorImpl::Generic(cursor) => cursor.get_element_type(),
        };
        let path = self
            .path_segments
            .iter()
            .map(|segment| match segment {
                PathSegment::Key(k) => k.clone(),
                PathSegment::Index(i) => i.to_string(),
            })
            .reduce(|acc, seg| (acc + "/") + &seg)
            .unwrap_or("".into());
        format!("<Cursor {{{node_type:?}}} @ /{path}>")
    }

    // /// Given a map node, caches key descriptor into hash-map internally
    // /// in order to reduce indexing from O(log N) to O(1).;
    // fn cache_map(&mut self) -> PyResult<()> {
    //     let map = match &self.cursor_impl {
    //         CursorImpl::CachedMap(_) => return Ok(()),
    //         CursorImpl::Generic(generic) => generic.cache_map()?,
    //     };
    //     self.cursor_impl = CursorImpl::CachedMap(map);
    //     Ok(())
    // }

    #[getter]
    fn value(&self, py: Python<'_>) -> PyResult<PyObject> {
        let cursor = match &self.cursor_impl {
            // CursorImpl::CachedMap(_) => {
            //     return Err(pyo3::exceptions::PyTypeError::new_err(
            //         "Cannot get the value of a non-leaf node.",
            //     ))
            // }
            CursorImpl::Generic(g) => g,
        };

        let value = match cursor.get_element_type() {
            ElementTypeCode::Map | ElementTypeCode::Array | ElementTypeCode::MapCHD => {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "Cannot get the value of a non-leaf node.",
                ))
            }
            ElementTypeCode::String => cursor.get_str()?.into_py(py),
            ElementTypeCode::None => py.None(),
            ElementTypeCode::True => true.into_py(py),
            ElementTypeCode::False => false.into_py(py),
            ElementTypeCode::Int32 => cursor.get_i32()?.into_py(py),
            ElementTypeCode::Int64 => cursor.get_i64()?.into_py(py),
            ElementTypeCode::UInt32 => unimplemented!(),
            ElementTypeCode::UInt64 => unimplemented!(),
            ElementTypeCode::Double => unimplemented!(),
            ElementTypeCode::Binary => unimplemented!(),
        };
        Ok(value)
    }

    /// Query along the given path and return a cursor pointing to the specified node.
    fn goto(&self, path_segments: Vec<PathSegment>) -> PyResult<Self> {
        let current_node = match &self.cursor_impl {
            CursorImpl::Generic(g) => g,
        };
        let cursor = current_node.goto(path_segments.iter().map(|seg| match seg {
            PathSegment::Key(k) => sbson::PathSegment::Key(k.as_str()),
            PathSegment::Index(i) => sbson::PathSegment::Index(*i),
        }))?;

        let mut new_path_segments = self.path_segments.clone();
        new_path_segments.extend(path_segments);

        let cursor = PyCursor {
            path_segments: new_path_segments,
            cursor_impl: CursorImpl::Generic(cursor),
        };
        Ok(cursor)
    }

    fn pythonize(&self, py: Python<'_>) -> PyResult<PyObject> {
        // If this is a map, we don't really need it to be cached,
        // since we're going to iterate the elements by order.
        let cursor = match &self.cursor_impl {
            CursorImpl::Generic(g) => g,
            // CursorImpl::CachedMap(cache) => &cache.cursor,
        };
        pythonize(py, cursor.borrow())
    }

    fn keys(&self) -> Result<Vec<&str>, CursorError> {
        let v = match &self.cursor_impl {
            CursorImpl::Generic(g) => match g.get_element_type() {
                ElementTypeCode::Map | ElementTypeCode::MapCHD => {
                    g.iter_map()?.map(|(key, _cursor)| key).collect()
                }
                _ => vec![],
            },
            // CursorImpl::CachedMap(cache) => cache.children.keys().cloned().collect(),
        };
        Ok(v)
    }
}

fn pythonize(py: Python<'_>, cursor: Cursor<&[u8]>) -> PyResult<PyObject> {
    let value = match cursor.get_element_type() {
        ElementTypeCode::Map | ElementTypeCode::MapCHD => cursor
            .iter_map()?
            .flat_map(|(key, cursor)| {
                let value = pythonize(py, cursor).ok().map(|obj| (key, obj));
                value
            })
            .into_py_dict(py)
            .into(),
        ElementTypeCode::Array => {
            let list = PyList::empty(py);
            for cursor in cursor.iter_array()? {
                let item = pythonize(py, cursor)?;
                list.append(item)?;
            }
            list.into()
        }
        ElementTypeCode::String => cursor.get_str()?.into_py(py),
        ElementTypeCode::None => py.None(),
        ElementTypeCode::True => true.into_py(py),
        ElementTypeCode::False => false.into_py(py),
        ElementTypeCode::Int32 => cursor.get_i32()?.into_py(py),
        ElementTypeCode::Int64 => cursor.get_i64()?.into_py(py),
        ElementTypeCode::UInt32 => cursor.get_u32()?.into_py(py),
        ElementTypeCode::UInt64 => cursor.get_u64()?.into_py(py),
        ElementTypeCode::Double => cursor.get_double()?.into_py(py),
        ElementTypeCode::Binary => unimplemented!(),
    };
    Ok(value)
}

#[pymodule]
#[pyo3(name = "sbson")]
fn top_level_module(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyCursor>()?;
    Ok(())
}
