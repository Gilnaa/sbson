use pyo3::{
    prelude::*,
    types::{IntoPyDict, PyList},
};
use sbson::{BorrowedCursor, CursorError, ElementTypeCode};

enum CursorImpl {
    Generic(sbson::ArcCursor),
    CachedMap(sbson::CachedMapCursor),
}

#[derive(Debug, Clone, FromPyObject)]
enum PathSegment {
    Key(String),
    Index(usize),
}

#[pyclass]
struct Cursor {
    path_segments: Vec<PathSegment>,
    cursor_impl: CursorImpl,
}

#[pymethods]
impl Cursor {
    #[new]
    fn new(data: &[u8]) -> PyResult<Self> {
        let cursor = sbson::ArcCursor::new(data)?;
        Ok(Cursor {
            path_segments: vec![],
            cursor_impl: CursorImpl::Generic(cursor),
        })
    }

    #[staticmethod]
    fn new_from_file(file_name: &str) -> PyResult<Self> {
        let data = std::fs::read(file_name)?;
        let cursor = sbson::ArcCursor::new(data)?;
        Ok(Cursor {
            path_segments: vec![],
            cursor_impl: CursorImpl::Generic(cursor),
        })
    }

    fn __len__(&self, _py: Python<'_>) -> usize {
        match &self.cursor_impl {
            CursorImpl::CachedMap(cursor) => cursor.children.len(),
            CursorImpl::Generic(cursor) => cursor.get_children_count(),
        }
    }

    fn __getattr__(&self, _py: Python<'_>, attr: &str) -> PyResult<Self> {
        let cursor = match &self.cursor_impl {
            CursorImpl::CachedMap(cursor) => cursor.get_value_by_key(attr)?,
            CursorImpl::Generic(cursor) => cursor.get_value_by_key(attr)?,
        };
        let mut path_segments = self.path_segments.clone();
        path_segments.push(PathSegment::Key(attr.into()));
        let cursor = Cursor {
            path_segments: path_segments,
            cursor_impl: CursorImpl::Generic(cursor),
        };
        Ok(cursor)
    }

    fn __getitem__<'a>(&'a self, index: PathSegment) -> PyResult<Self> {
        let cursor = match (&index, &self.cursor_impl) {
            (PathSegment::Index(_), CursorImpl::CachedMap(_)) => {
                return Err(pyo3::exceptions::PyNotImplementedError::new_err(
                    "Whoopsie Doopsie",
                ))
            }
            (PathSegment::Index(index), CursorImpl::Generic(cursor)) => {
                cursor.get_value_by_index(*index)?
            }
            (PathSegment::Key(key), CursorImpl::CachedMap(cursor)) => {
                cursor.get_value_by_key(key)?
            }
            (PathSegment::Key(key), CursorImpl::Generic(cursor)) => cursor.get_value_by_key(key)?,
        };
        let mut path_segments = self.path_segments.clone();
        path_segments.push(index);
        let cursor = Cursor {
            path_segments: path_segments,
            cursor_impl: CursorImpl::Generic(cursor),
        };
        Ok(cursor)
    }

    fn __repr__(&self) -> String {
        let node_type = match &self.cursor_impl {
            CursorImpl::CachedMap(_) => ElementTypeCode::Map,
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

    /// Given a map node, caches key descriptor into hash-map internally
    /// in order to reduce indexing from O(log N) to O(1).;
    fn cache_map(&mut self) -> PyResult<()> {
        let map = match &self.cursor_impl {
            CursorImpl::CachedMap(_) => return Ok(()),
            CursorImpl::Generic(generic) => generic.cache_map()?,
        };
        self.cursor_impl = CursorImpl::CachedMap(map);
        Ok(())
    }

    #[getter]
    fn value(&self, py: Python<'_>) -> PyResult<PyObject> {
        let cursor = match &self.cursor_impl {
            CursorImpl::CachedMap(_) => {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "Cannot get the value of a non-leaf node.",
                ))
            }
            CursorImpl::Generic(g) => g,
        };

        let value = match cursor.get_element_type() {
            ElementTypeCode::Map | ElementTypeCode::Array => {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "Cannot get the value of a non-leaf node.",
                ))
            }
            ElementTypeCode::String => cursor.parse_str()?.into_py(py),
            ElementTypeCode::None => py.None(),
            ElementTypeCode::True => true.into_py(py),
            ElementTypeCode::False => false.into_py(py),
            ElementTypeCode::Int32 => cursor.parse_i32()?.into_py(py),
            ElementTypeCode::Int64 => cursor.parse_i64()?.into_py(py),
            ElementTypeCode::UInt32 => unimplemented!(),
            ElementTypeCode::UInt64 => unimplemented!(),
            ElementTypeCode::Double => unimplemented!(),
            ElementTypeCode::Binary => unimplemented!(),
        };
        Ok(value)
    }

    /// Query along the given path and return a cursor pointing to the specified node.
    fn goto(&self, path_segments: Vec<PathSegment>) -> PyResult<Self> {
        let mut depth = 0;
        let mut current_node = None;
        for segment in path_segments {
            let next_cursor = current_node.as_ref().unwrap_or(self).__getitem__(segment)?;
            // TODO Return better error with depth+segment in it.
            current_node = Some(next_cursor);
            depth += 1;
        }
        current_node.ok_or(CursorError::KeyNotFound.into())
    }

    fn pythonize(&self, py: Python<'_>) -> PyResult<PyObject> {
        // If this is a map, we don't really need it to be cached,
        // since we're going to iterate the elements by order.
        let cursor = match &self.cursor_impl {
            CursorImpl::Generic(g) => g,
            CursorImpl::CachedMap(cache) => &cache.cursor,
        };
        pythonize(py, cursor.borrow())
    }

    // TODO: Return Vec<CStr>/Vec<PyStr> to avoid double-allocation per key (second copy happens when moving key to python)
    fn keys(&self) -> Result<Vec<String>, CursorError> {
        let v = match &self.cursor_impl {
            CursorImpl::Generic(g) => match g.get_element_type() {
                ElementTypeCode::Map => g.borrow().iter_map()?.map(|(key, _cursor)| key).collect(),
                _ => vec![],
            },
            CursorImpl::CachedMap(cache) => cache.children.keys().cloned().collect(),
        };
        Ok(v)
    }
}

fn pythonize(py: Python<'_>, cursor: BorrowedCursor<'_>) -> PyResult<PyObject> {
    let value = match cursor.get_element_type() {
        ElementTypeCode::Map => cursor
            .iter_map()?
            .flat_map(|(key, cursor)| pythonize(py, cursor).ok().map(|obj| (key, obj)))
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
        ElementTypeCode::String => cursor.parse_str()?.into_py(py),
        ElementTypeCode::None => py.None(),
        ElementTypeCode::True => true.into_py(py),
        ElementTypeCode::False => false.into_py(py),
        ElementTypeCode::Int32 => cursor.parse_i32()?.into_py(py),
        ElementTypeCode::Int64 => cursor.parse_i64()?.into_py(py),
        ElementTypeCode::UInt32 => unimplemented!(),
        ElementTypeCode::UInt64 => unimplemented!(),
        ElementTypeCode::Double => unimplemented!(),
        ElementTypeCode::Binary => unimplemented!(),
    };
    Ok(value)
}

#[pymodule]
#[pyo3(name = "sbson")]
fn top_level_module(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Cursor>()?;
    Ok(())
}
