use crate::config::*;
use crate::storage::row::TupleError;
use crate::{Error, Result};
use std::convert::TryInto;

#[derive(Default, Clone, Serialize, Deserialize, Debug)]
pub struct TableSchema {
    columns: Vec<Column>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Column {
    /// Data type.
    pub type_: ColumnType,
    /// Offset from tuple start.
    pub offset: usize,
    pub name: String,
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnType {
    Int64,
    Double,
    String { len: usize },
}

impl ColumnType {
    pub fn len(&self) -> usize {
        match *self {
            ColumnType::Int64 => 8,
            ColumnType::Double => 8,
            ColumnType::String { len } => len,
        }
    }
}

use crate::storage::row::TUPLE_HEADER;

impl TableSchema {
    pub fn new() -> Self {
        TableSchema {
            columns: Vec::new(),
        }
    }

    pub fn push(&mut self, column: ColumnType, column_name: &str) {
        let c = Column {
            type_: column,
            offset: self.tuple_size(),
            name: String::from(column_name),
        };
        self.columns.push(c);
    }

    pub fn tuple_size(&self) -> usize {
        self.columns
            .last()
            .map(|c| c.offset + c.type_.len())
            .unwrap_or(TUPLE_HEADER)
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn get_column_offset(&self, id: usize) -> core::ops::Range<usize> {
        // println!("{} {} {}", self.columns.len(), self.columns[3].name, id);
        let column = &self.columns[id];
        column.offset..column.offset + column.type_.len()
    }

    pub fn get_column_type(&self, id: usize) -> ColumnType {
        let column = &self.columns[id];
        column.type_
    }

    pub fn column_to_bytes(column: &Column) -> Vec<u8> {
        let mut bytes = Vec::new();
        match column.type_ {
            ColumnType::Int64 => {
                bytes.append(&mut vec![0, 0]);
            }
            ColumnType::Double => {
                bytes.append(&mut vec![0, 1]);
            }
            ColumnType::String { len } => {
                bytes.append(&mut vec![1, 0]);
                bytes.extend_from_slice(&(len.to_le_bytes()));
            } // _ => {
              //     assert!(false);
              // }
        }
        bytes.extend_from_slice(&(column.offset.to_le_bytes()));
        let mut name_bytes = column.name.clone().into_bytes();

        bytes.append(&mut name_bytes);

        bytes
    }

    /// schema:
    /// |column_count(usize)|column_len(usize)|column_detail|column_len(usize)|column_detail|
    ///
    /// column_detail:
    /// |type(2bit) + len(usize only String)|offset(usize)|name_len(usize)|name(string)|
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut schema_bytes = Vec::<u8>::new();
        schema_bytes.extend_from_slice(&self.columns.len().to_le_bytes());
        for column in &self.columns {
            let mut bytes = TableSchema::column_to_bytes(&column);
            schema_bytes.extend_from_slice(&bytes.len().to_le_bytes());
            schema_bytes.append(&mut bytes);
        }
        schema_bytes
    }

    pub fn add_column_by_bytes(&mut self, bytes: &[u8]) {
        let (size, columns) = bytes.split_at(2);
        let mut columns = columns;
        let mut column = Column {
            type_: ColumnType::Int64,
            offset: 0,
            name: String::from(""),
        };

        column.type_ = match size {
            [0, 0] => ColumnType::Int64,
            [0, 1] => ColumnType::Double,
            [1, 0] => {
                let (len, other) = columns.split_at(USIZE_OFFSET as usize);
                columns = other;
                let len = usize::from_le_bytes(len.try_into().unwrap());
                ColumnType::String { len }
            }
            _ => {
                assert!(false);
                ColumnType::Int64
            }
        };

        let (offset, name) = columns.split_at(USIZE_OFFSET as usize);
        column.offset = usize::from_le_bytes(offset.try_into().unwrap());

        column.name = String::from_utf8(Vec::from(name)).unwrap();
        self.columns.push(column);
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut schema = TableSchema::new();
        let (size, columns) = bytes.split_at(USIZE_OFFSET as usize);
        let mut columns = columns;
        let size: usize = usize::from_le_bytes(size.try_into().unwrap());

        for _ in 0..size {
            let (size, other) = columns.split_at(USIZE_OFFSET as usize);
            let size: usize = usize::from_le_bytes(size.try_into().unwrap());
            let (column, other) = other.split_at(size);
            columns = other;
            schema.add_column_by_bytes(column);
        }

        schema
    }
    pub fn search_by_name(&self, name: &str) -> Result<usize> {
        for (i, column) in self.columns.iter().enumerate() {
            if column.name == name {
                // println!("search {}, {}", name, i);
                return Ok(i);
            }
        }
        Err(Error::Tuple(TupleError::ColumnNotExists))
    }
}
