//! Encoding and decoding utilities for storing structured data in the LSM tree.
//!
//! This module provides:
//! - Order-preserving key encoding for composite primary keys
//! - Row encoding/decoding using MessagePack

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::scalar::ScalarValue;
use rmpv::Value;

/// Encodes a table prefix and primary key columns into an order-preserving byte key.
///
/// The key format is: [table_id (4 bytes big-endian)] [encoded pk columns...]
///
/// Order-preserving encoding ensures that byte comparison matches logical comparison:
/// - Integers are encoded with sign bit flipped and big-endian
/// - Strings are encoded with null byte escaping
/// - NULLs sort before non-NULL values
pub fn encode_key(table_id: u32, pk_values: &[ScalarValue]) -> Vec<u8> {
    let mut key = Vec::with_capacity(64);

    // Table ID prefix (big-endian for correct ordering)
    key.extend_from_slice(&table_id.to_be_bytes());

    // Encode each primary key column
    for value in pk_values {
        encode_scalar_ordered(value, &mut key);
    }

    key
}

/// Decodes a key (without table prefix) back to ScalarValues given the column types.
/// Returns the decoded values.
pub fn decode_key(bytes: &[u8], pk_types: &[&DataType]) -> Vec<ScalarValue> {
    let mut values = Vec::with_capacity(pk_types.len());
    let mut pos = 0;

    for data_type in pk_types {
        if pos >= bytes.len() {
            break;
        }

        let (value, consumed) = decode_scalar_ordered(&bytes[pos..], data_type);
        values.push(value);
        pos += consumed;
    }

    values
}

/// Decodes a single ScalarValue from order-preserving encoding.
/// Returns the decoded value and the number of bytes consumed.
fn decode_scalar_ordered(bytes: &[u8], data_type: &DataType) -> (ScalarValue, usize) {
    if bytes.is_empty() {
        return (ScalarValue::Null, 0);
    }

    // Check for null marker
    if bytes[0] == 0x00 {
        return (null_for_type(data_type), 1);
    }

    // Skip non-null marker (0x01)
    let bytes = &bytes[1..];

    match data_type {
        DataType::Boolean => {
            let val = bytes.get(0).map(|&b| b != 0);
            (ScalarValue::Boolean(val), 2)
        }
        DataType::Int8 => {
            let val = bytes.get(0).map(|&b| (b ^ 0x80) as i8);
            (ScalarValue::Int8(val), 2)
        }
        DataType::Int16 => {
            if bytes.len() >= 2 {
                let encoded = u16::from_be_bytes(bytes[..2].try_into().unwrap());
                let val = (encoded ^ 0x8000) as i16;
                (ScalarValue::Int16(Some(val)), 3)
            } else {
                (ScalarValue::Int16(None), 1)
            }
        }
        DataType::Int32 => {
            if bytes.len() >= 4 {
                let encoded = u32::from_be_bytes(bytes[..4].try_into().unwrap());
                let val = (encoded ^ 0x8000_0000) as i32;
                (ScalarValue::Int32(Some(val)), 5)
            } else {
                (ScalarValue::Int32(None), 1)
            }
        }
        DataType::Int64 => {
            if bytes.len() >= 8 {
                let encoded = u64::from_be_bytes(bytes[..8].try_into().unwrap());
                let val = (encoded ^ 0x8000_0000_0000_0000) as i64;
                (ScalarValue::Int64(Some(val)), 9)
            } else {
                (ScalarValue::Int64(None), 1)
            }
        }
        DataType::UInt8 => {
            let val = bytes.get(0).copied();
            (ScalarValue::UInt8(val), 2)
        }
        DataType::UInt16 => {
            if bytes.len() >= 2 {
                let val = u16::from_be_bytes(bytes[..2].try_into().unwrap());
                (ScalarValue::UInt16(Some(val)), 3)
            } else {
                (ScalarValue::UInt16(None), 1)
            }
        }
        DataType::UInt32 => {
            if bytes.len() >= 4 {
                let val = u32::from_be_bytes(bytes[..4].try_into().unwrap());
                (ScalarValue::UInt32(Some(val)), 5)
            } else {
                (ScalarValue::UInt32(None), 1)
            }
        }
        DataType::UInt64 => {
            if bytes.len() >= 8 {
                let val = u64::from_be_bytes(bytes[..8].try_into().unwrap());
                (ScalarValue::UInt64(Some(val)), 9)
            } else {
                (ScalarValue::UInt64(None), 1)
            }
        }
        DataType::Utf8 => {
            let (s, len) = decode_escaped_string(bytes);
            (ScalarValue::Utf8(Some(s)), 1 + len)
        }
        DataType::LargeUtf8 => {
            let (s, len) = decode_escaped_string(bytes);
            (ScalarValue::LargeUtf8(Some(s)), 1 + len)
        }
        DataType::Binary => {
            let (b, len) = decode_escaped_binary(bytes);
            (ScalarValue::Binary(Some(b)), 1 + len)
        }
        DataType::LargeBinary => {
            let (b, len) = decode_escaped_binary(bytes);
            (ScalarValue::LargeBinary(Some(b)), 1 + len)
        }
        _ => {
            // Unknown type - try to decode as string (fallback encoding)
            let (s, len) = decode_escaped_string(bytes);
            (ScalarValue::Utf8(Some(s)), 1 + len)
        }
    }
}

/// Returns a null ScalarValue for the given DataType.
fn null_for_type(data_type: &DataType) -> ScalarValue {
    match data_type {
        DataType::Boolean => ScalarValue::Boolean(None),
        DataType::Int8 => ScalarValue::Int8(None),
        DataType::Int16 => ScalarValue::Int16(None),
        DataType::Int32 => ScalarValue::Int32(None),
        DataType::Int64 => ScalarValue::Int64(None),
        DataType::UInt8 => ScalarValue::UInt8(None),
        DataType::UInt16 => ScalarValue::UInt16(None),
        DataType::UInt32 => ScalarValue::UInt32(None),
        DataType::UInt64 => ScalarValue::UInt64(None),
        DataType::Utf8 => ScalarValue::Utf8(None),
        DataType::LargeUtf8 => ScalarValue::LargeUtf8(None),
        DataType::Binary => ScalarValue::Binary(None),
        DataType::LargeBinary => ScalarValue::LargeBinary(None),
        DataType::Float32 => ScalarValue::Float32(None),
        DataType::Float64 => ScalarValue::Float64(None),
        _ => ScalarValue::Null,
    }
}

/// Decodes an escaped string and returns (string, bytes_consumed including terminator).
fn decode_escaped_string(bytes: &[u8]) -> (String, usize) {
    let mut result = Vec::new();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == 0x00 {
            if i + 1 < bytes.len() && bytes[i + 1] == 0xFF {
                // Escaped null byte
                result.push(0x00);
                i += 2;
            } else {
                // Terminator (0x00 0x00 or end)
                i += 2; // Skip terminator
                break;
            }
        } else {
            result.push(bytes[i]);
            i += 1;
        }
    }

    (String::from_utf8_lossy(&result).to_string(), i)
}

/// Decodes escaped binary and returns (bytes, bytes_consumed including terminator).
fn decode_escaped_binary(bytes: &[u8]) -> (Vec<u8>, usize) {
    let mut result = Vec::new();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == 0x00 {
            if i + 1 < bytes.len() && bytes[i + 1] == 0xFF {
                // Escaped null byte
                result.push(0x00);
                i += 2;
            } else {
                // Terminator
                i += 2;
                break;
            }
        } else {
            result.push(bytes[i]);
            i += 1;
        }
    }

    (result, i)
}

/// Encodes a ScalarValue in an order-preserving manner.
/// The encoding ensures that memcmp ordering matches logical ordering.
fn encode_scalar_ordered(value: &ScalarValue, buf: &mut Vec<u8>) {
    match value {
        ScalarValue::Null => {
            // NULL sorts first (0x00)
            buf.push(0x00);
        }
        ScalarValue::Boolean(None) => buf.push(0x00),
        ScalarValue::Boolean(Some(false)) => {
            buf.push(0x01); // Non-null marker
            buf.push(0x00);
        }
        ScalarValue::Boolean(Some(true)) => {
            buf.push(0x01);
            buf.push(0x01);
        }
        ScalarValue::Int8(None) => buf.push(0x00),
        ScalarValue::Int8(Some(v)) => {
            buf.push(0x01);
            // Flip sign bit for correct ordering
            buf.push((*v as u8) ^ 0x80);
        }
        ScalarValue::Int16(None) => buf.push(0x00),
        ScalarValue::Int16(Some(v)) => {
            buf.push(0x01);
            let encoded = (*v as u16) ^ 0x8000;
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        ScalarValue::Int32(None) => buf.push(0x00),
        ScalarValue::Int32(Some(v)) => {
            buf.push(0x01);
            let encoded = (*v as u32) ^ 0x8000_0000;
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        ScalarValue::Int64(None) => buf.push(0x00),
        ScalarValue::Int64(Some(v)) => {
            buf.push(0x01);
            let encoded = (*v as u64) ^ 0x8000_0000_0000_0000;
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        ScalarValue::UInt8(None) => buf.push(0x00),
        ScalarValue::UInt8(Some(v)) => {
            buf.push(0x01);
            buf.push(*v);
        }
        ScalarValue::UInt16(None) => buf.push(0x00),
        ScalarValue::UInt16(Some(v)) => {
            buf.push(0x01);
            buf.extend_from_slice(&v.to_be_bytes());
        }
        ScalarValue::UInt32(None) => buf.push(0x00),
        ScalarValue::UInt32(Some(v)) => {
            buf.push(0x01);
            buf.extend_from_slice(&v.to_be_bytes());
        }
        ScalarValue::UInt64(None) => buf.push(0x00),
        ScalarValue::UInt64(Some(v)) => {
            buf.push(0x01);
            buf.extend_from_slice(&v.to_be_bytes());
        }
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => buf.push(0x00),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            buf.push(0x01);
            // Escape null bytes: 0x00 -> 0x00 0xFF, then terminate with 0x00 0x00
            for byte in s.as_bytes() {
                if *byte == 0x00 {
                    buf.push(0x00);
                    buf.push(0xFF);
                } else {
                    buf.push(*byte);
                }
            }
            buf.push(0x00);
            buf.push(0x00); // Terminator
        }
        ScalarValue::Binary(None) | ScalarValue::LargeBinary(None) => buf.push(0x00),
        ScalarValue::Binary(Some(b)) | ScalarValue::LargeBinary(Some(b)) => {
            buf.push(0x01);
            // Same escaping as strings
            for byte in b.iter() {
                if *byte == 0x00 {
                    buf.push(0x00);
                    buf.push(0xFF);
                } else {
                    buf.push(*byte);
                }
            }
            buf.push(0x00);
            buf.push(0x00);
        }
        // For other types, fall back to a less efficient but working encoding
        other => {
            buf.push(0x01);
            // Use debug string representation (not ideal but functional)
            let s = format!("{:?}", other);
            for byte in s.as_bytes() {
                if *byte == 0x00 {
                    buf.push(0x00);
                    buf.push(0xFF);
                } else {
                    buf.push(*byte);
                }
            }
            buf.push(0x00);
            buf.push(0x00);
        }
    }
}

/// Converts a ScalarValue to an rmpv::Value for MessagePack encoding.
fn scalar_to_msgpack(value: &ScalarValue) -> Value {
    match value {
        ScalarValue::Null => Value::Nil,
        ScalarValue::Boolean(None) => Value::Nil,
        ScalarValue::Boolean(Some(v)) => Value::Boolean(*v),
        ScalarValue::Int8(None) => Value::Nil,
        ScalarValue::Int8(Some(v)) => Value::Integer((*v as i64).into()),
        ScalarValue::Int16(None) => Value::Nil,
        ScalarValue::Int16(Some(v)) => Value::Integer((*v as i64).into()),
        ScalarValue::Int32(None) => Value::Nil,
        ScalarValue::Int32(Some(v)) => Value::Integer((*v as i64).into()),
        ScalarValue::Int64(None) => Value::Nil,
        ScalarValue::Int64(Some(v)) => Value::Integer((*v).into()),
        ScalarValue::UInt8(None) => Value::Nil,
        ScalarValue::UInt8(Some(v)) => Value::Integer((*v as u64).into()),
        ScalarValue::UInt16(None) => Value::Nil,
        ScalarValue::UInt16(Some(v)) => Value::Integer((*v as u64).into()),
        ScalarValue::UInt32(None) => Value::Nil,
        ScalarValue::UInt32(Some(v)) => Value::Integer((*v as u64).into()),
        ScalarValue::UInt64(None) => Value::Nil,
        ScalarValue::UInt64(Some(v)) => Value::Integer((*v).into()),
        ScalarValue::Float32(None) => Value::Nil,
        ScalarValue::Float32(Some(v)) => Value::F32(*v),
        ScalarValue::Float64(None) => Value::Nil,
        ScalarValue::Float64(Some(v)) => Value::F64(*v),
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => Value::Nil,
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            Value::String(s.clone().into())
        }
        ScalarValue::Binary(None) | ScalarValue::LargeBinary(None) => Value::Nil,
        ScalarValue::Binary(Some(b)) | ScalarValue::LargeBinary(Some(b)) => {
            Value::Binary(b.clone())
        }
        // For unsupported types, convert to string representation
        other => Value::String(format!("{:?}", other).into()),
    }
}

/// Converts an rmpv::Value back to a ScalarValue given the expected DataType.
fn msgpack_to_scalar(value: &Value, data_type: &DataType) -> ScalarValue {
    match (value, data_type) {
        (Value::Nil, DataType::Boolean) => ScalarValue::Boolean(None),
        (Value::Nil, DataType::Int8) => ScalarValue::Int8(None),
        (Value::Nil, DataType::Int16) => ScalarValue::Int16(None),
        (Value::Nil, DataType::Int32) => ScalarValue::Int32(None),
        (Value::Nil, DataType::Int64) => ScalarValue::Int64(None),
        (Value::Nil, DataType::UInt8) => ScalarValue::UInt8(None),
        (Value::Nil, DataType::UInt16) => ScalarValue::UInt16(None),
        (Value::Nil, DataType::UInt32) => ScalarValue::UInt32(None),
        (Value::Nil, DataType::UInt64) => ScalarValue::UInt64(None),
        (Value::Nil, DataType::Float32) => ScalarValue::Float32(None),
        (Value::Nil, DataType::Float64) => ScalarValue::Float64(None),
        (Value::Nil, DataType::Utf8) => ScalarValue::Utf8(None),
        (Value::Nil, DataType::LargeUtf8) => ScalarValue::LargeUtf8(None),
        (Value::Nil, DataType::Binary) => ScalarValue::Binary(None),
        (Value::Nil, DataType::LargeBinary) => ScalarValue::LargeBinary(None),
        (Value::Nil, _) => ScalarValue::Null,

        (Value::Boolean(v), _) => ScalarValue::Boolean(Some(*v)),

        (Value::Integer(i), DataType::Int8) => ScalarValue::Int8(i.as_i64().map(|v| v as i8)),
        (Value::Integer(i), DataType::Int16) => ScalarValue::Int16(i.as_i64().map(|v| v as i16)),
        (Value::Integer(i), DataType::Int32) => ScalarValue::Int32(i.as_i64().map(|v| v as i32)),
        (Value::Integer(i), DataType::Int64) => ScalarValue::Int64(i.as_i64()),
        (Value::Integer(i), DataType::UInt8) => ScalarValue::UInt8(i.as_u64().map(|v| v as u8)),
        (Value::Integer(i), DataType::UInt16) => ScalarValue::UInt16(i.as_u64().map(|v| v as u16)),
        (Value::Integer(i), DataType::UInt32) => ScalarValue::UInt32(i.as_u64().map(|v| v as u32)),
        (Value::Integer(i), DataType::UInt64) => ScalarValue::UInt64(i.as_u64()),
        (Value::Integer(i), _) => ScalarValue::Int64(i.as_i64()),

        (Value::F32(f), _) => ScalarValue::Float32(Some(*f)),
        (Value::F64(f), _) => ScalarValue::Float64(Some(*f)),

        (Value::String(s), DataType::LargeUtf8) => {
            ScalarValue::LargeUtf8(s.as_str().map(|s| s.to_string()))
        }
        (Value::String(s), _) => ScalarValue::Utf8(s.as_str().map(|s| s.to_string())),

        (Value::Binary(b), DataType::LargeBinary) => ScalarValue::LargeBinary(Some(b.clone())),
        (Value::Binary(b), _) => ScalarValue::Binary(Some(b.clone())),

        // Fallback for unexpected combinations
        _ => ScalarValue::Null,
    }
}

/// Encodes a row of values using MessagePack.
pub fn encode_row(values: &[ScalarValue]) -> Vec<u8> {
    let msgpack_values: Vec<Value> = values.iter().map(scalar_to_msgpack).collect();
    let array = Value::Array(msgpack_values);
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &array).expect("MessagePack encoding failed");
    buf
}

/// Decodes a row of values from MessagePack bytes.
pub fn decode_row(bytes: &[u8], schema: &Schema) -> Vec<ScalarValue> {
    let value = rmpv::decode::read_value(&mut &bytes[..]).expect("MessagePack decoding failed");

    match value {
        Value::Array(values) => values
            .iter()
            .zip(schema.fields())
            .map(|(v, field)| msgpack_to_scalar(v, field.data_type()))
            .collect(),
        _ => panic!("Expected array in MessagePack data"),
    }
}

/// Decodes only specific columns from a row.
/// More efficient when only a subset of columns is needed.
pub fn decode_row_projected(bytes: &[u8], schema: &Schema, indices: &[usize]) -> Vec<ScalarValue> {
    let value = rmpv::decode::read_value(&mut &bytes[..]).expect("MessagePack decoding failed");

    match value {
        Value::Array(values) => indices
            .iter()
            .map(|&i| {
                let field = schema.field(i);
                msgpack_to_scalar(&values[i], field.data_type())
            })
            .collect(),
        _ => panic!("Expected array in MessagePack data"),
    }
}

/// Formats an rmpv::Value for debug display.
pub fn format_msgpack_value(v: &Value) -> String {
    match v {
        Value::Nil => "NULL".to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Integer(i) => {
            if let Some(n) = i.as_i64() {
                n.to_string()
            } else if let Some(n) = i.as_u64() {
                n.to_string()
            } else {
                format!("{:?}", i)
            }
        }
        Value::F32(f) => format!("{:.2}", f),
        Value::F64(f) => format!("{:.2}", f),
        Value::String(s) => format!("\"{}\"", s.as_str().unwrap_or("<invalid utf8>")),
        Value::Binary(b) => format!("0x{}", b.iter().map(|x| format!("{:02x}", x)).collect::<String>()),
        Value::Array(arr) => format!("[{}]", arr.iter().map(format_msgpack_value).collect::<Vec<_>>().join(", ")),
        Value::Map(m) => format!("{{{}}}", m.iter().map(|(k, v)| format!("{}: {}", format_msgpack_value(k), format_msgpack_value(v))).collect::<Vec<_>>().join(", ")),
        Value::Ext(t, b) => format!("Ext({}, {:?})", t, b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;

    #[test]
    fn test_key_encoding_ordering() {
        // Test that encoded keys maintain correct ordering
        let key1 = encode_key(1, &[ScalarValue::Int64(Some(100))]);
        let key2 = encode_key(1, &[ScalarValue::Int64(Some(200))]);
        let key3 = encode_key(1, &[ScalarValue::Int64(Some(-50))]);

        assert!(key3 < key1, "negative should be less than positive");
        assert!(key1 < key2, "100 should be less than 200");
    }

    #[test]
    fn test_string_key_encoding_ordering() {
        let key1 = encode_key(1, &[ScalarValue::Utf8(Some("apple".to_string()))]);
        let key2 = encode_key(1, &[ScalarValue::Utf8(Some("banana".to_string()))]);
        let key3 = encode_key(1, &[ScalarValue::Utf8(Some("aardvark".to_string()))]);

        assert!(key3 < key1, "aardvark < apple");
        assert!(key1 < key2, "apple < banana");
    }

    #[test]
    fn test_null_ordering() {
        let key_null = encode_key(1, &[ScalarValue::Int64(None)]);
        let key_value = encode_key(1, &[ScalarValue::Int64(Some(0))]);

        assert!(key_null < key_value, "NULL should sort before any value");
    }

    #[test]
    fn test_composite_key_encoding() {
        let key1 = encode_key(
            1,
            &[
                ScalarValue::Utf8(Some("user1".to_string())),
                ScalarValue::Int64(Some(100)),
            ],
        );
        let key2 = encode_key(
            1,
            &[
                ScalarValue::Utf8(Some("user1".to_string())),
                ScalarValue::Int64(Some(200)),
            ],
        );
        let key3 = encode_key(
            1,
            &[
                ScalarValue::Utf8(Some("user2".to_string())),
                ScalarValue::Int64(Some(50)),
            ],
        );

        assert!(key1 < key2, "same user, 100 < 200");
        assert!(key2 < key3, "user1 < user2");
    }

    #[test]
    fn test_row_encoding_roundtrip() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]);

        let values = vec![
            ScalarValue::Int64(Some(42)),
            ScalarValue::Utf8(Some("test".to_string())),
            ScalarValue::Float64(Some(3.14)),
        ];

        let encoded = encode_row(&values);
        let decoded = decode_row(&encoded, &schema);

        assert_eq!(values, decoded);
    }

    #[test]
    fn test_row_encoding_with_nulls() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let values = vec![ScalarValue::Int64(Some(1)), ScalarValue::Utf8(None)];

        let encoded = encode_row(&values);
        let decoded = decode_row(&encoded, &schema);

        assert_eq!(values, decoded);
    }

    #[test]
    fn test_projected_decode() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true),
        ]);

        let values = vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Utf8(Some("hello".to_string())),
            ScalarValue::Float64(Some(2.5)),
        ];

        let encoded = encode_row(&values);
        let projected = decode_row_projected(&encoded, &schema, &[0, 2]);

        assert_eq!(
            projected,
            vec![ScalarValue::Int64(Some(1)), ScalarValue::Float64(Some(2.5)),]
        );
    }
}
