use super::super::*;

const VALID_HEX: &str = "0a1b2c3d4e5f67890123456789abcdefabcdef12";

const VALID_BYTES: [u8; 20] = [
    0x0a, 0x1b, 0x2c, 0x3d, 0x4e, 0x5f, 0x67, 0x89, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
    0xab, 0xcd, 0xef, 0x12,
];

#[test]
fn hash_to_hex() {
    // Arrange
    // Act
    let hash = Hash::new(VALID_BYTES);

    // Assert
    assert_eq!(hash.to_hex(), VALID_HEX);
}

#[test]
fn hash_from_string() {
    // Arrange
    // Act
    let hash = Hash::from_string(VALID_HEX).unwrap();

    // Assert
    assert_eq!(hash.as_bytes(), &VALID_BYTES);
}

#[test]
fn hash_from_string_invalid_length() {
    // Arrange
    let invalid_hex_length = "12345";

    // Act
    let result = Hash::<20>::from_string(invalid_hex_length);

    // Assert
    assert!(result.is_err());
}

#[test]
fn hash_from_string_invalid_chars() {
    // Arrange
    let invalid_hex_chars = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";

    // Act
    let result = Hash::<20>::from_string(invalid_hex_chars);

    // Assert
    assert!(result.is_err());
}
