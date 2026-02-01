"""Unit tests for SQL injection prevention."""

import pytest
from dbadmin.sanitize import (
    detect_sql_injection,
    validate_sql_identifier,
    sanitize_order_direction,
    sanitize_limit,
)


class TestDetectSqlInjection:
    """Tests for SQL injection detection."""
    
    def test_safe_input(self):
        """Test safe input is not flagged."""
        assert detect_sql_injection("users") is False
        assert detect_sql_injection("SELECT * FROM users") is False
        assert detect_sql_injection("john.doe@email.com") is False
    
    def test_multi_statement_injection(self):
        """Test detection of multi-statement attacks."""
        assert detect_sql_injection("; DROP TABLE users") is True
        assert detect_sql_injection("1; DELETE FROM accounts") is True
    
    def test_union_injection(self):
        """Test detection of UNION-based injection."""
        assert detect_sql_injection("UNION SELECT password FROM users") is True
        assert detect_sql_injection("1 UNION ALL SELECT * FROM secrets") is True
    
    def test_comment_injection(self):
        """Test detection of SQL comments."""
        assert detect_sql_injection("admin'--") is True
        assert detect_sql_injection("admin/*comment*/") is True
    
    def test_boolean_injection(self):
        """Test detection of boolean-based injection."""
        assert detect_sql_injection("OR 1=1") is True
        assert detect_sql_injection("AND '1'='1'") is True
    
    def test_timing_attacks(self):
        """Test detection of timing-based attacks."""
        assert detect_sql_injection("SLEEP(5)") is True
        assert detect_sql_injection("BENCHMARK(1000000,SHA1('test'))") is True
        assert detect_sql_injection("pg_sleep(10)") is True


class TestValidateSqlIdentifier:
    """Tests for SQL identifier validation."""
    
    def test_valid_identifiers(self):
        """Test valid identifiers pass."""
        assert validate_sql_identifier("users") == "users"
        assert validate_sql_identifier("user_accounts") == "user_accounts"
        assert validate_sql_identifier("_private") == "_private"
        assert validate_sql_identifier("Table123") == "Table123"
    
    def test_empty_identifier(self):
        """Test empty identifier raises."""
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_sql_identifier("")
    
    def test_invalid_characters(self):
        """Test invalid characters raise."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_sql_identifier("users; DROP")
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_sql_identifier("table-name")
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_sql_identifier("table name")
    
    def test_length_limits(self):
        """Test identifier length limits."""
        long_name = "a" * 64  # Too long for PostgreSQL (63 max)
        with pytest.raises(ValueError, match="too long"):
            validate_sql_identifier(long_name, db_type="postgresql")
    
    def test_reserved_words(self):
        """Test reserved words are rejected."""
        with pytest.raises(ValueError, match="reserved word"):
            validate_sql_identifier("null")
        with pytest.raises(ValueError, match="reserved word"):
            validate_sql_identifier("TRUE")


class TestSanitizeOrderDirection:
    """Tests for ORDER BY direction sanitization."""
    
    def test_valid_directions(self):
        """Test valid directions are accepted."""
        assert sanitize_order_direction("ASC") == "ASC"
        assert sanitize_order_direction("DESC") == "DESC"
        assert sanitize_order_direction("asc") == "ASC"
        assert sanitize_order_direction("  desc  ") == "DESC"
    
    def test_invalid_direction(self):
        """Test invalid direction raises."""
        with pytest.raises(ValueError, match="Invalid sort direction"):
            sanitize_order_direction("ASC; DROP TABLE")


class TestSanitizeLimit:
    """Tests for LIMIT value sanitization."""
    
    def test_valid_limits(self):
        """Test valid limits are accepted."""
        assert sanitize_limit(10) == 10
        assert sanitize_limit("100") == 100
        assert sanitize_limit(0) == 0
    
    def test_negative_limit(self):
        """Test negative limit raises."""
        with pytest.raises(ValueError, match="cannot be negative"):
            sanitize_limit(-1)
    
    def test_excessive_limit(self):
        """Test excessive limit raises."""
        with pytest.raises(ValueError, match="too large"):
            sanitize_limit(100000, max_limit=1000)
    
    def test_invalid_value(self):
        """Test non-numeric value raises."""
        with pytest.raises(ValueError, match="Invalid limit"):
            sanitize_limit("abc")
