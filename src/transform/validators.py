"""
Data Validators Module
======================

Validates data quality with configurable rules and thresholds.
Generates quality reports for monitoring and alerting.

Implements:
- Null percentage checks
- Schema validation
- Business rule validation
- Outlier detection
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

import pandas as pd
import numpy as np

from ..utils.config import config
from ..utils.logger import ETLLogger


class ValidationSeverity(Enum):
    """Severity levels for validation failures."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ValidationResult:
    """Result of a single validation check."""
    rule_name: str
    passed: bool
    severity: ValidationSeverity
    message: str
    details: dict = field(default_factory=dict)


@dataclass
class ValidationReport:
    """Complete validation report for a DataFrame."""
    table_name: str
    row_count: int
    results: list[ValidationResult] = field(default_factory=list)
    
    @property
    def passed(self) -> bool:
        """Check if all validations passed."""
        return all(r.passed for r in self.results)
    
    @property
    def has_critical_failures(self) -> bool:
        """Check if any critical validations failed."""
        return any(
            not r.passed and r.severity == ValidationSeverity.CRITICAL
            for r in self.results
        )
    
    @property
    def error_count(self) -> int:
        """Count of failed validations."""
        return sum(1 for r in self.results if not r.passed)
    
    def to_dict(self) -> dict:
        """Convert report to dictionary format."""
        return {
            "table_name": self.table_name,
            "row_count": self.row_count,
            "passed": self.passed,
            "error_count": self.error_count,
            "has_critical_failures": self.has_critical_failures,
            "results": [
                {
                    "rule": r.rule_name,
                    "passed": r.passed,
                    "severity": r.severity.value,
                    "message": r.message,
                    "details": r.details
                }
                for r in self.results
            ]
        }


class DataValidator:
    """
    Configurable data quality validator.
    
    Runs a suite of validation checks on DataFrames and generates
    detailed reports for monitoring data quality.
    
    Similar to how fleet management validates GPS data quality,
    delivery timestamps, and trip consistency.
    
    Example:
        validator = DataValidator("orders")
        validator.add_null_check("order_id", max_null_pct=0.0)
        validator.add_schema_check({"order_id": "object", "price": "float64"})
        report = validator.validate(df)
    """
    
    def __init__(self, table_name: str):
        """
        Initialize validator for a specific table.
        
        Args:
            table_name: Name of the table being validated
        """
        self.table_name = table_name
        self.logger = ETLLogger("transform")
        self._checks: list[Callable[[pd.DataFrame], ValidationResult]] = []
        
        # Load config thresholds
        self.null_threshold = config.transform.get("null_threshold", 0.3)
    
    def add_null_check(
        self,
        column: str,
        max_null_pct: Optional[float] = None,
        severity: ValidationSeverity = ValidationSeverity.ERROR
    ) -> "DataValidator":
        """
        Add a null percentage check for a column.
        
        Args:
            column: Column name to check
            max_null_pct: Maximum allowed null percentage (0.0 to 1.0)
            severity: Severity level if check fails
            
        Returns:
            self for method chaining
        """
        threshold = max_null_pct if max_null_pct is not None else self.null_threshold
        
        def check(df: pd.DataFrame) -> ValidationResult:
            if column not in df.columns:
                return ValidationResult(
                    rule_name=f"null_check_{column}",
                    passed=False,
                    severity=severity,
                    message=f"Column '{column}' not found in DataFrame",
                    details={"column": column}
                )
            
            null_pct = df[column].isnull().mean()
            passed = null_pct <= threshold
            
            return ValidationResult(
                rule_name=f"null_check_{column}",
                passed=passed,
                severity=severity,
                message=(
                    f"Column '{column}' null check: {null_pct:.2%} null "
                    f"({'PASS' if passed else 'FAIL'}, threshold: {threshold:.2%})"
                ),
                details={
                    "column": column,
                    "null_percentage": round(null_pct, 4),
                    "threshold": threshold
                }
            )
        
        self._checks.append(check)
        return self
    
    def add_schema_check(
        self,
        expected_schema: dict[str, str],
        severity: ValidationSeverity = ValidationSeverity.ERROR
    ) -> "DataValidator":
        """
        Add a schema validation check.
        
        Args:
            expected_schema: Dict mapping column names to expected dtypes
            severity: Severity level if check fails
            
        Returns:
            self for method chaining
        """
        def check(df: pd.DataFrame) -> ValidationResult:
            mismatches = []
            
            for col, expected_dtype in expected_schema.items():
                if col not in df.columns:
                    mismatches.append(f"Missing column: {col}")
                elif str(df[col].dtype) != expected_dtype:
                    mismatches.append(
                        f"{col}: expected {expected_dtype}, got {df[col].dtype}"
                    )
            
            passed = len(mismatches) == 0
            
            return ValidationResult(
                rule_name="schema_check",
                passed=passed,
                severity=severity,
                message=(
                    "Schema validation passed" if passed 
                    else f"Schema mismatches: {mismatches}"
                ),
                details={"mismatches": mismatches}
            )
        
        self._checks.append(check)
        return self
    
    def add_unique_check(
        self,
        columns: list[str],
        severity: ValidationSeverity = ValidationSeverity.ERROR
    ) -> "DataValidator":
        """
        Add a uniqueness check for specified columns.
        
        Args:
            columns: Columns that should be unique together
            severity: Severity level if check fails
            
        Returns:
            self for method chaining
        """
        def check(df: pd.DataFrame) -> ValidationResult:
            existing_cols = [c for c in columns if c in df.columns]
            
            if not existing_cols:
                return ValidationResult(
                    rule_name=f"unique_check_{','.join(columns)}",
                    passed=False,
                    severity=severity,
                    message=f"None of the specified columns exist: {columns}",
                    details={"columns": columns}
                )
            
            duplicate_count = df.duplicated(subset=existing_cols).sum()
            passed = duplicate_count == 0
            
            return ValidationResult(
                rule_name=f"unique_check_{','.join(columns)}",
                passed=passed,
                severity=severity,
                message=(
                    f"Uniqueness check on {existing_cols}: "
                    f"{duplicate_count} duplicates found"
                ),
                details={
                    "columns": existing_cols,
                    "duplicate_count": int(duplicate_count)
                }
            )
        
        self._checks.append(check)
        return self
    
    def add_range_check(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        severity: ValidationSeverity = ValidationSeverity.WARNING
    ) -> "DataValidator":
        """
        Add a value range check for a numeric column.
        
        Args:
            column: Column name to check
            min_value: Minimum allowed value (inclusive)
            max_value: Maximum allowed value (inclusive)
            severity: Severity level if check fails
            
        Returns:
            self for method chaining
        """
        def check(df: pd.DataFrame) -> ValidationResult:
            if column not in df.columns:
                return ValidationResult(
                    rule_name=f"range_check_{column}",
                    passed=False,
                    severity=severity,
                    message=f"Column '{column}' not found",
                    details={"column": column}
                )
            
            violations = 0
            actual_min = df[column].min()
            actual_max = df[column].max()
            
            if min_value is not None:
                violations += (df[column] < min_value).sum()
            if max_value is not None:
                violations += (df[column] > max_value).sum()
            
            passed = violations == 0
            
            return ValidationResult(
                rule_name=f"range_check_{column}",
                passed=passed,
                severity=severity,
                message=(
                    f"Range check on '{column}': "
                    f"range [{actual_min}, {actual_max}], "
                    f"{violations} violations"
                ),
                details={
                    "column": column,
                    "expected_min": min_value,
                    "expected_max": max_value,
                    "actual_min": float(actual_min) if not pd.isna(actual_min) else None,
                    "actual_max": float(actual_max) if not pd.isna(actual_max) else None,
                    "violations": int(violations)
                }
            )
        
        self._checks.append(check)
        return self
    
    def add_business_rule(
        self,
        rule_name: str,
        condition: Callable[[pd.DataFrame], pd.Series],
        description: str,
        severity: ValidationSeverity = ValidationSeverity.WARNING
    ) -> "DataValidator":
        """
        Add a custom business rule validation.
        
        Args:
            rule_name: Name for the rule
            condition: Function that returns a boolean Series (True = valid)
            description: Human-readable description
            severity: Severity level if check fails
            
        Returns:
            self for method chaining
            
        Example:
            validator.add_business_rule(
                "delivery_after_order",
                lambda df: df["delivered_at"] >= df["ordered_at"],
                "Delivery date must be after order date"
            )
        """
        def check(df: pd.DataFrame) -> ValidationResult:
            try:
                valid_mask = condition(df)
                violations = (~valid_mask).sum()
                passed = violations == 0
                
                return ValidationResult(
                    rule_name=f"business_rule_{rule_name}",
                    passed=passed,
                    severity=severity,
                    message=f"{description}: {violations} violations",
                    details={
                        "rule": rule_name,
                        "description": description,
                        "violations": int(violations),
                        "violation_pct": round((violations / len(df)) * 100, 2)
                    }
                )
            except Exception as e:
                return ValidationResult(
                    rule_name=f"business_rule_{rule_name}",
                    passed=False,
                    severity=ValidationSeverity.ERROR,
                    message=f"Error evaluating rule: {e}",
                    details={"error": str(e)}
                )
        
        self._checks.append(check)
        return self
    
    def validate(self, df: pd.DataFrame) -> ValidationReport:
        """
        Run all validation checks on the DataFrame.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            ValidationReport with all check results
        """
        self.logger.info(
            f"Starting validation",
            table=self.table_name,
            checks=len(self._checks)
        )
        
        report = ValidationReport(
            table_name=self.table_name,
            row_count=len(df)
        )
        
        for check_func in self._checks:
            result = check_func(df)
            report.results.append(result)
            
            if not result.passed:
                log_method = (
                    self.logger.error if result.severity in [
                        ValidationSeverity.ERROR, 
                        ValidationSeverity.CRITICAL
                    ] else self.logger.warning
                )
                log_method(
                    f"Validation failed: {result.rule_name} - {result.message}"
                )
        
        self.logger.info(
            "Validation complete",
            table=self.table_name,
            passed=report.passed,
            errors=report.error_count
        )
        
        return report


def create_orders_validator() -> DataValidator:
    """Create a pre-configured validator for orders data."""
    return (
        DataValidator("orders")
        .add_null_check("order_id", max_null_pct=0.0, severity=ValidationSeverity.CRITICAL)
        .add_null_check("customer_id", max_null_pct=0.0)
        .add_null_check("order_purchase_timestamp", max_null_pct=0.0)
        .add_unique_check(["order_id"])
    )


def create_order_items_validator() -> DataValidator:
    """Create a pre-configured validator for order items data."""
    return (
        DataValidator("order_items")
        .add_null_check("order_id", max_null_pct=0.0, severity=ValidationSeverity.CRITICAL)
        .add_null_check("product_id", max_null_pct=0.0)
        .add_range_check("price", min_value=0.0)
        .add_range_check("freight_value", min_value=0.0)
    )
