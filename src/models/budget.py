"""Budget data models."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Optional, Set

@dataclass
class ValidationResult:
    """Validation result for a budget item."""
    is_valid: bool = True
    messages: List[str] = field(default_factory=list)

@dataclass
class BudgetLineItem:
    """Single budget line item with estimates and actuals"""
    number: int
    description: str
    estimate_days: Optional[float]
    estimate_rate: Optional[float]
    actual_days: Optional[float]
    actual_rate: Optional[float]
    estimate_ot_rate: Optional[float] = None
    estimate_ot_hours: Optional[float] = None
    validation: ValidationResult = field(default_factory=ValidationResult)

    def __post_init__(self):
        self._validate()

    def _validate(self):
        """Validate the line item."""
        if not self.description:
            self.validation.is_valid = False
            self.validation.messages.append("Missing description")
        
        if self.estimate_rate and not self.estimate_days:
            self.validation.messages.append("Has rate but missing days")
        
        if self.actual_rate and not self.actual_days:
            self.validation.messages.append("Has actual rate but missing days")
            
        # OT validation
        if self.estimate_ot_rate is not None:
            if self.estimate_ot_rate > 0 and not self.estimate_ot_hours:
                self.validation.messages.append("Has OT rate but missing OT hours")
            elif self.estimate_ot_hours and not self.estimate_ot_rate:
                self.validation.messages.append("Has OT hours but missing OT rate")

    @property
    def estimate_total(self) -> float:
        """Calculate estimate total following AICP rules"""
        total = 0.0
        
        # Regular hours
        if self.estimate_rate and self.estimate_days:
            total += self.estimate_days * self.estimate_rate
        
        # OT hours if applicable
        if self.estimate_ot_rate and self.estimate_ot_hours:
            total += self.estimate_ot_rate * self.estimate_ot_hours
            
        return total

    @property
    def actual_total(self) -> float:
        """Calculate actual total following AICP rules"""
        if not self.actual_rate:
            return 0.0
        if not self.actual_days:
            return 0.0
        return self.actual_days * self.actual_rate

    @property
    def has_actuals(self) -> bool:
        """Check if line item has any actual values."""
        return any([self.actual_days, self.actual_rate])

@dataclass
class BudgetClass:
    """Budget class (e.g., A: PREPRODUCTION & WRAP CREW)"""
    code: str
    name: str
    pnw_percent: float = 28.0
    line_items: List[BudgetLineItem] = None
    validation: ValidationResult = field(default_factory=ValidationResult)

    def __post_init__(self):
        self.line_items = self.line_items or []
        self._validate()

    def _validate(self):
        """Validate the budget class."""
        if not self.code or not self.name:
            self.validation.is_valid = False
            self.validation.messages.append("Missing code or name")
        
        if not self.line_items:
            self.validation.messages.append("No line items found")

    @property
    def estimate_subtotal(self) -> float:
        return sum(item.estimate_total for item in self.line_items)

    @property
    def actual_subtotal(self) -> float:
        return sum(item.actual_total for item in self.line_items)

    @property
    def estimate_pnw(self) -> float:
        return self.estimate_subtotal * (self.pnw_percent / 100)

    @property
    def actual_pnw(self) -> float:
        return self.actual_subtotal * (self.pnw_percent / 100)

    @property
    def estimate_total(self) -> float:
        return self.estimate_subtotal + self.estimate_pnw

    @property
    def actual_total(self) -> float:
        return self.actual_subtotal + self.actual_pnw

    @property
    def has_actuals(self) -> bool:
        """Check if any line items have actuals."""
        return any(item.has_actuals for item in self.line_items)

    @property
    def has_missing_days(self) -> bool:
        """Check if any line items have rates but missing days."""
        return any(
            item.estimate_rate and not item.estimate_days
            for item in self.line_items
        )

@dataclass
class Budget:
    """Complete AICP budget"""
    upload_id: str
    budget_name: str
    version_status: str
    upload_timestamp: datetime
    classes: Dict[str, BudgetClass]
    validation: ValidationResult = field(default_factory=ValidationResult)

    def __post_init__(self):
        self._validate()

    def _validate(self):
        """Validate the entire budget."""
        # Initialize validation state
        self.validation.is_valid = True
        self.validation.messages = []
        
        # Check for classes
        if not self.classes:
            self.validation.messages.append("No budget classes found")
            self.validation.is_valid = False
            return
        
        # Validate each class
        class_summaries = {}
        for code, budget_class in self.classes.items():
            class_summary = self._validate_class(budget_class)
            class_summaries[code] = class_summary
            
            # Add class validation messages
            if class_summary['messages']:
                self.validation.messages.extend(
                    f"Class {code}: {msg}" for msg in class_summary['messages']
                )
            
            # Update overall validation state
            if not class_summary['is_valid']:
                self.validation.is_valid = False

    def _validate_class(self, budget_class: BudgetClass) -> Dict:
        """Validate a single budget class and return summary."""
        summary = {
            'is_valid': True,
            'messages': [],
            'total_items': len(budget_class.line_items),
            'items_with_rates': sum(1 for item in budget_class.line_items if item.estimate_rate is not None),
            'items_with_days': sum(1 for item in budget_class.line_items if item.estimate_days is not None),
            'items_complete': sum(1 for item in budget_class.line_items if item.estimate_total > 0),
            'has_actuals': budget_class.has_actuals,
            # Add totals to summary
            'estimate_subtotal': budget_class.estimate_subtotal,
            'estimate_pnw': budget_class.estimate_pnw,
            'estimate_total': budget_class.estimate_total,
            'actual_subtotal': budget_class.actual_subtotal,
            'actual_pnw': budget_class.actual_pnw,
            'actual_total': budget_class.actual_total
        }
        
        # Validate class structure
        if not budget_class.code or not budget_class.name:
            summary['is_valid'] = False
            summary['messages'].append("Missing code or name")
        
        # Validate line items
        if not budget_class.line_items:
            summary['messages'].append("No line items found")
        else:
            # Check for incomplete items
            incomplete_items = [
                item for item in budget_class.line_items
                if item.estimate_rate and not item.estimate_days
            ]
            if incomplete_items:
                summary['messages'].append(
                    f"{len(incomplete_items)} items have rates but missing days"
                )
            
            # Additional validation for Class B OT fields
            if budget_class.code == 'B':
                ot_issues = []
                for item in budget_class.line_items:
                    if item.estimate_ot_rate and not item.estimate_ot_hours:
                        ot_issues.append("rate without hours")
                    elif item.estimate_ot_hours and not item.estimate_ot_rate:
                        ot_issues.append("hours without rate")
                
                if ot_issues:
                    summary['messages'].append(
                        f"{len(ot_issues)} items have OT {', '.join(set(ot_issues))}"
                    )
        
        return summary

    @property
    def validation_summary(self) -> Dict:
        """Get enhanced validation summary for the budget."""
        return {
            'is_valid': self.validation.is_valid,
            'missing_days': any(
                budget_class.has_missing_days
                for budget_class in self.classes.values()
            ),
            'has_actuals': self.has_actuals,
            'messages': self.validation.messages,
            'class_summaries': {
                code: self._validate_class(budget_class)
                for code, budget_class in self.classes.items()
            }
        }

    @property
    def total_estimate(self) -> float:
        return sum(budget_class.estimate_total for budget_class in self.classes.values())

    @property
    def total_actual(self) -> float:
        return sum(budget_class.actual_total for budget_class in self.classes.values())

    @property
    def processed_class_codes(self) -> List[str]:
        """Get list of processed class codes."""
        return sorted(self.classes.keys())

    @property
    def total_line_items(self) -> int:
        """Get total number of line items across all classes."""
        return sum(len(budget_class.line_items) for budget_class in self.classes.values())

    @property
    def has_actuals(self) -> bool:
        """Check if any classes have actuals."""
        return any(budget_class.has_actuals for budget_class in self.classes.values())

    def to_bigquery_rows(self) -> List[Dict]:
        """Convert budget to BigQuery row format"""
        rows = []
        for budget_class in self.classes.values():
            for line_item in budget_class.line_items:
                row = {
                    'upload_id': self.upload_id,
                    'budget_name': self.budget_name,
                    'upload_timestamp': self.upload_timestamp.isoformat(),
                    'class_code': budget_class.code,
                    'class_name': budget_class.name,
                    'line_item_number': line_item.number,
                    'line_item_description': line_item.description,
                    'estimate_days': line_item.estimate_days,
                    'estimate_rate': line_item.estimate_rate,
                    'estimate_total': line_item.estimate_total,
                    'actual_days': line_item.actual_days,
                    'actual_rate': line_item.actual_rate,
                    'actual_total': line_item.actual_total,
                    # Class totals (same for all line items in class)
                    'class_estimate_subtotal': budget_class.estimate_subtotal,
                    'class_estimate_pnw': budget_class.estimate_pnw,
                    'class_estimate_total': budget_class.estimate_total,
                    'class_actual_subtotal': budget_class.actual_subtotal,
                    'class_actual_pnw': budget_class.actual_pnw,
                    'class_actual_total': budget_class.actual_total,
                    # Validation
                    'validation_status': 'valid' if line_item.validation.is_valid else 'invalid',
                    'validation_messages': line_item.validation.messages
                }
                
                # Add OT fields if present (for Class B)
                if budget_class.code == 'B':
                    row.update({
                        'estimate_ot_rate': line_item.estimate_ot_rate,
                        'estimate_ot_hours': line_item.estimate_ot_hours,
                        'estimate_ot_total': (line_item.estimate_ot_rate or 0) * (line_item.estimate_ot_hours or 0)
                    })
                
                rows.append(row)
        return rows 