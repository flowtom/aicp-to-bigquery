"""Budget data models."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

@dataclass
class ValidationResult:
    """Validation result with messages."""
    is_valid: bool = True
    messages: List[str] = field(default_factory=list)

@dataclass
class BudgetLineItem:
    """Single budget line item with estimates and actuals"""
    number: int
    description: str
    estimate_days: Optional[float]
    estimate_rate: Optional[float]
    estimate_total: Optional[float]
    actual_days: Optional[float]
    actual_rate: Optional[float]
    actual_total: Optional[float]
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

    def to_dict(self):
        """Convert BudgetLineItem to a dictionary for JSON serialization."""
        return {
            'number': self.number,
            'description': self.description,
            'estimate_days': self.estimate_days,
            'estimate_rate': self.estimate_rate,
            'estimate_total': self.estimate_total,
            'actual_days': self.actual_days,
            'actual_rate': self.actual_rate,
            'actual_total': self.actual_total,
            'estimate_ot_rate': self.estimate_ot_rate,
            'estimate_ot_hours': self.estimate_ot_hours,
            'validation': self.validation.__dict__ if self.validation else None
        }

    @property
    def has_actuals(self) -> bool:
        """Check if line item has any actual values."""
        return any([self.actual_days, self.actual_rate])

@dataclass
class BudgetClass:
    """A class of budget items (e.g. PREPRODUCTION & WRAP CREW)."""
    class_code: str
    class_name: str
    estimate_subtotal: float = 0.0
    estimate_pnw: float = 0.0
    estimate_total: float = 0.0
    actual_subtotal: float = 0.0
    actual_pnw: float = 0.0
    actual_total: float = 0.0
    line_items: List[BudgetLineItem] = field(default_factory=list)
    validation: ValidationResult = field(default_factory=ValidationResult)

    def __post_init__(self):
        self._validate()

    def _validate(self):
        """Validate the budget class."""
        if not self.class_code or not self.class_name:
            self.validation.is_valid = False
            self.validation.messages.append("Missing code or name")
        
        if not self.line_items:
            self.validation.messages.append("No line items found")

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

    def to_dict(self):
        """Convert BudgetClass object to a dictionary for JSON serialization."""
        return {
            'class_code': self.class_code,
            'class_name': self.class_name,
            'estimate_subtotal': self.estimate_subtotal,
            'estimate_pnw': self.estimate_pnw,
            'estimate_total': self.estimate_total,
            'actual_subtotal': self.actual_subtotal,
            'actual_pnw': self.actual_pnw,
            'actual_total': self.actual_total,
            'line_items': [
                item.to_dict() if hasattr(item, 'to_dict') else item 
                for item in self.line_items
            ],
            'validation': self.validation.__dict__ if self.validation else None
        }

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
        """Validate a budget class and return summary."""
        return {
            'is_valid': budget_class.validation.is_valid,
            'messages': budget_class.validation.messages,
            'line_items': len(budget_class.line_items),
            'has_actuals': any(item.get('actual', 0) for item in budget_class.line_items)
        } 