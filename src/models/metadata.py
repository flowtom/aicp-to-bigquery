"""
Enhanced metadata tracking for budget processing.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Any, Optional

@dataclass
class BudgetMetadata:
    """Enhanced metadata for budget tracking"""
    version_id: str
    previous_version_id: Optional[str] = None
    creation_timestamp: datetime = field(default_factory=datetime.now)
    last_modified_timestamp: datetime = field(default_factory=datetime.now)
    modified_by: str = ""
    status: str = "draft"  # TODO: Define status workflow (draft/review/approved/etc)
    approval_chain: List[Dict[str, Any]] = field(default_factory=list)  # TODO: Define approval workflow
    change_log: List[Dict[str, Any]] = field(default_factory=list)
    validation_status: Dict[str, Any] = field(default_factory=dict)
    source_information: Dict[str, Any] = field(default_factory=dict)
    processing_statistics: Dict[str, Any] = field(default_factory=dict)

    def track_change(self, change_type: str, details: Dict[str, Any], user: str = None) -> None:
        """Track changes made to the budget"""
        self.change_log.append({
            'timestamp': datetime.now().isoformat(),
            'type': change_type,
            'details': details,
            'user': user or self.modified_by
        })
        self.last_modified_timestamp = datetime.now()
        if user:
            self.modified_by = user

    # TODO: Define and implement approval workflow
    # This should include:
    # - Approval steps/stages
    # - Required approvers
    # - Status transitions
    # - Notifications
    # - Audit trail

    def update_status(self, new_status: str, user: str = None, notes: str = None) -> None:
        """Update the budget status"""
        old_status = self.status
        self.status = new_status
        self.track_change('status_change', {
            'old_status': old_status,
            'new_status': new_status,
            'notes': notes
        }, user)

    def update_validation_status(self, validation_results: Dict[str, Any]) -> None:
        """Update validation status with new results"""
        self.validation_status = {
            'timestamp': datetime.now().isoformat(),
            'results': validation_results,
            'is_valid': all(result.get('is_valid', False) for result in validation_results.values())
        }

    def update_processing_stats(self, stats: Dict[str, Any]) -> None:
        """Update processing statistics"""
        self.processing_statistics = {
            'timestamp': datetime.now().isoformat(),
            **stats
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary format"""
        return {
            'version_id': self.version_id,
            'previous_version_id': self.previous_version_id,
            'creation_timestamp': self.creation_timestamp.isoformat(),
            'last_modified_timestamp': self.last_modified_timestamp.isoformat(),
            'modified_by': self.modified_by,
            'status': self.status,
            'approval_chain': self.approval_chain,
            'change_log': self.change_log,
            'validation_status': self.validation_status,
            'source_information': self.source_information,
            'processing_statistics': self.processing_statistics
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BudgetMetadata':
        """Create metadata instance from dictionary"""
        return cls(
            version_id=data['version_id'],
            previous_version_id=data.get('previous_version_id'),
            creation_timestamp=datetime.fromisoformat(data['creation_timestamp']),
            last_modified_timestamp=datetime.fromisoformat(data['last_modified_timestamp']),
            modified_by=data['modified_by'],
            status=data['status'],
            approval_chain=data.get('approval_chain', []),
            change_log=data.get('change_log', []),
            validation_status=data.get('validation_status', {}),
            source_information=data.get('source_information', {}),
            processing_statistics=data.get('processing_statistics', {})
        ) 