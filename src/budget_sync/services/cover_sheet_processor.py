from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

class CoverSheetProcessor:
    """Handles extraction and processing of cover sheet data."""
    
    def __init__(self, sheets_service):
        self.sheets_service = sheets_service

    def extract_cover_sheet(self, grid_data: List[Dict]) -> Dict[str, Any]:
        """Extract and validate cover sheet data."""
        try:
            data = {
                "project_summary": self._extract_project_summary(grid_data),
                "financials": self._extract_financials(grid_data)
            }
            
            # Validate the extracted data
            validation_errors = self.validate_cover_sheet(data)
            if validation_errors:
                logging.warning(f"Cover sheet validation errors: {validation_errors}")
                
            return data
        except Exception as e:
            logging.error(f"Error extracting cover sheet: {str(e)}")
            raise

    def _extract_project_summary(self, grid_data: List[Dict]) -> Dict[str, Any]:
        """Extract project summary information."""
        return {
            "project_info": {
                "title": str(self._get_cell_value(grid_data, "C5") or ''),
                "production_company": str(self._get_cell_value(grid_data, "C6") or ''),
                "contact_phone": str(self._get_cell_value(grid_data, "C7") or ''),
                "date": str(self._get_cell_value(grid_data, "H4") or '')
            },
            "core_team": {
                "director": str(self._get_cell_value(grid_data, "C9") or ''),
                "producer": str(self._get_cell_value(grid_data, "C10") or ''),
                "writer": str(self._get_cell_value(grid_data, "C11") or '')
            },
            "timeline": {
                "pre_prod_days": self._get_cell_value(grid_data, "D12"),
                "build_days": self._get_cell_value(grid_data, "D13"),
                "pre_light_days": self._get_cell_value(grid_data, "D14"),
                "studio_days": self._get_cell_value(grid_data, "D15"),
                "location_days": self._get_cell_value(grid_data, "D16"),
                "wrap_days": self._get_cell_value(grid_data, "D17")
            }
        }

    def _extract_financials(self, grid_data: List[Dict]) -> Dict[str, Any]:
        """Extract financial summary information."""
        columns = {
            "estimated": "G",
            "actual": "H", 
            "variance": "I",
            "client_actual": "J",
            "client_variance": "K"
        }

        def get_row_values(row: int) -> Dict[str, str]:
            return {
                col_name: self._format_money(self._get_cell_value(grid_data, f"{col_letter}{row}"))
                for col_name, col_letter in columns.items()
            }

        return {
            "firm_bid": {
                "pre_production_wrap": {
                    "description": "Pre-production and wrap costs",
                    "categories": "Total A,C",
                    **get_row_values(22)
                },
                "shooting_crew_labor": {
                    "description": "Shooting crew labor",
                    "categories": "Total B",
                    **get_row_values(23)
                },
                "location_studio_travel": {
                    "description": "Location, Studio, and travel expenses",
                    "categories": "Total D, F",
                    **get_row_values(24)
                },
                "props_wardrobe": {
                    "description": "Props, wardrobe, animals",
                    "categories": "Total E",
                    **get_row_values(25)
                },
                "art_labor": {
                    "description": "Art labor & expenses",
                    "categories": "Total G",
                    **get_row_values(26)
                },
                "equipment": {
                    "description": "Equipment costs",
                    "categories": "Total I",
                    **get_row_values(27)
                },
                "film_stock": {
                    "description": "Film stock and printing",
                    "categories": "Total J",
                    **get_row_values(28)
                },
                "creative_fees": {
                    "description": "Creative fees",
                    "categories": "Total K",
                    **get_row_values(29)
                },
                "director_fees": {
                    "description": "Director fees",
                    "categories": "Total L",
                    **get_row_values(30)
                },
                "talent_costs": {
                    "description": "Talent costs and expenses",
                    "categories": "Totals M,N",
                    **get_row_values(31)
                },
                "agency_services": {
                    "description": "Agency Services",
                    "categories": "Total O",
                    **get_row_values(32)
                },
                "post_expenses": {
                    "description": "Post Expenses",
                    "categories": "Total P",
                    **get_row_values(33)
                },
                "production_fee": {
                    "description": "Production Fee",
                    "note": "20% NOT on K, L, O",
                    **get_row_values(34)
                },
                "subtotal": {
                    "description": "FIRM BID",
                    **get_row_values(35)
                }
            },
            "cost_plus": {
                "pnw": {
                    "description": "All P&W",
                    "categories": "Sub Total A, B G, M1",
                    **get_row_values(40)
                },
                "production_fee_pnw": {
                    "description": "Production Fee 10% on P&W",
                    "categories": "Total A, B G, M1",
                    **get_row_values(41)
                },
                "cost_plus_expenses": {
                    "description": "Cost Plus expenses",
                    "categories": "Sub Total H",
                    **get_row_values(42)
                },
                "production_fee_cost_plus": {
                    "description": "Production Fee 10% on Cost Plus",
                    "categories": "Total H",
                    **get_row_values(43)
                },
                "subtotal": {
                    "description": "COST PLUS",
                    **get_row_values(45)
                }
            },
            "grand_total": {
                "description": "GRAND BID TOTAL",
                **get_row_values(47)
            }
        }

    def _get_cell_value(self, cell_ref):
        """Get value from a cell using A1 notation."""
        try:
            row, col = self._parse_cell_ref(cell_ref)
            if not self.grid_data.get('rowData'):
                return None
            if row >= len(self.grid_data['rowData']):
                return None
            row_data = self.grid_data['rowData'][row]
            if not row_data.get('values'):
                return None
            if col >= len(row_data['values']):
                return None
            cell = row_data['values'][col]
            
            if not cell:
                return None

            # Handle different value types
            if 'formattedValue' in cell:
                return cell['formattedValue']
            elif 'effectiveValue' in cell:
                effective_value = cell['effectiveValue']
                if 'numberValue' in effective_value:
                    # Convert numeric values to strings for certain fields
                    if cell_ref in ["C5", "C6", "C7", "H4", "C9", "C10", "C11"]:
                        return str(effective_value['numberValue'])
                    return effective_value['numberValue']
                elif 'stringValue' in effective_value:
                    return effective_value['stringValue']
                elif 'boolValue' in effective_value:
                    return effective_value['boolValue']
            return None
        except Exception as e:
            logging.warning(f"Error getting cell {cell_ref}: {str(e)}")
            return None

    def _format_money(self, value: Optional[float]) -> str:
        """Format number as money string."""
        if value is None:
            return "$0.00"
        try:
            if isinstance(value, str):
                value = float(value.replace('$', '').replace(',', ''))
            return "${:,.2f}".format(value)
        except (ValueError, TypeError):
            return "$0.00"

    def _parse_money(self, value: str) -> float:
        """Parse money string to float."""
        try:
            return float(value.replace('$', '').replace(',', ''))
        except (ValueError, AttributeError):
            return 0.0

    def _calculate_production_fee(self, firm_bid: Dict[str, Any]) -> float:
        """Calculate 20% production fee (not on K, L, O)."""
        excluded_categories = ['creative_fees', 'director_fees', 'agency_services', 'subtotal']
        fee_base = sum(
            self._parse_money(v["estimated"])
            for k, v in firm_bid.items()
            if k not in excluded_categories
        )
        return fee_base * 0.20

    def _calculate_pnw(self, firm_bid: Dict[str, Any]) -> float:
        """Calculate P&W (28% on applicable categories)."""
        applicable_categories = ['pre_production_wrap', 'shooting_crew_labor', 'art_labor']
        return sum(
            self._parse_money(firm_bid[cat]["estimated"]) * 0.28
            for cat in applicable_categories
            if cat in firm_bid
        )

    def _validate_financial_data(self, value):
        """Validates and converts financial string to float."""
        if not value:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                # Remove currency symbols and commas
                cleaned = value.replace("$", "").replace(",", "").strip()
                return float(cleaned)
            except ValueError:
                return 0.0
        return 0.0

    def validate_cover_sheet(self, cover_sheet):
        """Validates the extracted cover sheet data."""
        errors = []
        
        # Validate project info
        if not cover_sheet['project_summary']['project_info']['title']:
            errors.append("Missing project title")
        if not cover_sheet['project_summary']['project_info']['production_company']:
            errors.append("Missing production company")
        
        # Validate timeline data
        timeline = cover_sheet['project_summary']['timeline']
        for key, value in timeline.items():
            if value is not None:
                timeline[key] = self._validate_financial_data(value)
        
        # Validate financial calculations
        financials = cover_sheet['financials']['firm_bid']
        categories_sum = sum(
            self._validate_financial_data(cat.get('estimated', 0))
            for cat in financials.values()
            if isinstance(cat, dict) and cat.get('estimated') is not None
        )
        
        subtotal = self._validate_financial_data(financials['subtotal']['estimated'])
        if abs(categories_sum - subtotal) > 0.01:  # Allow small floating point differences
            errors.append(f"Firm bid total mismatch: categories sum to ${categories_sum:,.2f} but subtotal is ${subtotal:,.2f}")
        
        return errors 

    def _parse_cell_ref(self, cell_ref):
        """Convert A1 notation to row/col indices."""
        col = 0
        row = 0
        for c in cell_ref:
            if c.isalpha():
                col = col * 26 + (ord(c.upper()) - ord('A'))
            else:
                row = row * 10 + (ord(c) - ord('0'))
        return row - 1, col  # Convert to 0-based indices 