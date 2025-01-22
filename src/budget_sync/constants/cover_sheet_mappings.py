"""Mappings for cover sheet cell references."""

COVER_SHEET_MAPPINGS = {
    "project_info": {
        "project_title": "C5",
        "production_company": "C6",
        "contact_phone": "C7",
        "date": "H4"
    },
    "core_team": {
        "director": "C9",
        "producer": "C10",
        "writer": "C11"
    },
    "timeline": {
        "pre_prod_days": "D12",
        "build_days": "D13",
        "pre_light_days": "D14",
        "studio_days": "D15",
        "location_days": "D16",
        "wrap_days": "D17"
    },
    "firm_bid_summary": {
        "pre_production_wrap": {
            "description": "Pre-production and wrap costs",
            "categories": "Total A,C",
            "estimated": "G22",
            "actual": "H22",
            "variance": "I22",
            "client_actual": "J22",
            "client_variance": "K22"
        },
        "shooting_crew_labor": {
            "description": "Shooting crew labor",
            "categories": "Total B",
            "estimated": "G23",
            "actual": "H23",
            "variance": "I23",
            "client_actual": "J23",
            "client_variance": "K23"
        }
    },
    "grand_total": {
        "description": "GRAND BID TOTAL",
        "estimated": "G47",
        "actual": "H47",
        "variance": "I47",
        "client_actual": "J47",
        "client_variance": "K47"
    }
} 